package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/olezhek28/grpc-workshop/internal/interceptor"
	"github.com/olezhek28/grpc-workshop/pkg/api"
	spaceshipV1 "github.com/olezhek28/grpc-workshop/pkg/proto/spaceship/v1"
)

const (
	httpReadHeaderTimeout = 10 * time.Second
	shutdownTimeout       = 5 * time.Second
)

var (
	host     = flag.String("host", "localhost", "Host to bind to (0.0.0.0 to accept external connections)")
	grpcPort = flag.Int("grpc-port", 50051, "gRPC server port")
	httpPort = flag.Int("http-port", 8081, "HTTP server port (gRPC-Gateway)")
	serverID = flag.String("server-id", "", "Server ID for load balancing demo (default: hostname:grpc-port)")
)

// subscription представляет подписку клиента на обновления
type subscription struct {
	id         string
	uuid       *string // nil означает подписку на все корабли
	eventChan  chan *spaceshipV1.WatchUpdatesResponse
	cancelChan chan struct{}
}

// spaceshipService реализует gRPC сервис для работы с космическими кораблями
//
// Хранилище устроено по принципу copy-on-write: попав в мапу, *Spaceship больше
// никогда не мутируется. Обновление кладет в мапу новую копию.
// Это важно, потому что указатель на сообщение уезжает в ответ (его маршалит gRPC
// уже после выхода из хендлера) и в события подписчиков — мутация на месте
// была бы гонкой данных на protobuf-сообщении.
type spaceshipService struct {
	spaceshipV1.UnimplementedSpaceshipServiceServer

	mu            sync.RWMutex
	spaceships    map[string]*spaceshipV1.Spaceship
	subscriptions map[string]subscription
	subsMu        sync.RWMutex
}

// Create создает новый космический корабль
func (s *spaceshipService) Create(_ context.Context, req *spaceshipV1.CreateRequest) (*spaceshipV1.CreateResponse, error) {
	if req.GetInfo() == nil {
		return nil, status.Error(codes.InvalidArgument, "info cannot be nil")
	}

	// Генерируем UUID для нового корабля
	newUUID := uuid.NewString()
	now := timestamppb.New(time.Now())

	spaceship := &spaceshipV1.Spaceship{
		Uuid: newUUID,
		// Клонируем info: сообщение запроса принадлежит gRPC, держать указатели
		// на его части в своем состоянии небезопасно.
		Info:      proto.CloneOf(req.GetInfo()),
		CreatedAt: now,
		UpdatedAt: now,
	}

	s.mu.Lock()
	s.spaceships[newUUID] = spaceship
	s.mu.Unlock()

	log.Printf("Создан корабль с UUID %s", newUUID)
	printEngineInfo(spaceship.GetInfo())

	// Отправляем событие о создании
	s.broadcastEvent(&spaceshipV1.WatchUpdatesResponse{
		UpdateEvent: &spaceshipV1.UpdateEvent{
			EventType: spaceshipV1.EventType_EVENT_TYPE_CREATED,
			Spaceship: spaceship,
			Timestamp: timestamppb.New(time.Now()),
		},
	})

	return &spaceshipV1.CreateResponse{
		Spaceship: spaceship,
	}, nil
}

// Get возвращает космический корабль по UUID
func (s *spaceshipService) Get(_ context.Context, req *spaceshipV1.GetRequest) (*spaceshipV1.GetResponse, error) {
	if req.GetUuid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uuid cannot be empty")
	}

	s.mu.RLock()
	spaceship, ok := s.spaceships[req.GetUuid()]
	s.mu.RUnlock()

	// Мягко удаленный корабль остается в хранилище, но для API его больше нет
	if !ok || spaceship.GetDeletedAt() != nil {
		return nil, status.Errorf(codes.NotFound, "spaceship with UUID %s not found", req.GetUuid())
	}

	// Клонировать не нужно: объекты в хранилище неизменяемы (copy-on-write),
	// поэтому gRPC может спокойно маршалить его уже вне лока.
	return &spaceshipV1.GetResponse{
		Spaceship: spaceship,
	}, nil
}

// Update обновляет существующий космический корабль
func (s *spaceshipService) Update(_ context.Context, req *spaceshipV1.UpdateRequest) (*spaceshipV1.UpdateResponse, error) {
	if req.GetUuid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uuid cannot be empty")
	}

	if req.GetUpdateInfo() == nil {
		return nil, status.Error(codes.InvalidArgument, "update_info cannot be nil")
	}

	spaceship, err := s.applyUpdate(req.GetUuid(), req.GetUpdateInfo())
	if err != nil {
		return nil, err
	}

	log.Printf("Обновлен корабль с UUID %s", req.GetUuid())
	printEngineInfo(spaceship.GetInfo())

	// Отправляем событие об обновлении
	s.broadcastEvent(&spaceshipV1.WatchUpdatesResponse{
		UpdateEvent: &spaceshipV1.UpdateEvent{
			EventType: spaceshipV1.EventType_EVENT_TYPE_UPDATED,
			Spaceship: spaceship,
			Timestamp: timestamppb.New(time.Now()),
		},
	})

	return &spaceshipV1.UpdateResponse{
		Spaceship: spaceship,
	}, nil
}

// applyUpdate атомарно применяет изменения к кораблю и возвращает его новую версию.
// Работает по принципу copy-on-write — см. комментарий к spaceshipService.
func (s *spaceshipService) applyUpdate(
	spaceshipUUID string,
	reqUpdateInfo *spaceshipV1.SpaceshipUpdateInfo,
) (*spaceshipV1.Spaceship, error) {
	// Клонируем данные запроса разом, чтобы не тащить в хранилище указатели
	// на части сообщения, которым владеет gRPC.
	updateInfo := proto.CloneOf(reqUpdateInfo)

	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.spaceships[spaceshipUUID]
	if !ok || current.GetDeletedAt() != nil {
		return nil, status.Errorf(codes.NotFound, "spaceship with UUID %s not found", spaceshipUUID)
	}

	// Мутируем копию, а не объект из хранилища: указатель на него уже могли отдать
	// в ответ на Get или в событие подписчику, и там его в этот момент
	// может маршалить другая горутина.
	spaceship := proto.CloneOf(current)
	if spaceship.GetInfo() == nil {
		spaceship.Info = &spaceshipV1.SpaceshipInfo{}
	}

	// Обновляем поля, только если они были установлены в запросе
	if updateInfo.GetName() != nil {
		spaceship.Info.Name = updateInfo.GetName().GetValue()
	}

	if updateInfo.GetDescription() != nil {
		spaceship.Info.Description = updateInfo.GetDescription()
	}

	if updateInfo.GetModel() != nil {
		spaceship.Info.Model = updateInfo.GetModel().GetValue()
	}

	if updateInfo.GetManufacturer() != nil {
		spaceship.Info.Manufacturer = updateInfo.GetManufacturer().GetValue()
	}

	if updateInfo.GetPrice() != nil {
		spaceship.Info.Price = updateInfo.GetPrice().GetValue()
	}

	// Обновляем двигатель, если он был указан в запросе
	switch engine := updateInfo.GetEngine().(type) {
	case *spaceshipV1.SpaceshipUpdateInfo_IonEngine:
		spaceship.Info.Engine = &spaceshipV1.SpaceshipInfo_IonEngine{
			IonEngine: engine.IonEngine,
		}
		log.Print("Установлен новый двигатель: Ионный")

	case *spaceshipV1.SpaceshipUpdateInfo_WarpDrive:
		spaceship.Info.Engine = &spaceshipV1.SpaceshipInfo_WarpDrive{
			WarpDrive: engine.WarpDrive,
		}
		log.Print("Установлен новый двигатель: Варп-двигатель")

	case *spaceshipV1.SpaceshipUpdateInfo_NuclearEngine:
		spaceship.Info.Engine = &spaceshipV1.SpaceshipInfo_NuclearEngine{
			NuclearEngine: engine.NuclearEngine,
		}
		log.Print("Установлен новый двигатель: Ядерный")

	case *spaceshipV1.SpaceshipUpdateInfo_AntimatterReactor:
		spaceship.Info.Engine = &spaceshipV1.SpaceshipInfo_AntimatterReactor{
			AntimatterReactor: engine.AntimatterReactor,
		}
		log.Print("Установлен новый двигатель: Антиматериальный реактор")
	}

	spaceship.UpdatedAt = timestamppb.New(time.Now())

	// Публикуем новую версию в хранилище
	s.spaceships[spaceshipUUID] = spaceship

	return spaceship, nil
}

// Delete выполняет мягкое удаление корабля: проставляет deleted_at, а сама запись
// остается в хранилище. Для API корабль после этого не существует — Get и Update
// отвечают на него NotFound, как и повторный Delete.
func (s *spaceshipService) Delete(_ context.Context, req *spaceshipV1.DeleteRequest) (*emptypb.Empty, error) {
	if req.GetUuid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uuid cannot be empty")
	}

	spaceship, err := s.markDeleted(req.GetUuid())
	if err != nil {
		return nil, err
	}

	log.Printf("Удален корабль с UUID %s", req.GetUuid())

	// В событии отдаем tombstone целиком: подписчик видит и данные корабля,
	// и время удаления, а не один голый UUID.
	s.broadcastEvent(&spaceshipV1.WatchUpdatesResponse{
		UpdateEvent: &spaceshipV1.UpdateEvent{
			EventType: spaceshipV1.EventType_EVENT_TYPE_DELETED,
			Spaceship: spaceship,
			Timestamp: timestamppb.New(time.Now()),
		},
	})

	return &emptypb.Empty{}, nil
}

// markDeleted атомарно помечает корабль удаленным и возвращает получившийся tombstone.
// Как и applyUpdate, работает по copy-on-write — см. комментарий к spaceshipService.
func (s *spaceshipService) markDeleted(spaceshipUUID string) (*spaceshipV1.Spaceship, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.spaceships[spaceshipUUID]
	if !ok || current.GetDeletedAt() != nil {
		return nil, status.Errorf(codes.NotFound, "spaceship with UUID %s not found", spaceshipUUID)
	}

	spaceship := proto.CloneOf(current)
	spaceship.DeletedAt = timestamppb.New(time.Now())
	s.spaceships[spaceshipUUID] = spaceship

	return spaceship, nil
}

// WatchUpdates реализует server streaming для отслеживания обновлений
func (s *spaceshipService) WatchUpdates(req *spaceshipV1.WatchUpdatesRequest, stream spaceshipV1.SpaceshipService_WatchUpdatesServer) error {
	// Создаем подписку
	sub := subscription{
		id:         uuid.NewString(),
		eventChan:  make(chan *spaceshipV1.WatchUpdatesResponse, 100), // Буфер для событий
		cancelChan: make(chan struct{}),
	}

	// Если указан конкретный UUID, фильтруем по нему
	if req.GetUuid() != nil && req.GetUuid().Value != "" {
		sub.uuid = &req.GetUuid().Value
	}

	// Регистрируем подписку
	s.subsMu.Lock()
	s.subscriptions[sub.id] = sub
	s.subsMu.Unlock()

	log.Printf("Новая подписка на обновления: ID=%s, UUID корабля=%v", sub.id, sub.uuid)

	// Удаляем подписку при завершении
	defer func() {
		s.subsMu.Lock()
		delete(s.subscriptions, sub.id)
		s.subsMu.Unlock()
		close(sub.cancelChan)
		close(sub.eventChan)
		log.Printf("Подписка завершена: ID=%s", sub.id)
	}()

	// Слушаем события и отправляем их клиенту
	for {
		select {
		case event := <-sub.eventChan:
			if err := stream.Send(event); err != nil {
				log.Printf("Ошибка отправки события клиенту %s: %v", sub.id, err)
				return err
			}

		case <-sub.cancelChan:
			log.Printf("Подписка отменена: ID=%s", sub.id)
			return nil

		case <-stream.Context().Done():
			log.Printf("Клиент отключился: ID=%s", sub.id)
			return stream.Context().Err()
		}
	}
}

// broadcastEvent отправляет событие всем подписчикам
func (s *spaceshipService) broadcastEvent(event *spaceshipV1.WatchUpdatesResponse) {
	s.subsMu.RLock()
	defer s.subsMu.RUnlock()

	for _, sub := range s.subscriptions {
		// Проверяем фильтр по UUID
		if sub.uuid != nil &&
			event.UpdateEvent.Spaceship != nil &&
			*sub.uuid != event.UpdateEvent.Spaceship.Uuid {
			continue // Пропускаем, если не подходит фильтр
		}

		// Отправляем событие неблокирующим способом
		select {
		case sub.eventChan <- event:
			// Событие отправлено
		default:
			// Канал переполнен, пропускаем событие
			log.Printf("Пропущено событие для подписки %s: канал переполнен", sub.id)
		}
	}

	log.Printf("Отправлено событие %s для %d подписчиков", event.UpdateEvent.EventType, len(s.subscriptions))
}

// printEngineInfo выводит информацию о двигателе корабля
// Демонстрирует использование type switch для oneof в Go
func printEngineInfo(info *spaceshipV1.SpaceshipInfo) {
	if info == nil {
		return
	}

	// Type switch для определения типа двигателя из oneof
	switch engine := info.GetEngine().(type) {
	case *spaceshipV1.SpaceshipInfo_IonEngine:
		log.Printf("  ⚡ Двигатель: Ионный двигатель")
		log.Printf("     Тяга: %.2f Н", engine.IonEngine.GetThrustNewtons())
		log.Printf("     Тип топлива: %s", engine.IonEngine.GetPropellantType())
		log.Printf("     Эффективность: %.2f сек", engine.IonEngine.GetFuelEfficiency())

	case *spaceshipV1.SpaceshipInfo_WarpDrive:
		log.Printf("  🚀 Двигатель: Варп-двигатель")
		log.Printf("     Варп-фактор: %d", engine.WarpDrive.GetWarpFactor())
		log.Printf("     Стабильность поля: %.2f%%", engine.WarpDrive.GetFieldStability()*100)
		if engine.WarpDrive.GetDilithiumCrystals() {
			log.Printf("     Дилитиевые кристаллы: установлены ✓")
		} else {
			log.Printf("     Дилитиевые кристаллы: отсутствуют ✗")
		}

	case *spaceshipV1.SpaceshipInfo_NuclearEngine:
		log.Printf("  ☢️  Двигатель: Ядерный реактор")
		log.Printf("     Мощность: %.2f МВт", engine.NuclearEngine.GetReactorPowerMw())
		log.Printf("     Охладитель: %s", engine.NuclearEngine.GetCoolantType())
		log.Printf("     Уровень защиты: %d/10", engine.NuclearEngine.GetShieldingLevel())

	case *spaceshipV1.SpaceshipInfo_AntimatterReactor:
		log.Printf("  💥 Двигатель: Антиматериальный реактор")
		log.Printf("     Система удержания: %s", engine.AntimatterReactor.GetAntimatterContainment())
		log.Printf("     Выходная мощность: %.2f ТВт", engine.AntimatterReactor.GetPowerOutputTw())
		log.Printf("     Магнитное поле: %.2f Тл", engine.AntimatterReactor.GetMagneticFieldStrength())

	case nil:
		log.Printf("  ⚠️  Двигатель: не указан")

	default:
		log.Printf("  ❓ Двигатель: неизвестный тип")
	}
}

// newGRPCServer собирает gRPC сервер с полной цепочкой интерцепторов.
// Отделено от initGRPCServer, чтобы тесты могли поднять ровно тот же сервер
// на bufconn, без слушающего сокета.
func newGRPCServer(serverID string, service spaceshipV1.SpaceshipServiceServer) *grpc.Server {
	s := grpc.NewServer(
		grpc.ChainStreamInterceptor(
			// Порядок тот же, что и в unary-цепочке, и по той же причине:
			// ServerID кладет header, Metadata его отправляет.
			interceptor.ServerIDStreamServerInterceptor(serverID),
			interceptor.MetadataStreamServerInterceptor(),
			interceptor.ValidationStreamServerInterceptor(),
			interceptor.ResponseTimeStreamServerInterceptor(),
		),
		grpc.ChainUnaryInterceptor(
			// Порядок важен: ServerID добавляет свой header через SetHeader,
			// а Metadata ниже делает SendHeader и тем самым отправляет headers клиенту.
			// Если поменять их местами, SetHeader вернет
			// "transport: SendHeader called multiple times", и server-id не доедет до клиента.
			interceptor.ServerIDUnaryServerInterceptor(serverID),
			interceptor.MetadataUnaryServerInterceptor(),
			interceptor.ValidationUnaryServerInterceptor(),
			interceptor.ResponseTimeUnaryServerInterceptor(),
		),
	)

	spaceshipV1.RegisterSpaceshipServiceServer(s, service)
	reflection.Register(s)

	return s
}

// newSpaceshipService создает сервис с пустым in-memory хранилищем
func newSpaceshipService() *spaceshipService {
	return &spaceshipService{
		spaceships:    make(map[string]*spaceshipV1.Spaceship),
		subscriptions: make(map[string]subscription),
	}
}

func initGRPCServer(
	ctx context.Context,
	grpcAddress, serverID string,
	service spaceshipV1.SpaceshipServiceServer,
) (*grpc.Server, net.Listener, error) {
	// ListenConfig, а не net.Listen: слушающий сокет тоже создается в контексте
	// (его можно отменить, пока резолвится адрес). Этого требует линтер noctx.
	var lc net.ListenConfig

	lis, err := lc.Listen(ctx, "tcp", grpcAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}

	return newGRPCServer(serverID, service), lis, nil
}

// Обслуживание статических файлов и роутинг в Go HTTP сервере
//
// Файловый сервер (http.FileServer):
//   - Это встроенный механизм Go для обслуживания статических файлов через HTTP
//   - http.FileServer создает обработчик, который предоставляет доступ к файлам из указанной директории
//   - При запросе файла, сервер проверяет его существование, устанавливает правильные HTTP заголовки
//     и отправляет содержимое клиенту
//   - Автоматически обрабатывает кэширование, диапазоны байтов и другие HTTP функции
//
// Роутинг в HTTP сервере:
// - http.ServeMux - это HTTP маршрутизатор (мультиплексор), который сопоставляет URL паттерны с обработчиками
// - Каждый зарегистрированный паттерн связан с обработчиком (Handler), который вызывается при совпадении URL
// - Паттерны сопоставляются по префиксу, например "/api/" обрабатывает все пути, начинающиеся с "/api/"
//
// В нашем приложении:
// 1. Мы создаем файловый сервер, который обслуживает статические файлы из директории "api"
// 2. Настраиваем маршрутизацию:
//   - "/api/" направляет запросы к gRPC-Gateway мультиплексору (REST API)
//   - "/swagger-ui.html" и "/swagger.swagger.json" обрабатываются файловым сервером
//   - Корневой путь "/" перенаправляет на Swagger UI или обрабатывается файловым сервером
//
// Такая архитектура позволяет в одном HTTP сервере обслуживать как API, так и документацию к нему.
func initSwaggerUI(gwMux *runtime.ServeMux) http.Handler {
	// Создаем файловый сервер для swagger-ui
	fileServer := http.FileServer(http.FS(api.FS))

	// Создаем HTTP маршрутизатор
	httpMux := http.NewServeMux()

	// Регистрируем API эндпоинты
	httpMux.Handle("/api/", gwMux)

	// Swagger UI эндпоинты
	httpMux.Handle("/swagger-ui.html", fileServer)
	httpMux.Handle("/swagger/swagger.swagger.json", fileServer)

	// Редирект с корня на Swagger UI
	httpMux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/swagger-ui.html", http.StatusMovedPermanently)
			return
		}
		fileServer.ServeHTTP(w, r)
	}))

	return httpMux
}

func initHTTPServer(ctx context.Context, httpAddress, grpcAddress string) (*http.Server, error) {
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	err := spaceshipV1.RegisterSpaceshipServiceHandlerFromEndpoint(
		ctx,
		mux,
		grpcAddress,
		opts,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway: %w", err)
	}

	// Интегрируем Swagger UI
	handler := initSwaggerUI(mux)

	gwServer := &http.Server{
		Addr:              httpAddress,
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
	}

	return gwServer, nil
}

func main() {
	flag.Parse()

	// Формируем адреса
	grpcAddr := net.JoinHostPort(*host, strconv.Itoa(*grpcPort))
	httpAddr := net.JoinHostPort(*host, strconv.Itoa(*httpPort))

	// Если serverID не указан, тогда берем grpcAddr в качестве значения
	actualServerID := *serverID
	if actualServerID == "" {
		actualServerID = grpcAddr
	}

	log.Printf("🆔 Server ID: %s", actualServerID)

	service := newSpaceshipService()

	ctx := context.Background()

	grpcServer, lis, err := initGRPCServer(ctx, grpcAddr, actualServerID, service)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	// err объявляем локально внутри горутины: общая переменная с main-горутиной
	// была бы гонкой данных.
	go func() {
		log.Printf("🚀 gRPC server listening on %s\n", grpcAddr)
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			log.Printf("failed to serve: %v\n", serveErr)
		}
	}()

	gwServer, err := initHTTPServer(ctx, httpAddr, grpcAddr)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	go func() {
		log.Printf("🌐 HTTP server with gRPC-Gateway listening on %s\n", httpAddr)
		log.Printf("📚 Swagger UI listening on %s\n", httpAddr)
		if serveErr := gwServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Printf("Failed to serve HTTP: %v\n", serveErr)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("🛑 Shutting down servers...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Ошибку остановки HTTP только логируем: gRPC сервер все равно надо погасить.
	if shutdownErr := gwServer.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Printf("HTTP server shutdown error: %v", shutdownErr)
	} else {
		log.Println("✅ HTTP server stopped")
	}

	grpcServer.GracefulStop()
	log.Println("✅ gRPC server stopped")
}
