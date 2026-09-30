package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	spaceshipV1 "github.com/olezhek28/grpc-workshop/pkg/proto/spaceship/v1"
)

const (
	serverAddr     = "127.0.0.1:50051"
	requestTimeout = 30 * time.Second

	streamTimeout = 60 * time.Second // сколько живет демонстрационная подписка
	connectDelay  = 2 * time.Second  // пауза на установку стрима перед первой операцией
	stepDelay     = 1 * time.Second  // пауза между шагами демо, чтобы события были видны
	drainDelay    = 2 * time.Second  // пауза на долет последних событий перед отменой
	shutdownWait  = 5 * time.Second  // сколько ждем корректного завершения стрима
)

// StreamingClient клиент для демонстрации streaming функциональности
type StreamingClient struct {
	conn   *grpc.ClientConn
	client spaceshipV1.SpaceshipServiceClient
}

// NewStreamingClient создает новый streaming клиент
func NewStreamingClient(serverAddr string) (*StreamingClient, error) {
	fmt.Printf("🔌 Подключаемся к серверу %s...\n", serverAddr)

	conn, err := grpc.NewClient(
		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &StreamingClient{
		conn:   conn,
		client: spaceshipV1.NewSpaceshipServiceClient(conn),
	}, nil
}

// Close закрывает соединение с сервером
func (c *StreamingClient) Close() error {
	return c.conn.Close()
}

// call оборачивает контекст дедлайном на один unary-вызов.
// Стрим живет по своему таймауту (streamTimeout), а разовые Create/Update/Delete
// не должны висеть дольше requestTimeout.
func call(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, requestTimeout)
}

// WatchAllUpdates подписывается на все обновления кораблей
func (c *StreamingClient) WatchAllUpdates(ctx context.Context) error {
	fmt.Println("🎧 Подписываемся на все обновления кораблей...")

	req := &spaceshipV1.WatchUpdatesRequest{
		// uuid не указываем, чтобы получать все события
	}

	stream, err := c.client.WatchUpdates(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to watch updates: %w", err)
	}

	return c.consumeStream(stream)
}

// WatchSpaceshipUpdates подписывается на обновления конкретного корабля
func (c *StreamingClient) WatchSpaceshipUpdates(ctx context.Context, uuid string) error {
	fmt.Printf("🎧 Подписываемся на обновления корабля %s...\n", uuid)

	req := &spaceshipV1.WatchUpdatesRequest{
		Uuid: wrapperspb.String(uuid),
	}

	stream, err := c.client.WatchUpdates(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to watch spaceship updates: %w", err)
	}

	return c.consumeStream(stream)
}

// consumeStream читает события из стрима до его завершения или ошибки
func (c *StreamingClient) consumeStream(stream spaceshipV1.SpaceshipService_WatchUpdatesClient) error {
	fmt.Println("✅ Подписка активна! Ожидаем события...")

	for {
		resp, err := stream.Recv()
		// io.EOF — штатное завершение стрима сервером, а не ошибка.
		// Сравнивать нужно через errors.Is: ошибка может прийти обернутой.
		if errors.Is(err, io.EOF) {
			fmt.Println("🔚 Сервер завершил стрим")
			return nil
		}
		if err != nil {
			return fmt.Errorf("error receiving event: %w", err)
		}

		c.printEvent(resp.GetUpdateEvent())
	}
}

// printEvent красиво выводит событие
func (c *StreamingClient) printEvent(event *spaceshipV1.UpdateEvent) {
	timestamp := event.GetTimestamp().AsTime().Format("15:04:05.000")
	eventType := c.getEventTypeString(event.GetEventType())

	fmt.Printf("📡 [%s] %s", timestamp, eventType)

	if event.GetSpaceship() != nil {
		spaceship := event.GetSpaceship()
		fmt.Printf(" | UUID: %s", spaceship.GetUuid())

		if spaceship.GetInfo() != nil {
			fmt.Printf(" | Имя: %s", spaceship.GetInfo().GetName())
			if event.GetEventType() != spaceshipV1.EventType_EVENT_TYPE_DELETED {
				fmt.Printf(" | Цена: %.2f", spaceship.GetInfo().GetPrice())
			}
		}
	}

	fmt.Println()
}

// getEventTypeString возвращает человекочитаемое название типа события
func (c *StreamingClient) getEventTypeString(eventType spaceshipV1.EventType) string {
	// Все значения enum перечислены явно (этого требует линтер exhaustive):
	// когда в .proto добавится новое событие, линтер укажет на этот switch,
	// а не даст ему молча провалиться в default.
	switch eventType {
	case spaceshipV1.EventType_EVENT_TYPE_CREATED:
		return "🚀 СОЗДАН"
	case spaceshipV1.EventType_EVENT_TYPE_UPDATED:
		return "✏️  ОБНОВЛЕН"
	case spaceshipV1.EventType_EVENT_TYPE_DELETED:
		return "🗑️  УДАЛЕН"
	case spaceshipV1.EventType_EVENT_TYPE_UNSPECIFIED:
		return "❔ НЕ УКАЗАН"
	default:
		return "❓ НЕИЗВЕСТНО"
	}
}

// DeleteTestSpaceship удаляет тестовый корабль
func (c *StreamingClient) DeleteTestSpaceship(ctx context.Context, uuid string) error {
	req := &spaceshipV1.DeleteRequest{
		Uuid: uuid,
	}

	ctx, cancel := call(ctx)
	defer cancel()

	_, err := c.client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to delete test spaceship: %w", err)
	}

	return nil
}

// RunStreamingDemo запускает демонстрацию streaming с автоматическими операциями.
// Держит только каркас: поднять подписку — прогнать операции — дождаться закрытия стрима.
// Сами операции живут в runDemoOperations, а тела запросов — в конструкторах ниже.
func (c *StreamingClient) RunStreamingDemo(ctx context.Context) error {
	// Создаем контекст с timeout для streaming
	streamCtx, streamCancel := context.WithTimeout(ctx, streamTimeout)
	defer streamCancel()

	// Запускаем стрим в отдельной горутине
	streamDone := make(chan error, 1)
	go func() {
		streamDone <- c.WatchAllUpdates(streamCtx)
	}()

	// Даем время на установку подключения
	if err := wait(ctx, connectDelay); err != nil {
		return err
	}

	if err := c.runDemoOperations(ctx); err != nil {
		return err
	}

	// Ждем немного для обработки последних событий
	if err := wait(ctx, drainDelay); err != nil {
		return err
	}

	// Отменяем стрим
	fmt.Println("\n🔚 Завершаем streaming соединение...")
	streamCancel()

	if err := awaitStreamShutdown(streamDone); err != nil {
		return err
	}

	fmt.Println("\n🎉 Демонстрация streaming завершена!")

	return nil
}

// runDemoOperations выполняет CRUD-операции, события которых должны прилететь в стрим.
// Между шагами — пауза, чтобы события было видно в выводе по одному.
func (c *StreamingClient) runDemoOperations(ctx context.Context) error {
	fmt.Println("\n🎬 Начинаем демонстрацию streaming операций...")

	// 1. Создаем корабль с ионным двигателем
	fmt.Println("\n1️⃣ Создаем корабль с ионным двигателем...")

	resp1, err := c.create(ctx, ionEngineShipRequest())
	if err != nil {
		return fmt.Errorf("demo failed on create: %w", err)
	}

	firstUUID := resp1.GetSpaceship().GetUuid()

	if err = wait(ctx, stepDelay); err != nil {
		return err
	}

	// 2. Обновляем корабль, меняем двигатель на ядерный
	fmt.Println("\n2️⃣ Обновляем корабль, устанавливаем ядерный двигатель...")

	if err = c.update(ctx, nuclearEngineUpdateRequest(firstUUID)); err != nil {
		return fmt.Errorf("demo failed on update: %w", err)
	}

	if err = wait(ctx, stepDelay); err != nil {
		return err
	}

	// 3. Создаем еще один корабль с варп-двигателем
	fmt.Println("\n3️⃣ Создаем второй корабль с варп-двигателем...")

	resp2, err := c.create(ctx, warpDriveShipRequest())
	if err != nil {
		return fmt.Errorf("demo failed on create: %w", err)
	}

	secondUUID := resp2.GetSpaceship().GetUuid()

	if err = wait(ctx, stepDelay); err != nil {
		return err
	}

	// 4. Удаляем первый корабль
	fmt.Println("\n4️⃣ Удаляем первый корабль...")

	if err = c.DeleteTestSpaceship(ctx, firstUUID); err != nil {
		return fmt.Errorf("demo failed on delete: %w", err)
	}

	if err = wait(ctx, stepDelay); err != nil {
		return err
	}

	// 5. Удаляем второй корабль
	fmt.Println("\n5️⃣ Удаляем второй корабль...")

	if err = c.DeleteTestSpaceship(ctx, secondUUID); err != nil {
		return fmt.Errorf("demo failed on delete: %w", err)
	}

	return nil
}

// create выполняет Create с дедлайном на вызов
func (c *StreamingClient) create(ctx context.Context, req *spaceshipV1.CreateRequest) (*spaceshipV1.CreateResponse, error) {
	ctx, cancel := call(ctx)
	defer cancel()

	return c.client.Create(ctx, req)
}

// update выполняет Update с дедлайном на вызов
func (c *StreamingClient) update(ctx context.Context, req *spaceshipV1.UpdateRequest) error {
	ctx, cancel := call(ctx)
	defer cancel()

	_, err := c.client.Update(ctx, req)

	return err
}

// RunFilteredWatchDemo показывает подписку с фильтром по UUID.
// Создаем два корабля, подписываемся только на первый и трогаем оба:
// в стрим приезжают события только по отслеживаемому кораблю.
func (c *StreamingClient) RunFilteredWatchDemo(ctx context.Context) error {
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("🎯 Демонстрация фильтрации событий по UUID")
	fmt.Println(strings.Repeat("=", 70))

	watched, err := c.create(ctx, ionEngineShipRequest())
	if err != nil {
		return fmt.Errorf("filtered demo failed on create: %w", err)
	}

	other, err := c.create(ctx, warpDriveShipRequest())
	if err != nil {
		return fmt.Errorf("filtered demo failed on create: %w", err)
	}

	watchedUUID := watched.GetSpaceship().GetUuid()
	otherUUID := other.GetSpaceship().GetUuid()

	streamCtx, streamCancel := context.WithTimeout(ctx, streamTimeout)
	defer streamCancel()

	streamDone := make(chan error, 1)

	go func() {
		streamDone <- c.WatchSpaceshipUpdates(streamCtx, watchedUUID)
	}()

	if err = wait(ctx, connectDelay); err != nil {
		return err
	}

	fmt.Printf("\n✏️  Обновляем ОТСЛЕЖИВАЕМЫЙ корабль %s — событие должно прийти\n", watchedUUID[:8])

	if err = c.update(ctx, nuclearEngineUpdateRequest(watchedUUID)); err != nil {
		return fmt.Errorf("filtered demo failed on update: %w", err)
	}

	if err = wait(ctx, stepDelay); err != nil {
		return err
	}

	fmt.Printf("\n✏️  Обновляем ДРУГОЙ корабль %s — события быть НЕ должно\n", otherUUID[:8])

	if err = c.update(ctx, nuclearEngineUpdateRequest(otherUUID)); err != nil {
		return fmt.Errorf("filtered demo failed on update: %w", err)
	}

	if err = wait(ctx, drainDelay); err != nil {
		return err
	}

	streamCancel()

	return awaitStreamShutdown(streamDone)
}

// ionEngineShipRequest — корабль с ионным двигателем (oneof: ion_engine)
func ionEngineShipRequest() *spaceshipV1.CreateRequest {
	return &spaceshipV1.CreateRequest{
		Info: &spaceshipV1.SpaceshipInfo{
			Name:         "Streaming Enterprise",
			Description:  wrapperspb.String("Тестовый корабль с ионным двигателем"),
			Model:        "STREAM-ION-001",
			Manufacturer: "Streaming Corp",
			Price:        999999.99,
			Engine: &spaceshipV1.SpaceshipInfo_IonEngine{
				IonEngine: &spaceshipV1.IonEngine{
					ThrustNewtons:  15000.0,
					PropellantType: "Ксенон",
					FuelEfficiency: 3100.0,
				},
			},
		},
	}
}

// warpDriveShipRequest — корабль с варп-двигателем (oneof: warp_drive)
func warpDriveShipRequest() *spaceshipV1.CreateRequest {
	return &spaceshipV1.CreateRequest{
		Info: &spaceshipV1.SpaceshipInfo{
			Name:         "Streaming Voyager",
			Description:  wrapperspb.String("Тестовый корабль с варп-двигателем"),
			Model:        "STREAM-WARP-002",
			Manufacturer: "Streaming Corp",
			Price:        2500000.00,
			Engine: &spaceshipV1.SpaceshipInfo_WarpDrive{
				WarpDrive: &spaceshipV1.WarpDrive{
					WarpFactor:        9,
					FieldStability:    0.95,
					DilithiumCrystals: true,
				},
			},
		},
	}
}

// nuclearEngineUpdateRequest — смена двигателя на ядерный (oneof в update-сообщении)
func nuclearEngineUpdateRequest(spaceshipUUID string) *spaceshipV1.UpdateRequest {
	return &spaceshipV1.UpdateRequest{
		Uuid: spaceshipUUID,
		UpdateInfo: &spaceshipV1.SpaceshipUpdateInfo{
			Name:  wrapperspb.String("Updated Streaming Enterprise"),
			Price: wrapperspb.Double(1234567.89),
			Engine: &spaceshipV1.SpaceshipUpdateInfo_NuclearEngine{
				NuclearEngine: &spaceshipV1.NuclearEngine{
					ReactorPowerMw: 500.0,
					CoolantType:    "Жидкий натрий",
					ShieldingLevel: 8,
				},
			},
		},
	}
}

// awaitStreamShutdown ждет завершения стрима и отличает штатную отмену от реальной ошибки
func awaitStreamShutdown(streamDone <-chan error) error {
	select {
	case err := <-streamDone:
		switch {
		case err == nil:
			fmt.Println("✅ Streaming завершен без ошибок")
		case isCancellation(err):
			fmt.Println("✅ Streaming корректно завершен (контекст отменен)")
		default:
			return fmt.Errorf("streaming error: %w", err)
		}

		return nil

	case <-time.After(shutdownWait):
		return errors.New("timeout waiting for stream to finish")
	}
}

// isCancellation сообщает, вызвано ли завершение стрима отменой контекста,
// а не настоящей ошибкой. Отмена приезжает в двух видах: как gRPC-статус
// Canceled/DeadlineExceeded и как обычная context.Canceled/DeadlineExceeded.
func isCancellation(err error) bool {
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.Canceled || st.Code() == codes.DeadlineExceeded {
			return true
		}
	}

	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// wait приостанавливает выполнение на d, но прерывается при отмене контекста.
// Именно поэтому здесь таймер, а не time.Sleep: sleep нельзя прервать,
// и на отмене контекста демо продолжало бы висеть до конца паузы.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("❌ %v", err)
	}
}

// run содержит логику демо: вынесено из main, чтобы log.Fatal не съедал
// отложенное закрытие соединения (defer не выполняется при os.Exit).
func run() error {
	client, err := NewStreamingClient(serverAddr)
	if err != nil {
		return fmt.Errorf("не удалось создать streaming клиент: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("⚠️  Ошибка при закрытии соединения: %v", closeErr)
		}
	}()

	ctx := context.Background()

	if err = client.RunStreamingDemo(ctx); err != nil {
		return fmt.Errorf("ошибка в streaming демонстрации: %w", err)
	}

	if err = client.RunFilteredWatchDemo(ctx); err != nil {
		return fmt.Errorf("ошибка в демонстрации фильтрации: %w", err)
	}

	return nil
}
