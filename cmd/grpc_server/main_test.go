package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"

	spaceshipV1 "github.com/olezhek28/grpc-workshop/pkg/proto/spaceship/v1"
)

const (
	testServerID = "test-server"
	bufSize      = 1024 * 1024
	// Сколько ждем события в стриме, прежде чем считать, что оно не придет
	eventWait = 2 * time.Second
)

// newTestClient поднимает сервер с полной цепочкой интерцепторов на bufconn
// и возвращает подключенный к нему клиент.
//
// bufconn — это net.Listener поверх буфера в памяти: настоящий gRPC со всей
// его сериализацией, интерцепторами и стримами, но без TCP и без выбора порта,
// поэтому тесты можно гонять параллельно и они ни с чем не конфликтуют.
// Каждый вызов дает свежий сервер с пустым хранилищем — тесты изолированы.
func newTestClient(t *testing.T) spaceshipV1.SpaceshipServiceClient {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := newGRPCServer(testServerID, newSpaceshipService())

	go func() {
		// После lis.Close() Serve возвращает ошибку — это штатное завершение.
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("не удалось создать клиент: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})

	return spaceshipV1.NewSpaceshipServiceClient(conn)
}

// validInfo возвращает корректные данные корабля для тестов
func validInfo() *spaceshipV1.SpaceshipInfo {
	return &spaceshipV1.SpaceshipInfo{
		Name:         "Test Ship",
		Description:  wrapperspb.String("Тестовый корабль"),
		Model:        "TEST-001",
		Manufacturer: "Test Corp",
		Price:        1000,
		Engine: &spaceshipV1.SpaceshipInfo_WarpDrive{
			WarpDrive: &spaceshipV1.WarpDrive{
				WarpFactor:        5,
				FieldStability:    0.9,
				DilithiumCrystals: true,
			},
		},
	}
}

// createShip создает корабль и возвращает его UUID
func createShip(t *testing.T, ctx context.Context, client spaceshipV1.SpaceshipServiceClient) string {
	t.Helper()

	resp, err := client.Create(ctx, &spaceshipV1.CreateRequest{Info: validInfo()})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	return resp.GetSpaceship().GetUuid()
}

// requireCode проверяет, что ошибка — это gRPC-статус с ожидаемым кодом
func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()

	if err == nil {
		t.Fatalf("ожидали ошибку %s, получили nil", want)
	}

	if got := status.Code(err); got != want {
		t.Fatalf("ожидали код %s, получили %s (%v)", want, got, err)
	}
}

func TestCreateAndGet(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestClient(t)

	created, err := client.Create(ctx, &spaceshipV1.CreateRequest{Info: validInfo()})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	ship := created.GetSpaceship()
	if ship.GetUuid() == "" {
		t.Fatal("Create не вернул UUID")
	}

	if ship.GetCreatedAt() == nil || ship.GetUpdatedAt() == nil {
		t.Error("Create не проставил created_at/updated_at")
	}

	if ship.GetDeletedAt() != nil {
		t.Error("свежесозданный корабль не должен иметь deleted_at")
	}

	got, err := client.Get(ctx, &spaceshipV1.GetRequest{Uuid: ship.GetUuid()})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.GetSpaceship().GetInfo().GetName() != validInfo().GetName() {
		t.Errorf("Get вернул имя %q, ожидали %q",
			got.GetSpaceship().GetInfo().GetName(), validInfo().GetName())
	}

	// oneof должен доехать целиком
	if got.GetSpaceship().GetInfo().GetWarpDrive().GetWarpFactor() != 5 {
		t.Error("oneof engine потерялся при сохранении")
	}
}

func TestCreateValidation(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestClient(t)

	cases := []struct {
		name string
		req  *spaceshipV1.CreateRequest
	}{
		{
			// Регресс на панику: раньше info == nil проходил валидацию,
			// в хранилище попадал корабль с nil Info, и следующий Update ронял сервер.
			name: "info не задан",
			req:  &spaceshipV1.CreateRequest{},
		},
		{
			name: "имя короче min_len",
			req: &spaceshipV1.CreateRequest{
				Info: &spaceshipV1.SpaceshipInfo{Name: "Te", Model: "M", Manufacturer: "C"},
			},
		},
		{
			name: "имя длиннее max_len",
			req: &spaceshipV1.CreateRequest{
				Info: &spaceshipV1.SpaceshipInfo{
					Name:         "TeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTeTe",
					Model:        "M",
					Manufacturer: "C",
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Create(ctx, tc.req)
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestGetNotFound(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestClient(t)

	_, err := client.Get(ctx, &spaceshipV1.GetRequest{Uuid: "00000000-0000-0000-0000-000000000000"})
	requireCode(t, err, codes.NotFound)

	_, err = client.Get(ctx, &spaceshipV1.GetRequest{Uuid: ""})
	requireCode(t, err, codes.InvalidArgument)
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestClient(t)
	shipUUID := createShip(t, ctx, client)

	updated, err := client.Update(ctx, &spaceshipV1.UpdateRequest{
		Uuid: shipUUID,
		UpdateInfo: &spaceshipV1.SpaceshipUpdateInfo{
			Name:  wrapperspb.String("Renamed Ship"),
			Price: wrapperspb.Double(4242),
		},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	info := updated.GetSpaceship().GetInfo()
	if info.GetName() != "Renamed Ship" || info.GetPrice() != 4242 {
		t.Errorf("Update не применил поля: name=%q price=%v", info.GetName(), info.GetPrice())
	}

	// Незаданные поля должны остаться прежними — это частичное обновление
	if info.GetModel() != validInfo().GetModel() {
		t.Errorf("Update затер model: %q", info.GetModel())
	}

	// И двигатель тоже не должен был потеряться
	if info.GetWarpDrive() == nil {
		t.Error("Update затер oneof engine, хотя в запросе его не было")
	}
}

func TestUpdateNotFound(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestClient(t)

	_, err := client.Update(ctx, &spaceshipV1.UpdateRequest{
		Uuid:       "00000000-0000-0000-0000-000000000000",
		UpdateInfo: &spaceshipV1.SpaceshipUpdateInfo{Name: wrapperspb.String("Nope")},
	})
	requireCode(t, err, codes.NotFound)

	// update_info обязателен по контракту (validate.rules.message.required)
	_, err = client.Update(ctx, &spaceshipV1.UpdateRequest{Uuid: "any"})
	requireCode(t, err, codes.InvalidArgument)
}

func TestSoftDelete(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestClient(t)
	shipUUID := createShip(t, ctx, client)

	if _, err := client.Delete(ctx, &spaceshipV1.DeleteRequest{Uuid: shipUUID}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// После мягкого удаления корабля для API больше нет
	_, err := client.Get(ctx, &spaceshipV1.GetRequest{Uuid: shipUUID})
	requireCode(t, err, codes.NotFound)

	_, err = client.Update(ctx, &spaceshipV1.UpdateRequest{
		Uuid:       shipUUID,
		UpdateInfo: &spaceshipV1.SpaceshipUpdateInfo{Name: wrapperspb.String("Zombie")},
	})
	requireCode(t, err, codes.NotFound)

	// Повторное удаление — тоже NotFound, а не молчаливый OK
	_, err = client.Delete(ctx, &spaceshipV1.DeleteRequest{Uuid: shipUUID})
	requireCode(t, err, codes.NotFound)

	// И удаление того, чего никогда не было
	_, err = client.Delete(ctx, &spaceshipV1.DeleteRequest{Uuid: "00000000-0000-0000-0000-000000000000"})
	requireCode(t, err, codes.NotFound)
}

func TestServerIDHeader(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestClient(t)

	// Регресс: ServerID-интерцептор вызывал SetHeader уже ПОСЛЕ того, как
	// Metadata-интерцептор сделал SendHeader. SetHeader возвращал ошибку, она
	// затирала результат хендлера — и КАЖДЫЙ unary-вызов падал с
	// Internal "SendHeader called multiple times", а server-id не доезжал никогда.
	var header metadata.MD

	_, err := client.Create(ctx, &spaceshipV1.CreateRequest{Info: validInfo()}, grpc.Header(&header))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got := header.Get("server-id")
	if len(got) == 0 || got[0] != testServerID {
		t.Errorf("server-id в headers = %v, ожидали [%s]", got, testServerID)
	}

	if len(header.Get("server-version")) == 0 {
		t.Error("server-version не доехал в headers")
	}
}

func TestHandlerErrorIsNotReplaced(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestClient(t)

	// Регресс на ту же поломку с другой стороны: ошибка хендлера должна доезжать
	// как есть, а не подменяться ошибкой отправки headers.
	_, err := client.Get(ctx, &spaceshipV1.GetRequest{Uuid: "00000000-0000-0000-0000-000000000000"})
	requireCode(t, err, codes.NotFound)

	if status.Code(err) == codes.Internal {
		t.Error("ошибка хендлера подменена внутренней ошибкой интерцептора")
	}
}

func TestWatchUpdatesReceivesEvents(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client := newTestClient(t)

	stream, err := client.WatchUpdates(ctx, &spaceshipV1.WatchUpdatesRequest{})
	if err != nil {
		t.Fatalf("WatchUpdates: %v", err)
	}

	// Ждем headers — по ним видно, что подписка на сервере уже зарегистрирована
	// и события не потеряются. Без этого тест был бы гонкой.
	if _, err = stream.Header(); err != nil {
		t.Fatalf("Header: %v", err)
	}

	shipUUID := createShip(t, ctx, client)

	event := recvEvent(t, stream)
	if event.GetEventType() != spaceshipV1.EventType_EVENT_TYPE_CREATED {
		t.Errorf("ожидали CREATED, получили %s", event.GetEventType())
	}

	if event.GetSpaceship().GetUuid() != shipUUID {
		t.Errorf("событие про %q, ожидали %q", event.GetSpaceship().GetUuid(), shipUUID)
	}

	if _, err = client.Delete(ctx, &spaceshipV1.DeleteRequest{Uuid: shipUUID}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	event = recvEvent(t, stream)
	if event.GetEventType() != spaceshipV1.EventType_EVENT_TYPE_DELETED {
		t.Errorf("ожидали DELETED, получили %s", event.GetEventType())
	}

	// В событии удаления едет tombstone целиком, включая deleted_at
	if event.GetSpaceship().GetDeletedAt() == nil {
		t.Error("в событии DELETED нет deleted_at")
	}
}

func TestWatchUpdatesFiltersByUUID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client := newTestClient(t)

	watched := createShip(t, ctx, client)
	other := createShip(t, ctx, client)

	stream, err := client.WatchUpdates(ctx, &spaceshipV1.WatchUpdatesRequest{
		Uuid: wrapperspb.String(watched),
	})
	if err != nil {
		t.Fatalf("WatchUpdates: %v", err)
	}

	if _, err = stream.Header(); err != nil {
		t.Fatalf("Header: %v", err)
	}

	// Трогаем сначала НЕотслеживаемый корабль, потом отслеживаемый.
	// Если фильтр не работает, первым придет событие про other.
	if _, err = client.Delete(ctx, &spaceshipV1.DeleteRequest{Uuid: other}); err != nil {
		t.Fatalf("Delete other: %v", err)
	}

	if _, err = client.Delete(ctx, &spaceshipV1.DeleteRequest{Uuid: watched}); err != nil {
		t.Fatalf("Delete watched: %v", err)
	}

	event := recvEvent(t, stream)
	if event.GetSpaceship().GetUuid() != watched {
		t.Errorf("фильтр пропустил событие про %q, подписка была на %q",
			event.GetSpaceship().GetUuid(), watched)
	}
}

// recvEvent читает одно событие из стрима с таймаутом
func recvEvent(t *testing.T, stream spaceshipV1.SpaceshipService_WatchUpdatesClient) *spaceshipV1.UpdateEvent {
	t.Helper()

	type result struct {
		resp *spaceshipV1.WatchUpdatesResponse
		err  error
	}

	ch := make(chan result, 1)

	go func() {
		resp, err := stream.Recv()
		ch <- result{resp: resp, err: err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			if errors.Is(res.err, io.EOF) {
				t.Fatal("стрим закрылся, события не дождались")
			}

			t.Fatalf("Recv: %v", res.err)
		}

		return res.resp.GetUpdateEvent()

	case <-time.After(eventWait):
		t.Fatal("событие не пришло за отведенное время")

		return nil
	}
}

func TestConcurrentGetAndUpdate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newTestClient(t)
	shipUUID := createShip(t, ctx, client)

	// Регресс на гонку данных: сервис отдавал наружу тот же *Spaceship, что лежал
	// в хранилище, и Update мутировал его на месте — параллельный Get маршалил
	// сообщение, пока Update в него писал. Под -race тест краснел бы стабильно.
	const workers = 20

	var wg sync.WaitGroup

	for i := range workers {
		wg.Go(func() {
			_, err := client.Update(ctx, &spaceshipV1.UpdateRequest{
				Uuid:       shipUUID,
				UpdateInfo: &spaceshipV1.SpaceshipUpdateInfo{Price: wrapperspb.Double(float64(i))},
			})
			if err != nil {
				t.Errorf("Update: %v", err)
			}
		})

		wg.Go(func() {
			if _, err := client.Get(ctx, &spaceshipV1.GetRequest{Uuid: shipUUID}); err != nil {
				t.Errorf("Get: %v", err)
			}
		})
	}

	wg.Wait()
}
