package main

import (
	"context"
	"fmt"
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
	requestTimeout = 10 * time.Second
)

// SpaceshipClient обертка над gRPC клиентом
type SpaceshipClient struct {
	conn   *grpc.ClientConn
	client spaceshipV1.SpaceshipServiceClient
}

// NewSpaceshipClient создает новый клиент для работы с космическими кораблями
func NewSpaceshipClient(serverAddr string) (*SpaceshipClient, error) {
	fmt.Printf("🔌 Подключаемся к серверу %s...\n", serverAddr)

	conn, err := grpc.NewClient(
		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &SpaceshipClient{
		conn:   conn,
		client: spaceshipV1.NewSpaceshipServiceClient(conn),
	}, nil
}

// Close закрывает соединение с сервером.
// grpc.ClientConn держит живые TCP-соединения и фоновые горутины — его положено
// закрывать, даже если процесс сразу завершается: в длинной программе утечка была бы
// настоящей, и приучаться стоит сразу.
func (c *SpaceshipClient) Close() error {
	return c.conn.Close()
}

// CreateSpaceship создает новый космический корабль
// engine может быть: *spaceshipV1.IonEngine, *spaceshipV1.WarpDrive,
// *spaceshipV1.NuclearEngine, *spaceshipV1.AntimatterReactor или nil
func (c *SpaceshipClient) CreateSpaceship(
	ctx context.Context,
	name string,
	description string,
	model string,
	manufacturer string,
	price float64,
	engine any,
) (*spaceshipV1.Spaceship, error) {
	fmt.Printf("🚀 Создаем корабль: %s\n", name)

	// Дедлайн на каждый RPC: без него вызов висит, пока жив контекст вызывающего.
	// В gRPC дедлайн едет на сервер вместе с запросом — сервер тоже увидит отмену.
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	info := &spaceshipV1.SpaceshipInfo{
		Name:         name,
		Description:  wrapperspb.String(description),
		Model:        model,
		Manufacturer: manufacturer,
		Price:        price,
	}

	// Устанавливаем двигатель через type switch для oneof
	if engine != nil {
		switch e := engine.(type) {
		case *spaceshipV1.IonEngine:
			info.Engine = &spaceshipV1.SpaceshipInfo_IonEngine{IonEngine: e}
			fmt.Printf("     Двигатель: Ионный (тяга %.0f Н)\n", e.GetThrustNewtons())
		case *spaceshipV1.WarpDrive:
			info.Engine = &spaceshipV1.SpaceshipInfo_WarpDrive{WarpDrive: e}
			fmt.Printf("     Двигатель: Варп-двигатель (фактор %d)\n", e.GetWarpFactor())
		case *spaceshipV1.NuclearEngine:
			info.Engine = &spaceshipV1.SpaceshipInfo_NuclearEngine{NuclearEngine: e}
			fmt.Printf("     Двигатель: Ядерный (%.0f МВт)\n", e.GetReactorPowerMw())
		case *spaceshipV1.AntimatterReactor:
			info.Engine = &spaceshipV1.SpaceshipInfo_AntimatterReactor{AntimatterReactor: e}
			fmt.Printf("     Двигатель: Антиматериальный (%.2f ТВт)\n", e.GetPowerOutputTw())
		default:
			fmt.Printf("     ⚠️  Неизвестный тип двигателя\n")
		}
	}

	req := &spaceshipV1.CreateRequest{Info: info}

	resp, err := c.client.Create(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create spaceship: %w", err)
	}

	spaceship := resp.GetSpaceship()
	fmt.Printf("  ✅ Создан корабль с UUID: %s\n", spaceship.GetUuid())
	fmt.Printf("     Название: %s\n", spaceship.GetInfo().GetName())
	fmt.Printf("     Модель: %s\n", spaceship.GetInfo().GetModel())
	fmt.Printf("     Цена: %.2f\n", spaceship.GetInfo().GetPrice())

	return spaceship, nil
}

// GetSpaceship получает космический корабль по UUID
func (c *SpaceshipClient) GetSpaceship(ctx context.Context, uuid string) (*spaceshipV1.Spaceship, error) {
	fmt.Printf("🔍 Получаем корабль с UUID: %s\n", uuid)

	// Дедлайн на каждый RPC: без него вызов висит, пока жив контекст вызывающего.
	// В gRPC дедлайн едет на сервер вместе с запросом — сервер тоже увидит отмену.
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req := &spaceshipV1.GetRequest{
		Uuid: uuid,
	}

	resp, err := c.client.Get(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				fmt.Printf("  ❌ Корабль не найден: %s\n", st.Message())
				return nil, err
			}
		}

		return nil, fmt.Errorf("failed to get spaceship: %w", err)
	}

	spaceship := resp.GetSpaceship()
	fmt.Printf("  ✅ Найден корабль: %s\n", spaceship.GetInfo().GetName())
	fmt.Printf("     Описание: %s\n", spaceship.GetInfo().GetDescription().GetValue())
	fmt.Printf("     Производитель: %s\n", spaceship.GetInfo().GetManufacturer())

	return spaceship, nil
}

// UpdateSpaceship обновляет существующий космический корабль
// newName и newPrice могут быть nil, если не требуется обновление
// engine может быть: *spaceshipV1.IonEngine, *spaceshipV1.WarpDrive,
// *spaceshipV1.NuclearEngine, *spaceshipV1.AntimatterReactor или nil
func (c *SpaceshipClient) UpdateSpaceship(
	ctx context.Context,
	uuid string,
	newName *string,
	newPrice *float64,
	newEngine any,
) (*spaceshipV1.Spaceship, error) {
	fmt.Printf("✏️  Обновляем корабль с UUID: %s\n", uuid)

	// Дедлайн на каждый RPC: без него вызов висит, пока жив контекст вызывающего.
	// В gRPC дедлайн едет на сервер вместе с запросом — сервер тоже увидит отмену.
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	updateInfo := &spaceshipV1.SpaceshipUpdateInfo{}

	// Обновляем имя, если указано
	if newName != nil {
		updateInfo.Name = wrapperspb.String(*newName)
		fmt.Printf("     Новое имя: %s\n", *newName)
	}

	// Обновляем цену, если указана
	if newPrice != nil {
		updateInfo.Price = wrapperspb.Double(*newPrice)
		fmt.Printf("     Новая цена: %.2f\n", *newPrice)
	}

	// Обновляем двигатель через type switch для oneof
	if newEngine != nil {
		switch e := newEngine.(type) {
		case *spaceshipV1.IonEngine:
			updateInfo.Engine = &spaceshipV1.SpaceshipUpdateInfo_IonEngine{IonEngine: e}
			fmt.Printf("     Новый двигатель: Ионный (тяга %.0f Н)\n", e.GetThrustNewtons())
		case *spaceshipV1.WarpDrive:
			updateInfo.Engine = &spaceshipV1.SpaceshipUpdateInfo_WarpDrive{WarpDrive: e}
			fmt.Printf("     Новый двигатель: Варп-двигатель (фактор %d)\n", e.GetWarpFactor())
		case *spaceshipV1.NuclearEngine:
			updateInfo.Engine = &spaceshipV1.SpaceshipUpdateInfo_NuclearEngine{NuclearEngine: e}
			fmt.Printf("     Новый двигатель: Ядерный (%.0f МВт)\n", e.GetReactorPowerMw())
		case *spaceshipV1.AntimatterReactor:
			updateInfo.Engine = &spaceshipV1.SpaceshipUpdateInfo_AntimatterReactor{AntimatterReactor: e}
			fmt.Printf("     Новый двигатель: Антиматериальный (%.2f ТВт)\n", e.GetPowerOutputTw())
		default:
			fmt.Printf("     ⚠️  Неизвестный тип двигателя\n")
		}
	}

	req := &spaceshipV1.UpdateRequest{
		Uuid:       uuid,
		UpdateInfo: updateInfo,
	}

	resp, err := c.client.Update(ctx, req)
	if err != nil {
		// Раньше return стоял ВНУТРИ `if ok`, и когда status.FromError не опознавал
		// ошибку, выполнение проваливалось дальше на resp == nil — функция возвращала
		// (nil, nil), молча проглотив ошибку.
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			fmt.Printf("  ❌ Корабль не найден для обновления: %s\n", st.Message())
			return nil, err
		}

		return nil, fmt.Errorf("failed to update spaceship: %w", err)
	}

	spaceship := resp.GetSpaceship()
	fmt.Printf("  ✅ Обновлен корабль: %s\n", spaceship.GetInfo().GetName())
	fmt.Printf("     Цена: %.2f\n", spaceship.GetInfo().GetPrice())

	return spaceship, nil
}

// DeleteSpaceship удаляет космический корабль
func (c *SpaceshipClient) DeleteSpaceship(ctx context.Context, uuid string) error {
	fmt.Printf("🗑️  Удаляем корабль с UUID: %s\n", uuid)

	// Дедлайн на каждый RPC: без него вызов висит, пока жив контекст вызывающего.
	// В gRPC дедлайн едет на сервер вместе с запросом — сервер тоже увидит отмену.
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req := &spaceshipV1.DeleteRequest{
		Uuid: uuid,
	}

	_, err := c.client.Delete(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				fmt.Printf("  ❌ Корабль не найден для удаления: %s\n", st.Message())
				return err
			}
		}
		return fmt.Errorf("failed to delete spaceship: %w", err)
	}

	fmt.Printf("  ✅ Корабль успешно удален\n")
	return nil
}

// RunDemo запускает демонстрацию всех операций CRUD
func (c *SpaceshipClient) RunDemo(ctx context.Context) error {
	fmt.Println("🎬 Начинаем демонстрацию gRPC клиента...")
	fmt.Println(strings.Repeat("=", 52))

	// 1. Создаем корабль с варп-двигателем
	spaceship, err := c.CreateSpaceship(
		ctx,
		"Enterprise NX-01",
		"Первый корабль класса NX с варп-двигателем",
		"NX-01",
		"Earth Starfleet",
		150000000.00,
		&spaceshipV1.WarpDrive{
			WarpFactor:        9,
			FieldStability:    0.98,
			DilithiumCrystals: true,
		},
	)
	if err != nil {
		return fmt.Errorf("demo failed on create: %w", err)
	}
	fmt.Println()

	// 2. Получаем созданный корабль
	_, err = c.GetSpaceship(ctx, spaceship.GetUuid())
	if err != nil {
		return fmt.Errorf("demo failed on get: %w", err)
	}
	fmt.Println()

	// 3. Обновляем корабль - меняем имя, цену и устанавливаем ионный двигатель
	newName := "Enterprise NX-01 (Upgraded)"
	newPrice := 175000000.00
	_, err = c.UpdateSpaceship(
		ctx,
		spaceship.GetUuid(),
		&newName,
		&newPrice,
		&spaceshipV1.IonEngine{
			ThrustNewtons:  20000.0,
			PropellantType: "Аргон",
			FuelEfficiency: 3500.0,
		},
	)
	if err != nil {
		return fmt.Errorf("demo failed on update: %w", err)
	}
	fmt.Println()

	// 4. Проверяем обновленный корабль
	fmt.Println("🔍 Проверяем обновленные данные:")
	updatedShip, err := c.GetSpaceship(ctx, spaceship.GetUuid())
	if err != nil {
		return fmt.Errorf("demo failed on get updated: %w", err)
	}
	fmt.Println()

	// 5. Удаляем корабль
	err = c.DeleteSpaceship(ctx, updatedShip.GetUuid())
	if err != nil {
		return fmt.Errorf("demo failed on delete: %w", err)
	}
	fmt.Println()

	// 6. Проверяем, что корабль действительно удален
	fmt.Println("🔍 Проверяем, что корабль удален:")
	_, err = c.GetSpaceship(ctx, spaceship.GetUuid())
	if err != nil {
		// Ожидаем ошибку NotFound
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			fmt.Println("  ✅ Корабль действительно удален")
		}
	}
	fmt.Println()

	// 7. Демонстрируем обработку ошибок
	fmt.Println("🧪 Демонстрируем обработку ошибок:")
	err = c.DeleteSpaceship(ctx, "00000000-0000-0000-0000-000000000000")
	if err != nil {
		fmt.Println("  ✅ Корректно обработана ошибка удаления несуществующего корабля")
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("❌ %v", err)
	}

	fmt.Println("🎉 Демонстрация завершена успешно!")
}

// run содержит логику демо: вынесено из main, чтобы log.Fatal не съедал
// отложенное закрытие соединения (defer не выполняется при os.Exit).
func run() error {
	client, err := NewSpaceshipClient(serverAddr)
	if err != nil {
		return fmt.Errorf("не удалось создать клиент: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("⚠️  Ошибка при закрытии соединения: %v", closeErr)
		}
	}()

	if err = client.RunDemo(context.Background()); err != nil {
		return fmt.Errorf("ошибка в демонстрации: %w", err)
	}

	return nil
}
