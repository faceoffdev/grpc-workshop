# gRPC Client - Базовый CRUD клиент

Демонстрационный клиент, показывающий основные операции CRUD через gRPC.

## Что демонстрирует

### 1. Основные CRUD операции
- **Create** - создание нового космического корабля
- **Get** - получение корабля по UUID
- **Update** - обновление существующего корабля
- **Delete** - удаление корабля

### 2. Работа с oneof (разные типы двигателей)
Методы клиента поддерживают четыре варианта поля `oneof` в Protobuf:
- **IonEngine** - ионный двигатель
- **WarpDrive** - варп-двигатель
- **NuclearEngine** - ядерный двигатель
- **AntimatterReactor** - антиматериальный реактор

Запускаемый `RunDemo` создаёт корабль с `WarpDrive` и при обновлении заменяет двигатель на `IonEngine`.

### 3. Обработка ошибок
- Проверка gRPC статус-кодов
- Обработка ошибок NotFound

Ошибки валидации проверяются в [тестах сервера](../grpc_server/main_test.go) и через `task test:grpc:create`; отдельного сценария с некорректным запросом в этом клиенте нет.

### 4. Работа с опциональными полями
Использование `google.protobuf.StringValue` и `google.protobuf.DoubleValue` для частичного обновления.

### 5. Таймауты вызовов

Каждый CRUD-вызов использует `context.WithTimeout` с ограничением в 10 секунд. Отдельной симуляции медленного сервера в демо нет.

## Запуск

Из корня репозитория запустите `task run:server`, затем в другом терминале — `task run:client`.

## Примеры кода

### Создание корабля с варп-двигателем
```go
spaceship, err := client.CreateSpaceship(
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
```

### Обновление только некоторых полей
```go
newName := "Enterprise NX-01 (Upgraded)"
newPrice := 175000000.00

spaceship, err := client.UpdateSpaceship(
    ctx,
    spaceshipUUID,
    &newName,    // Обновляем имя
    &newPrice,   // Обновляем цену
    &spaceshipV1.IonEngine{  // Меняем двигатель
        ThrustNewtons:  20000.0,
        PropellantType: "Аргон",
        FuelEfficiency: 3500.0,
    },
)
```

### Обработка ошибок NotFound
```go
_, err := client.GetSpaceship(ctx, uuid)
if err != nil {
    if st, ok := status.FromError(err); ok {
        if st.Code() == codes.NotFound {
            fmt.Printf("Корабль не найден: %s\n", st.Message())
        }
    }
}
```

## Структура клиента

```
SpaceshipClient
├── CreateSpaceship() - создание корабля
├── GetSpaceship()    - получение корабля
├── UpdateSpaceship() - обновление корабля
├── DeleteSpaceship() - удаление корабля
└── RunDemo()         - полная демонстрация
```

## Смотрите также

- [gRPC Streaming Client](../grpc_streaming_client/README.md) - работа со streaming
- [gRPC Headers Client](../grpc_headers_client/README.md) - работа с metadata
