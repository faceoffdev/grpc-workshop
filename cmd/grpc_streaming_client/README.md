# gRPC Streaming Client

Демонстрация работы с gRPC Server Streaming для получения обновлений в реальном времени.

## Что демонстрирует

### 1. Server Streaming
Подписка на stream обновлений от сервера:
- Получение событий создания кораблей
- Получение событий обновления кораблей
- Получение событий удаления кораблей

### 2. Типы событий
- `EVENT_TYPE_CREATED` - корабль создан
- `EVENT_TYPE_UPDATED` - корабль обновлен
- `EVENT_TYPE_DELETED` - корабль удален

### 3. Фильтрация событий
- Подписка на **все корабли** (без фильтра)
- Подписка на **конкретный корабль** (по UUID)

### 4. Завершение подписки

- Ограничение времени жизни стрима через `context.WithTimeout`
- Явная отмена контекста после операций демо
- Ожидание завершения читающей горутины и обработка `io.EOF`, `Canceled` и `DeadlineExceeded`

Оба сценария запускаются автоматически: сначала `RunStreamingDemo`, затем `RunFilteredWatchDemo`. Обработчик сигналов Ctrl-C и автоматическое переподключение в клиенте не реализованы.

## Запуск

Из корня репозитория запустите `task run:server`, затем в другом терминале — `task run:client:streaming`. Клиент сам создаёт, обновляет и удаляет корабли для демонстрации событий.

## Примеры кода

### Подписка на все обновления
```go
stream, err := client.WatchUpdates(ctx, &spaceshipV1.WatchUpdatesRequest{
    // Uuid не указан - получаем события для всех кораблей
})

for {
    event, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        return err
    }
    
    fmt.Printf("Событие: %s, Корабль: %s\n",
        event.GetUpdateEvent().GetEventType(),
        event.GetUpdateEvent().GetSpaceship().GetUuid())
}
```

### Подписка на конкретный корабль
```go
stream, err := client.WatchUpdates(ctx, &spaceshipV1.WatchUpdatesRequest{
    Uuid: wrapperspb.String(spaceshipUUID),
})
```

### Отмена подписки в демо
```go
// После завершения CRUD-операций отменяем контекст подписки.
streamCancel()
if err := awaitStreamShutdown(streamDone); err != nil {
    return err
}
```

## Архитектура

```
Client                          Server
  │                               │
  ├──WatchUpdates(uuid?)─────────>│
  │                               │
  │<──────EVENT_TYPE_CREATED──────┤
  │                               │
  │<──────EVENT_TYPE_UPDATED──────┤
  │                               │
  │<──────EVENT_TYPE_DELETED──────┤
  │                               │
  │              ...              │
```

## Серверная реализация

На сервере используется паттерн **Pub/Sub**:
- Подписчики регистрируются в `subscriptions` map
- При изменении корабля событие отправляется всем подписчикам
- Поддержка фильтрации по UUID

```go
type subscription struct {
    id         string
    uuid       *string  // nil = все корабли
    eventChan  chan *WatchUpdatesResponse
    cancelChan chan struct{}
}
```

## Use-cases для Server Streaming

1. **Real-time уведомления** - мониторинг изменений
2. **Live updates** - обновление UI в реальном времени
3. **Мониторинг систем** - метрики, логи, алерты
4. **Chat-приложения** - получение сообщений
5. **IoT данные** - телеметрия с устройств

## Смотрите также

- [gRPC Client](../grpc_client/README.md) - базовые CRUD операции
- [gRPC Headers Client](../grpc_headers_client/README.md) - работа с metadata
