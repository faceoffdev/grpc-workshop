# gRPC Headers (Metadata) - Концепции и примеры

Учебный пример для глубокого понимания работы с gRPC metadata (headers и trailers).

Запускаемый пример выполняет один unary-вызов `Create`: отправляет четыре поля metadata, получает headers и trailer `processing-duration-us`. Значение `authorization` демонстрационное; сервер логирует metadata, проверка токена и аутентификация здесь не реализованы. Разделы «Когда использовать» ниже описывают возможные применения metadata.

## Содержание

- [Что такое Metadata в gRPC](#что-такое-metadata-в-grpc)
- [Headers vs Trailers](#headers-vs-trailers)
- [Связь с HTTP/2](#связь-с-http2)
- [Когда использовать](#когда-использовать)
- [Практический пример](#практический-пример)
- [Запуск демонстрации](#запуск-демонстрации)

## Что такое Metadata в gRPC

**Metadata** - это пары ключ-значение, которые передаются вместе с gRPC запросами и ответами. Это аналог HTTP headers в обычном REST API, но с некоторыми особенностями.

### Основные типы Metadata

1. **Request Metadata** - отправляется клиентом к серверу
2. **Response Headers** - отправляется сервером к клиенту **ДО** обработки запроса
3. **Response Trailers** - отправляется сервером к клиенту **ПОСЛЕ** обработки запроса

```
Client ──────[Request + Metadata]──────> Server
       <─────[Response Headers]─────────
       <─────[Response Body]────────────
       <─────[Response Trailers]────────
```

## Headers vs Trailers

### Response Headers (Заголовки)

**Headers** отправляются **ДО** начала передачи данных ответа.

**Когда известны:** До обработки запроса или в самом начале.

**Используются для:**
- Версия сервера
- Время начала обработки
- Идентификатор сервера/инстанса
- Информация о маршрутизации
- Ранняя информация для клиента

**Пример:**
```go
// На сервере - отправляем ДО вызова handler
header := metadata.Pairs(
    "server-version", "1.0.0",
    "response-timestamp", time.Now().Format(time.RFC3339),
    "processed-by", "server-instance-01",
)
grpc.SendHeader(ctx, header)

// Теперь можем обрабатывать запрос
result := processRequest(req)
```

### Response Trailers (Завершающие заголовки)

**Trailers** отправляются **ПОСЛЕ** завершения передачи данных ответа.

**Когда известны:** Только после полной обработки запроса.

**Используются для:**
- Время обработки (известно только в конце)
- Количество обработанных записей
- Checksum данных
- Финальный статус операции
- Метрики и статистика

**Пример:**
```go
// На сервере - обрабатываем запрос
start := time.Now()
result := processRequest(req)
duration := time.Since(start)

// Отправляем trailers ПОСЛЕ обработки
trailer := metadata.Pairs(
    "processing-duration-us", strconv.Itoa(int(duration.Microseconds())),
    "records-processed", strconv.Itoa(result.Count),
)
grpc.SetTrailer(ctx, trailer)
```

### Ключевое отличие

| Аспект | Headers | Trailers |
|--------|---------|----------|
| **Когда отправляются** | ДО обработки | ПОСЛЕ обработки |
| **Что содержат** | Информация, известная заранее | Метрики, результаты |
| **Когда доступны клиенту** | Сразу после подключения | После получения всех данных |
| **Примеры** | server-version, timestamp | processing-time, status |

## Связь с HTTP/2

gRPC построен поверх **HTTP/2**, который нативно поддерживает headers и trailers через механизм **фреймов** (frames).

### HTTP/2 Frame Structure

HTTP/2 передает данные через последовательность фреймов:

```
┌─────────────────────────────────────────────┐
│  HEADERS Frame                              │
│  :status: 200                               │
│  content-type: application/grpc             │
│  server-version: 1.0.0          ← Headers   │
│  response-timestamp: 2025-...               │
└─────────────────────────────────────────────┘
               ↓
┌─────────────────────────────────────────────┐
│  DATA Frame #1                              │
│  [protobuf encoded message part 1]          │  ← Response Body
├─────────────────────────────────────────────┤
│  DATA Frame #2                              │
│  [protobuf encoded message part 2]          │
└─────────────────────────────────────────────┘
               ↓
┌─────────────────────────────────────────────┐
│  HEADERS Frame (with END_STREAM flag)       │
│  grpc-status: 0                             │
│  grpc-message: OK                           │
│  processing-duration-us: 342    ← Trailers  │
└─────────────────────────────────────────────┘
```

### Почему Headers отправляются раньше?

1. **Early Feedback** - клиент может принять решение до получения данных:
   ```go
   header, _ := stream.Header()
   if header.Get("server-overloaded")[0] == "true" {
       stream.CloseSend() // Отменяем запрос
       return
   }
   ```

2. **Streaming Efficiency** - можно начать обработку данных сразу:
   ```go
   // Headers приходят сразу
   header, _ := stream.Header()
   log.Printf("Started processing at: %s", header.Get("started-at"))
   
   // Данные идут потоком
   for {
       msg, err := stream.Recv()
       // Обрабатываем по мере поступления
   }
   ```

3. **HTTP/2 Multiplexing** - несколько запросов параллельно:
   ```
   Stream 1: HEADERS → DATA → DATA → TRAILERS
   Stream 3: HEADERS → DATA → TRAILERS
   Stream 5: HEADERS → DATA → DATA → DATA → TRAILERS
           (все идут параллельно по одному TCP соединению)
   ```

### gRPC Status всегда в Trailers

**Важно:** gRPC статус код (`grpc-status`) **всегда** передается в trailers, даже для unary запросов:

```
HEADERS Frame:
  :status: 200                    ← HTTP статус (всегда 200 для gRPC)
  content-type: application/grpc
  server-version: 1.0.0

DATA Frame:
  [protobuf message]

TRAILERS Frame:
  grpc-status: 0                  ← gRPC статус (0 = OK, 5 = NOT_FOUND, etc.)
  grpc-message: OK
  processing-duration-us: 342
```

## Когда использовать

### Request Metadata (от клиента)

**Use-cases:**
- **Трейсинг:** request-id, trace-id, span-id
- **Аутентификация:** токены, API ключи
- **Аудит:** user-id, session-id
- **Маршрутизация:** region, datacenter
- **Версионирование:** client-version, api-version

**Пример:**
```go
md := metadata.Pairs(
    "request-id", uuid.New().String(),
    "user-id", "user-123",
    "authorization", "Bearer token",
    "client-version", "1.0.0",
)
ctx := metadata.NewOutgoingContext(ctx, md)
client.Method(ctx, req)
```

### Response Headers (от сервера)

**Use-cases:**
- Версия сервера
- Идентификатор инстанса
- Время начала обработки
- Routing information
- Cache hints

**Пример:**
```go
header := metadata.Pairs(
    "server-version", "1.0.0",
    "instance-id", "server-01",
    "request-started-at", time.Now().Format(time.RFC3339),
)
grpc.SendHeader(ctx, header)
```

### Response Trailers (от сервера)

**Use-cases:**
- Время обработки запроса
- Количество обработанных записей
- Cache hit/miss статистика
- Checksum/hash результата
- Финальные метрики

**Пример:**
```go
trailer := metadata.Pairs(
    "processing-duration-us", strconv.Itoa(durationUs),
    "db-queries-count", "3",
    "cache-hit-rate", "0.85",
    "records-processed", "150",
)
grpc.SetTrailer(ctx, trailer)
```

## Практический пример

Этот клиент демонстрирует полный цикл работы с metadata:

### Что отправляет клиент (Request Metadata)
- `request-id` - уникальный ID для трейсинга
- `user-id` - идентификатор пользователя
- `authorization` - токен авторизации
- `client-version` - версия клиента

### Что получает клиент от сервера

**Headers (ДО обработки):**
- `server-version` - версия сервера
- `response-timestamp` - время начала обработки (RFC3339)
- `processed-by` - идентификатор сервера

**Trailers (ПОСЛЕ обработки):**
- `processing-duration-us` - время обработки в микросекундах

Серверный интерцептор устанавливает только `processing-duration-us`. Статус RPC обрабатывается через возвращаемую ошибку; в `main.go` дополнительно есть условное чтение `grpc-status`, но это не trailer, установленный нашим интерцептором.

### Архитектура взаимодействия

```
┌──────────┐                                    ┌──────────┐
│  Client  │                                    │  Server  │
└────┬─────┘                                    └────┬─────┘
     │                                               │
     │  1. Request + Metadata                        │
     │  (request-id, user-id, auth, version)         │
     ├──────────────────────────────────────────────>│
     │                                               │
     │                                               │ Interceptor:
     │  2. Response Headers (ДО обработки)           │ - Читает metadata
     │  (server-version, timestamp, processed-by)    │ - Логирует
     │<──────────────────────────────────────────────┤ - Отправляет headers
     │                                               │
     │                                               │ Handler:
     │                                               │ - Обрабатывает запрос
     │  3. Response Body (Data)                      │ - Создает корабль
     │  (созданный корабль)                          │
     │<──────────────────────────────────────────────┤
     │                                               │
     │                                               │ Interceptor:
     │  4. Response Trailers (ПОСЛЕ обработки)       │ - Измеряет время
     │  (processing-duration-us, grpc-status)        │ - Отправляет trailers
     │<──────────────────────────────────────────────┤
     │                                               │
```

## Реализация

### Клиентская сторона

**Отправка metadata:**
```go
md := metadata.Pairs("key", "value")
ctx := metadata.NewOutgoingContext(ctx, md)
```

**Получение headers и trailers:**
```go
var header, trailer metadata.MD
resp, err := client.Method(ctx, req, 
    grpc.Header(&header),   // Headers - приходят сразу
    grpc.Trailer(&trailer), // Trailers - приходят в конце
)

// Читаем headers
serverVersion := header.Get("server-version")[0]

// Читаем trailers
duration := trailer.Get("processing-duration-us")[0]
```

### Серверная сторона (интерсептор)

**Чтение входящих metadata:**
```go
md, ok := metadata.FromIncomingContext(ctx)
if ok {
    requestID := md.Get("request-id")[0]
}
```

**Отправка headers (ДО handler):**
```go
header := metadata.Pairs("server-version", "1.0.0")
grpc.SendHeader(ctx, header)
```

**Отправка trailers (ПОСЛЕ handler):**
```go
trailer := metadata.Pairs("processing-duration-us", "342")
grpc.SetTrailer(ctx, trailer)
```

## Интерсептор

Реализация в `internal/interceptor/metadata.go` показывает:
1. Извлечение и логирование входящих metadata
2. Отправку headers ДО вызова handler
3. Измерение времени обработки
4. Отправку trailers ПОСЛЕ вызова handler

## Полезные ссылки

- [gRPC Metadata Guide](https://grpc.io/docs/guides/metadata/)
- [HTTP/2 Specification](https://httpwg.org/specs/rfc7540.html)
- [grpc-go metadata package](https://pkg.go.dev/google.golang.org/grpc/metadata)
