# gRPC на Go: практический воркшоп

Бесплатный практический фрагмент курса **[«Микросервисы на Go 3.0»](https://guide.olezhek28.courses/microservices/)** от [Олега Козырева](https://t.me/olezhek28go).

Здесь можно разобрать взаимодействие клиента и сервера на примере API космических кораблей: от protobuf-контракта и CRUD до стриминга, интерцепторов и клиентской балансировки. Запустите примеры, посмотрите, как запрос проходит через сервер, и попробуйте изменить контракт самостоятельно.

Проект подойдёт, если вы уже пишете бэкенд и хотите глубже разобраться в gRPC на Go — для текущей работы или перехода с другого стека. Для работы с примерами пригодятся основы Go, понимание `context` и горутин.

**[Подробнее о курсе и программе →](https://guide.olezhek28.courses/microservices/)**

## Что разберёте на практике

- **Контракты API:** protobuf, `oneof`, wrapper-поля и правила валидации — в [spaceship.proto](proto/spaceship/v1/spaceship.proto). Генерация настроена через [buf](proto/buf.gen.yaml) и [EasyP](proto/easyp.yaml).
- **Вызовы gRPC:** [CRUD-клиент](cmd/grpc_client/main.go) создаёт корабль с варп-двигателем, меняет его на ионный, обновляет отдельные поля и проверяет `NotFound` после удаления. Каждый RPC ограничен таймаутом в 10 секунд.
- **Server streaming:** [streaming-клиент](cmd/grpc_streaming_client/main.go) запускает два сценария: события создания, обновления и удаления всех кораблей, затем фильтрацию обновлений по UUID. Подписки завершаются отменой контекста.
- **Metadata и интерцепторы:** [headers-клиент](cmd/grpc_headers_client/main.go) передаёт metadata и читает headers и trailer с длительностью обработки. [Серверные интерцепторы](internal/interceptor/) обрабатывают metadata, валидируют запросы и измеряют время.
- **Клиентская балансировка:** [клиент с manual resolver](cmd/grpc_manual_resolver_client/main.go) отправляет запросы через `round_robin` сначала на три адреса, затем добавляет четвёртый и печатает статистику по `server-id`.
- **HTTP поверх gRPC:** [сервер](cmd/grpc_server/main.go) предоставляет REST API через gRPC-Gateway и Swagger UI из того же контракта.
- **Тестирование:** [тесты сервера](cmd/grpc_server/main_test.go) через `bufconn` проверяют CRUD, валидацию, headers, streaming и параллельное чтение с обновлением. `task test` запускает их с `-race`.

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Примеры клиентов](#примеры-клиентов)
- [Структура проекта](#структура-проекта)
- [API и устройство сервера](#api-и-устройство-сервера)
- [Генерация кода](#генерация-кода)
- [Проверки и команды](#проверки-и-команды)
- [Продолжение в курсе](#продолжение-в-курсе)

## Быстрый старт

Понадобятся **Go 1.27.0 или новее** (версия указана в [go.mod](go.mod)), [Task](https://taskfile.dev/installation/) и Git. При первом запуске нужен доступ к сети для скачивания Go-зависимостей.

```bash
git clone https://github.com/olezhek28/grpc-workshop.git
cd grpc-workshop

# Терминал 1: gRPC на :50051, HTTP API и Swagger UI на :8081
task run:server
```

В другом терминале из корня проекта:

```bash
# Терминал 2: создать, получить, обновить и удалить корабли
task run:client
```

В выводе клиента увидите результаты CRUD-операций и примеры работы с двигателями через `oneof`. Swagger UI доступен по адресу **<http://localhost:8081/swagger-ui.html>**.

Сгенерированный Go-код и OpenAPI-спецификация уже находятся в репозитории. Для первого запуска генерация не нужна; она понадобится после изменения `.proto`.

Сервер хранит данные в памяти: после перезапуска корабли и подписки исчезают. Остановить сервер можно через `Ctrl-C`.

## Примеры клиентов

При запущенном `task run:server` попробуйте клиентов по очереди:

| Пример | Команда | Что смотреть |
| --- | --- | --- |
| [CRUD](cmd/grpc_client/README.md) | `task run:client` | Вызовы Create/Get/Update/Delete, `oneof`, опциональные поля и ошибки |
| [Server streaming](cmd/grpc_streaming_client/README.md) | `task run:client:streaming` | События изменений, подписка на все корабли и фильтр по UUID |
| [Metadata](cmd/grpc_headers_client/README.md) | `task run:client:headers` | Передача metadata, получение headers и trailers |

### Клиентская балансировка

Остановите одиночный сервер, чтобы освободить порт `50051`, и запустите кластер:

```bash
# Терминал 1: четыре сервера на gRPC-портах 50051–50054
task run:cluster
```

```bash
# Терминал 2: из корня проекта
task run:client:lb
```

[Клиент с manual resolver](cmd/grpc_manual_resolver_client/README.md) показывает распределение запросов через `round_robin`, добавление сервера в пул и статистику по header `server-id`. У каждого экземпляра сервера своё хранилище в памяти. `Ctrl-C` в терминале кластера останавливает все экземпляры.

### Что попробовать самостоятельно

1. Измените правила валидации в `.proto`, перегенерируйте код и отправьте некорректный запрос. Посмотрите, какой статус получит клиент.
2. Проследите порядок unary-интерцепторов: где добавляется `server-id`, где отправляются headers и где проверяется запрос.
3. Сравните подписку на все события с подпиской на один UUID. Отмените контекст клиента и проследите завершение стрима.

## Структура проекта

```text
grpc-workshop/
├── proto/                            # Контракт API, конфигурации buf и EasyP
├── pkg/
│   ├── proto/                        # Сгенерированный Go-код
│   └── api/                          # Swagger UI и OpenAPI-спецификация
├── cmd/
│   ├── grpc_server/                  # gRPC-сервер и HTTP gateway
│   ├── grpc_client/                  # CRUD-клиент
│   ├── grpc_streaming_client/        # Server streaming
│   ├── grpc_headers_client/          # Metadata, headers и trailers
│   └── grpc_manual_resolver_client/  # Клиентская балансировка
├── internal/interceptor/             # Unary- и stream-интерцепторы
├── .github/workflows/                # CI
├── Taskfile.yaml                     # Запуск, генерация, инструменты и проверки
└── bin/                              # Локальные инструменты (в git не входят)
```

## API и устройство сервера

Контракт — [proto/spaceship/v1/spaceship.proto](proto/spaceship/v1/spaceship.proto), реализация — [cmd/grpc_server/main.go](cmd/grpc_server/main.go).

| RPC | Назначение | HTTP через gRPC-Gateway |
| --- | --- | --- |
| `Create` | Создать корабль | `POST /api/v1/spaceship` |
| `Get` | Получить корабль по UUID | `GET /api/v1/spaceship/{uuid}` |
| `Update` | Обновить переданные поля | `PATCH /api/v1/spaceship/{uuid}` |
| `Delete` | Выполнить мягкое удаление | `DELETE /api/v1/spaceship/{uuid}` |
| `WatchUpdates` | Подписаться на события изменений | Только gRPC |

`SpaceshipInfo` содержит название, описание, модель, производителя, цену и `oneof` для одного из четырёх типов двигателя. В `SpaceshipUpdateInfo` используются wrapper-поля: сервер различает отсутствие поля и явно переданное нулевое значение. Полная модель `Spaceship` дополнительно содержит UUID и временные метки.

При удалении сервер проставляет `deleted_at`; последующие Get, Update и Delete возвращают `NotFound`. Для безопасного параллельного чтения сервер при обновлении заменяет запись новой копией. События изменений отправляются подписчикам через Pub/Sub в памяти.

Unary- и stream-интерцепторы добавляют `server-id`, обрабатывают metadata, валидируют запросы и измеряют время обработки. В stream-интерцепторе валидация выполняется при `RecvMsg`. Swagger UI и спецификация встраиваются в бинарь через `go:embed`; сервер поддерживает graceful shutdown.

## Генерация кода

После изменения контракта используйте настроенный в проекте **buf**:

```bash
task proto:buf-gen
```

Команда устанавливает инструменты закреплённых версий в `./bin`, проверяет `.proto` и генерирует protobuf-модели, gRPC-клиент и серверный интерфейс, валидацию, gRPC-Gateway и OpenAPI-спецификацию.

Для альтернативной генерации настроен **EasyP**:

```bash
task proto:easyp-deps
task proto:easyp-gen
```

Конфигурации: [buf.gen.yaml](proto/buf.gen.yaml) и [easyp.yaml](proto/easyp.yaml). Версии локальных инструментов закреплены в [Taskfile.yaml](Taskfile.yaml).

## Проверки и команды

### Go-тесты

```bash
task test  # go test -race -count=1 ./...
```

Тесты сервера используют `bufconn`: вызовы проходят через gRPC, сериализацию и интерцепторы без TCP-порта.

### Проверка запущенного API

Для команд ниже запустите `task run:server`. Для задач `test:grpc:*` и `test:http:*` также понадобится `jq`; для HTTP-проверок — `curl`.

```bash
# grpcurl устанавливается локально в ./bin
task tools:install:test
./bin/grpcurl -plaintext localhost:50051 list

task test:grpc:all
task test:http:all
```

Пример создания корабля через HTTP:

```bash
curl -X POST http://localhost:8081/api/v1/spaceship \
  -H 'Content-Type: application/json' \
  -d '{"info":{"name":"Explorer","model":"EXP-001","manufacturer":"Workshop","price":100000}}'
```

### Работа с проектом

```bash
task --list           # Все доступные задачи
task build            # Сборка Go-пакетов
task format           # Форматирование Go-кода
task lint             # golangci-lint
task proto:buf-lint    # Проверка protobuf-контракта через buf
task ci               # Формат, линт, go.mod/go.sum, оба proto-линтера, сборка и тесты
task tools:outdated    # Проверка доступных обновлений инструментов
```

## Продолжение в курсе

В этом репозитории — один gRPC-сервис с хранилищем в памяти и четыре демонстрационных клиента. Это фрагмент курса **«Микросервисы на Go 3.0»** о взаимодействии через gRPC.

В полном курсе вы последовательно собираете систему из нескольких сервисов: проектируете взаимодействия, разделяете код по Clean Architecture, пишете тесты, подключаете PostgreSQL, Kafka и Redis, добавляете аутентификацию, метрики и трассировку, запускаете систему в Docker Compose. Эти темы относятся к программе полного курса.

Если вы уже пишете CRUD, но хотите разобраться, как связать сервисы в одну систему и почему выбираются конкретные решения, курс помогает пройти этот путь на практике. Для разработчиков с другого стека это возможность освоить подходы к разработке сервисов на Go на сквозном проекте.

**[Посмотреть программу и формат курса →](https://guide.olezhek28.courses/microservices/)**

Автор: [Олег Козырев](https://t.me/olezhek28go).

## Документация технологий

- [Protocol Buffers](https://protobuf.dev/)
- [gRPC](https://grpc.io/docs/)
- [gRPC-Go](https://github.com/grpc/grpc-go)
- [Buf](https://buf.build/docs/)
- [EasyP](https://easyp.tech/)
- [gRPC-Gateway](https://grpc-ecosystem.github.io/grpc-gateway/)
