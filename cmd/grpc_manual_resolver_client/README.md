## Клиентская балансировка (Manual Resolver)

Клиент `grpc_manual_resolver_client` показывает, как распределять запросы между несколькими серверами через `manual.Resolver` и `round_robin`.

### Запуск и сценарий

Из корня репозитория:

```bash
# Терминал 1: четыре экземпляра сервера на портах 50051–50054
task run:cluster
```

```bash
# Терминал 2
task run:client:lb
```

Одиночный сервер на порту `50051` перед запуском кластера нужно остановить.

В [main.go](main.go) запускаются две фазы:

1. 30 вызовов `Create` (10 воркеров по 3 операции) на три адреса.
2. Через 10 секунд клиент добавляет четвёртый адрес через `UpdateState` и выполняет ещё 40 вызовов (10 воркеров по 4 операции).

Четвёртый сервер уже работает в кластере; в демо меняется список адресов клиента. Каждый вызов ограничен таймаутом в 10 секунд. Статистика собирается по header `server-id`; итоговая таблица содержит накопленные результаты обеих фаз. У серверов отдельные хранилища в памяти.

Выполняемые сценарии — балансировка и изменение списка адресов. Отключение серверов, retry policy, health checking и внешнее service discovery в демо не настраиваются.

### Что такое resolver

Когда создаешь gRPC клиент, ты даешь ему адрес сервера, например `localhost:50051`. Но что если серверов несколько? Resolver - это компонент, который превращает одно имя в список реальных адресов серверов.

**Типы resolvers:**

- **dns** - берет адреса из DNS (например, DNS возвращает 3 IP для одного домена)
- **manual** - ты сам указываешь список адресов в коде
- **custom** - можно написать свой (например, читать из Consul или etcd)

В примере используется manual resolver - просто даем ему список адресов напрямую.

### Что такое схема (scheme)

Схема - это префикс в адресе, как `http://` или `ftp://`. В gRPC она определяет какой resolver использовать.

```go
manual.NewBuilderWithScheme("lb")  // создаем resolver со схемой "lb"
```

Теперь адрес `lb:///unused` говорит gRPC: "используй resolver со схемой 'lb'".

**Встроенные схемы:**
- `dns:///` - DNS resolver
- `unix:///` - Unix socket
- `passthrough:///` - использует адрес как есть (по умолчанию)

**Твоя схема:**
- `lb:///` - наш manual resolver
- Имя `lb` произвольное, можешь назвать `myscheme`, `custom`, что угодно
- Главное - не конфликтовать с встроенными схемами

Часть после `///` (`unused` в примере) не используется manual resolver'ом - он игнорирует это и берет адреса из `UpdateState`.

### Как это работает

```go
// 1. Создаем manual resolver
mr := manual.NewBuilderWithScheme("lb")

// 2. Создаем соединение с конфигом балансировки
conn, err := grpc.NewClient(
    mr.Scheme()+":///unused",  // "lb:///unused" - схема для нашего resolver
    grpc.WithResolvers(mr),    // говорим использовать наш resolver
    grpc.WithDefaultServiceConfig(`{
        "loadBalancingConfig": [{"round_robin":{}}]
    }`),
)

// 3. Даем resolver список адресов
mr.UpdateState(resolver.State{
    Addresses: []resolver.Address{
        {Addr: "localhost:50051"},
        {Addr: "localhost:50052"},
        {Addr: "localhost:50053"},
    },
})
```

Теперь gRPC:
1. Создаст соединение к каждому серверу из списка
2. При каждом запросе будет выбирать следующий сервер по кругу (round_robin)
3. Если какой-то сервер упал, автоматически пропустит его

### Почему конфиг в виде JSON строки

Конфиг балансировки - это строка `{"loadBalancingConfig": [{"round_robin":{}}]}`, а не обычные Go опции.

**Причины:**

1. **Кросс-языковая совместимость** - gRPC работает на 10+ языках. JSON понимают все языки одинаково. Go структуры работают только в Go.

2. **Динамическое получение** - конфиг может прийти от DNS или service discovery во время работы:
   ```bash
   # DNS TXT запись может вернуть конфиг:
   _grpc_config.myservice.com TXT "grpc_config={\"loadBalancingPolicy\":\"round_robin\"}"
   ```
   Resolver получает эту строку и gRPC применяет её.

3. **Изменение на лету** - конфиг можно менять без пересоздания клиента.

### Параметры балансировки

**Политики:**

```go
// round_robin - выбирает по кругу среди готовых соединений
grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`)

// pick_first - использует первый доступный сервер, переключается только при падении
grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"pick_first":{}}]}`)
```

### Динамическое обновление

Список серверов можно менять прямо во время работы:

```go
// Добавляем новый сервер
mr.UpdateState(resolver.State{
    Addresses: []resolver.Address{
        {Addr: "localhost:50051"},
        {Addr: "localhost:50052"},
        {Addr: "localhost:50053"},
        {Addr: "localhost:50054"},  // новый сервер
    },
})
```

gRPC:
1. Создаст соединение к новому серверу
2. Начнет отправлять туда запросы
3. Старые соединения останутся работать

Можно и убирать серверы - gRPC закроет соединения к ним.

### Пример из демо

```go
// Фаза 1: работаем с 3 серверами
client, _ := NewLoadBalancedClient([]string{
    "localhost:50051",
    "localhost:50052", 
    "localhost:50053",
})

// Фаза 1: выполняем 30 запросов на три адреса

// Через 10 секунд добавляем адрес уже запущенного 4-го сервера
client.UpdateServerAddresses([]string{
    "localhost:50051",
    "localhost:50052",
    "localhost:50053",
    "localhost:50054",  // новый
})

// Фаза 2: выполняем 40 запросов на четыре адреса
```

Клиент печатает фактическое распределение запросов. Числа могут отличаться от идеально равных долей, в том числе пока новые соединения устанавливаются.

### Когда использовать

- **Manual resolver** - если список серверов известен заранее или управляется вручную
- **DNS resolver** - если используешь Kubernetes headless service или DNS round-robin
- **Custom resolver** - если адреса берешь из Consul, etcd, или своей service registry

Для большинства случаев достаточно DNS или manual resolver.

## Полезные ссылки

- [Protocol Buffers Documentation](https://protobuf.dev/) - официальная документация
- [gRPC Go Quick Start](https://grpc.io/docs/languages/go/quickstart/) - быстрый старт с gRPC в Go
- [gRPC Load Balancing](https://grpc.io/docs/guides/load-balancing/) - официальная документация по балансировке
- [gRPC Name Resolution](https://github.com/grpc/grpc/blob/master/doc/naming.md) - как работают resolvers
- [buf.build](https://buf.build/) - документация buf
- [EasyP](https://easyp.tech/) - современный protobuf tooling
- [Google Protocol Buffers Go package](https://pkg.go.dev/google.golang.org/protobuf) - официальный Go пакет
