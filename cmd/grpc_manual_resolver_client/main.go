package main

import (
	"context"
	"fmt"
	"log"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/protobuf/types/known/wrapperspb"

	spaceshipV1 "github.com/olezhek28/grpc-workshop/pkg/proto/spaceship/v1"
)

const (
	requestTimeout     = 10 * time.Second
	requestDelay       = 1 * time.Second
	phaseSwitchTimeout = 10 * time.Second
)

// LoadBalancedClient клиент с встроенной балансировкой через manual resolver
type LoadBalancedClient struct {
	conn     *grpc.ClientConn
	client   spaceshipV1.SpaceshipServiceClient
	resolver *manual.Resolver

	// Статистика по серверам (server ID -> количество запросов)
	mu          sync.RWMutex
	serverStats map[string]int64
}

// NewLoadBalancedClient создает клиент с manual resolver и round_robin балансировкой
func NewLoadBalancedClient(serverAddresses []string) (*LoadBalancedClient, error) {
	fmt.Printf("🔌 Настраиваем балансировку через manual resolver...\n")
	fmt.Printf("   Начальные серверы: %v\n", serverAddresses)

	// Создаем manual resolver с собственной схемой
	mr := manual.NewBuilderWithScheme("lb")

	// Создаем соединение с балансировкой round_robin
	conn, err := grpc.NewClient(
		mr.Scheme()+":///unused", // схема + любое имя (не используется для manual resolver)
		grpc.WithResolvers(mr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{
			"loadBalancingConfig": [{"round_robin":{}}]
		}`),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	// Формируем список адресов для resolver
	addrs := make([]resolver.Address, len(serverAddresses))
	for i, addr := range serverAddresses {
		addrs[i] = resolver.Address{Addr: addr}
	}

	// Обновляем состояние resolver с начальными адресами
	mr.UpdateState(resolver.State{
		Addresses: addrs,
	})

	fmt.Println("   ✅ Балансировка настроена (round_robin)")

	return &LoadBalancedClient{
		conn:        conn,
		client:      spaceshipV1.NewSpaceshipServiceClient(conn),
		resolver:    mr,
		serverStats: make(map[string]int64),
	}, nil
}

// Close закрывает соединение
func (c *LoadBalancedClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// UpdateServerAddresses динамически обновляет список серверов
func (c *LoadBalancedClient) UpdateServerAddresses(addrs []string) {
	fmt.Printf("\n🔄 Обновляем список серверов: %v\n", addrs)

	resolverAddrs := make([]resolver.Address, len(addrs))
	for i, addr := range addrs {
		resolverAddrs[i] = resolver.Address{Addr: addr}
	}

	c.resolver.UpdateState(resolver.State{
		Addresses: resolverAddrs,
	})

	fmt.Println("   ✅ Список серверов обновлен")
}

// CreateSpaceshipWithStats создает корабль и собирает статистику по серверам
func (c *LoadBalancedClient) CreateSpaceshipWithStats(
	ctx context.Context,
	workerID int,
	name string,
) (*spaceshipV1.Spaceship, string, error) {
	// Создаем контекст для получения response metadata
	var header metadata.MD

	req := &spaceshipV1.CreateRequest{
		Info: &spaceshipV1.SpaceshipInfo{
			Name:         name,
			Description:  wrapperspb.String(fmt.Sprintf("Корабль создан worker-%d для теста балансировки", workerID)),
			Model:        fmt.Sprintf("LB-TEST-%d", workerID),
			Manufacturer: "Load Balancer Corp",
			Price:        float64(workerID * 1000),
		},
	}

	// Выполняем запрос и получаем header с server-id
	resp, err := c.client.Create(ctx, req, grpc.Header(&header))
	if err != nil {
		return nil, "", fmt.Errorf("failed to create spaceship: %w", err)
	}

	// Извлекаем server ID из metadata
	serverID := "unknown"
	if values := header.Get("server-id"); len(values) > 0 {
		serverID = values[0]
	}

	// Обновляем статистику
	c.incrementServerStats(serverID)

	return resp.GetSpaceship(), serverID, nil
}

// incrementServerStats увеличивает счетчик запросов для сервера
func (c *LoadBalancedClient) incrementServerStats(serverID string) {
	c.mu.Lock()
	c.serverStats[serverID]++
	c.mu.Unlock()
}

// RunLoadBalancingDemo запускает демонстрацию балансировки с двумя фазами
func (c *LoadBalancedClient) RunLoadBalancingDemo(ctx context.Context) error {
	fmt.Println("\n🎬 Начинаем демонстрацию клиентской балансировки...")
	fmt.Println(strings.Repeat("=", 70))

	// Фаза 1: 30 запросов на 3 сервера
	fmt.Println("\n📍 ФАЗА 1: Балансировка на 3 сервера")
	fmt.Println("   Запускаем 30 запросов (10 воркеров × 3 операции)")
	fmt.Println()

	err := c.runPhase(ctx, 1, 10, 3)
	if err != nil {
		return fmt.Errorf("фаза 1 завершилась с ошибкой: %w", err)
	}

	c.printStatistics("ФАЗА 1")

	fmt.Printf("\n⏰ Ожидаем %v перед динамическим добавлением сервера...\n", phaseSwitchTimeout)
	if err = wait(ctx, phaseSwitchTimeout); err != nil {
		return err
	}

	// Динамическое добавление 4-го сервера
	c.UpdateServerAddresses([]string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
		"localhost:50054",
	})

	// Фаза 2: 40 запросов на 4 сервера
	fmt.Println("\n📍 ФАЗА 2: Балансировка на 4 сервера")
	fmt.Println("   Запускаем 40 запросов (10 воркеров × 4 операции)")
	fmt.Println()

	err = c.runPhase(ctx, 2, 10, 4)
	if err != nil {
		return fmt.Errorf("фаза 2 завершилась с ошибкой: %w", err)
	}

	c.printStatistics("ФИНАЛЬНАЯ СТАТИСТИКА")

	return nil
}

// runPhase выполняет одну фазу демонстрации с параллельными запросами
func (c *LoadBalancedClient) runPhase(ctx context.Context, phaseNum, numWorkers, opsPerWorker int) error {
	startTime := time.Now()
	errChan := make(chan error, numWorkers)
	var wg sync.WaitGroup

	// Запускаем воркеры. wg.Go (Go 1.25+) сам делает Add(1)/Done —
	// забыть Done уже невозможно.
	for workerID := range numWorkers {
		wg.Go(func() {
			c.runWorker(ctx, phaseNum, workerID, opsPerWorker, errChan)
		})
	}

	// Ждем завершения всех воркеров
	wg.Wait()
	close(errChan)

	// Собираем ошибки
	errors := collectErrors(errChan)
	if len(errors) > 0 {
		return fmt.Errorf("фаза %d: обнаружено %d ошибок", phaseNum, len(errors))
	}

	fmt.Printf("⏱️  Фаза %d выполнена за %v\n\n", phaseNum, time.Since(startTime))
	return nil
}

// runWorker выполняет операции для одного воркера
func (c *LoadBalancedClient) runWorker(
	ctx context.Context,
	phaseNum, workerID, opsPerWorker int,
	errChan chan<- error,
) {
	for op := range opsPerWorker {
		if err := c.executeRequest(ctx, phaseNum, workerID, op); err != nil {
			errChan <- fmt.Errorf("worker-%d op-%d: %w", workerID, op, err)
			return
		}
		if err := wait(ctx, requestDelay); err != nil {
			return
		}
	}
}

// executeRequest выполняет один запрос и выводит результат
func (c *LoadBalancedClient) executeRequest(ctx context.Context, phaseNum, workerID, opID int) error {
	opCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	name := fmt.Sprintf("Ship-P%d-W%d-Op%d", phaseNum, workerID, opID)
	spaceship, serverID, err := c.CreateSpaceshipWithStats(opCtx, workerID, name)
	if err != nil {
		return err
	}

	fmt.Printf("🚀 [Worker-%02d] Server: %s | UUID: %s\n",
		workerID, serverID, shortUUID(spaceship.GetUuid()))
	return nil
}

// wait приостанавливает выполнение на d, но прерывается при отмене контекста.
// Именно поэтому здесь таймер, а не time.Sleep: sleep нельзя прервать,
// и воркеры продолжали бы работать после отмены контекста.
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

// shortUUID укорачивает UUID для вывода. Голый uuid[:8] паникует на пустой строке —
// а пустой UUID приедет, например, если сервер вернет ответ без корабля.
func shortUUID(uuid string) string {
	const shortLen = 8
	if len(uuid) < shortLen {
		return uuid
	}

	return uuid[:shortLen]
}

// collectErrors собирает все ошибки из канала
func collectErrors(errChan <-chan error) []error {
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}
	return errors
}

// printStatistics выводит статистику распределения нагрузки
func (c *LoadBalancedClient) printStatistics(title string) {
	c.mu.RLock()
	stats := make(map[string]int64, len(c.serverStats))
	maps.Copy(stats, c.serverStats)
	c.mu.RUnlock()

	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("📊 %s\n", title)
	fmt.Println(strings.Repeat("=", 50))

	if len(stats) == 0 {
		fmt.Println("Нет данных о запросах")
		return
	}

	// Подсчитываем общее количество
	totalRequests := int64(0)
	for _, count := range stats {
		totalRequests += count
	}

	// Сортируем и выводим
	var serverIDs []string
	for serverID := range stats {
		serverIDs = append(serverIDs, serverID)
	}
	sort.Strings(serverIDs)

	fmt.Printf("Всего запросов: %d\n", totalRequests)
	fmt.Printf("Серверов: %d\n\n", len(serverIDs))

	for _, serverID := range serverIDs {
		count := stats[serverID]
		percentage := float64(count) / float64(totalRequests) * 100
		fmt.Printf("%-20s %4d запросов (%.1f%%)\n", serverID, count, percentage)
	}

	fmt.Println(strings.Repeat("=", 50) + "\n")
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("❌ %v", err)
	}

	fmt.Println("\n🎉 Демонстрация завершена успешно!")
}

// run содержит всю логику демо: вынесено из main, чтобы log.Fatal не съедал
// отложенное закрытие соединения (defer не выполняется при os.Exit).
func run() error {
	fmt.Println("🎯 gRPC Manual Resolver Load Balancing Demo")
	fmt.Println(strings.Repeat("=", 70))

	// Начальные серверы для балансировки
	initialServers := []string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
	}

	// Создаем клиент с балансировкой
	client, err := NewLoadBalancedClient(initialServers)
	if err != nil {
		return fmt.Errorf("не удалось создать клиент: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("⚠️  Ошибка при закрытии соединения: %v", closeErr)
		}
	}()

	// Запускаем демонстрацию
	if err = client.RunLoadBalancingDemo(context.Background()); err != nil {
		return fmt.Errorf("ошибка в демонстрации: %w", err)
	}

	return nil
}
