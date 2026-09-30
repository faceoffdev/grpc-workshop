package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	spaceshipV1 "github.com/olezhek28/grpc-workshop/pkg/proto/spaceship/v1"
)

const (
	serverAddr     = "127.0.0.1:50051"
	requestTimeout = 10 * time.Second
)

// HeadersClient демонстрирует работу с gRPC metadata (headers)
type HeadersClient struct {
	conn   *grpc.ClientConn
	client spaceshipV1.SpaceshipServiceClient
}

// NewHeadersClient создает новый клиент для демонстрации работы с headers
func NewHeadersClient(serverAddr string) (*HeadersClient, error) {
	fmt.Printf("🔌 Подключаемся к серверу %s...\n", serverAddr)

	conn, err := grpc.NewClient(
		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &HeadersClient{
		conn:   conn,
		client: spaceshipV1.NewSpaceshipServiceClient(conn),
	}, nil
}

// Close закрывает соединение с сервером
func (c *HeadersClient) Close() error {
	return c.conn.Close()
}

// CreateSpaceshipWithHeaders демонстрирует отправку и получение headers в unary запросе
func (c *HeadersClient) CreateSpaceshipWithHeaders(ctx context.Context) error {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📝 Демонстрация работы с gRPC Headers (Metadata)")
	fmt.Println(strings.Repeat("=", 80))

	// 1. Создаем metadata (headers) для отправки на сервер
	requestID := uuid.New().String()
	md := metadata.Pairs(
		"request-id", requestID,
		"user-id", "demo-user-123",
		"authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
		"client-version", "1.0.0",
	)

	fmt.Println("\n📤 Отправляем headers на сервер:")
	fmt.Printf("  - request-id: %s\n", requestID)
	fmt.Printf("  - user-id: demo-user-123\n")
	fmt.Printf("  - authorization: Bearer eyJ...\n")
	fmt.Printf("  - client-version: 1.0.0\n")

	// 2. Добавляем metadata в контекст
	// Это основной способ отправки headers в gRPC
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Дедлайн на вызов
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	// 3. Подготавливаем переменные для получения headers и trailers от сервера
	var header, trailer metadata.MD

	// 4. Создаем корабль и отправляем запрос
	req := &spaceshipV1.CreateRequest{
		Info: &spaceshipV1.SpaceshipInfo{
			Name:         "USS Enterprise",
			Description:  wrapperspb.String("Звездолёт класса Constitution"),
			Model:        "NCC-1701",
			Manufacturer: "Starfleet",
			Price:        100000000.00,
			Engine: &spaceshipV1.SpaceshipInfo_WarpDrive{
				WarpDrive: &spaceshipV1.WarpDrive{
					WarpFactor:        5,
					FieldStability:    0.95,
					DilithiumCrystals: true,
				},
			},
		},
	}

	// 5. Вызываем метод с опциями для получения headers и trailers
	// grpc.Header() - получаем headers (отправляются в начале ответа)
	// grpc.Trailer() - получаем trailers (отправляются в конце ответа)
	resp, err := c.client.Create(ctx, req, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return fmt.Errorf("failed to create spaceship: %w", err)
	}

	fmt.Printf("\n✅ Создан корабль: %s (UUID: %s)\n", resp.GetSpaceship().GetInfo().GetName(), resp.GetSpaceship().GetUuid())

	// 6. Читаем headers от сервера (отправлены ДО обработки)
	fmt.Println("\n📥 Получены headers от сервера (отправлены ДО обработки):")
	if serverVersion := header.Get("server-version"); len(serverVersion) > 0 {
		fmt.Printf("  - server-version: %s\n", serverVersion[0])
	}
	if responseTimestamp := header.Get("response-timestamp"); len(responseTimestamp) > 0 {
		fmt.Printf("  - response-timestamp: %s\n", responseTimestamp[0])
	}
	if processedBy := header.Get("processed-by"); len(processedBy) > 0 {
		fmt.Printf("  - processed-by: %s\n", processedBy[0])
	}

	// 7. Читаем trailers от сервера (отправлены ПОСЛЕ обработки)
	fmt.Println("\n📥 Получены trailers от сервера (отправлены ПОСЛЕ обработки):")
	if processingDuration := trailer.Get("processing-duration-us"); len(processingDuration) > 0 {
		fmt.Printf("  - processing-duration-us: %s мкс (микросекунд)\n", processingDuration[0])
	}
	if grpcStatus := trailer.Get("grpc-status"); len(grpcStatus) > 0 {
		fmt.Printf("  - grpc-status: %s (0 = OK)\n", grpcStatus[0])
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("❌ %v", err)
	}
}

// run содержит логику демо: вынесено из main, чтобы log.Fatal не съедал
// отложенное закрытие соединения (defer не выполняется при os.Exit).
func run() error {
	client, err := NewHeadersClient(serverAddr)
	if err != nil {
		return fmt.Errorf("не удалось создать клиент: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("⚠️  Ошибка при закрытии соединения: %v", closeErr)
		}
	}()

	if err = client.CreateSpaceshipWithHeaders(context.Background()); err != nil {
		return fmt.Errorf("ошибка в демонстрации: %w", err)
	}

	return nil
}
