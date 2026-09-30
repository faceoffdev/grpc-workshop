package interceptor

import (
	"context"
	"log"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// Входящие metadata ключи (от клиента)
	requestIDKey     = "request-id"
	userIDKey        = "user-id"
	authTokenKey     = "authorization"
	clientVersionKey = "client-version"

	// Исходящие metadata ключи (от сервера)
	serverVersionKey        = "server-version"
	responseTimestampKey    = "response-timestamp"
	processedByKey          = "processed-by"
	processingDurationUsKey = "processing-duration-us"

	// Значения исходящих headers — общие для unary и stream версий интерцептора
	serverVersion    = "1.0.0"
	processedByValue = "grpc-workshop-server"
)

// MetadataUnaryServerInterceptor интерсептор для обработки metadata в unary запросах
// Демонстрирует:
// - Извлечение входящих headers от клиента
// - Логирование metadata для трейсинга/аудита
// - Автоматическое добавление исходящих headers в ответ
// - Отправку метрик обработки в trailers
func MetadataUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		startTime := time.Now()

		// 1. Извлекаем входящие metadata от клиента
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			logIncomingMetadata(info.FullMethod, md)
		} else {
			log.Printf("[metadata] method=%s warning: no incoming metadata", info.FullMethod)
		}

		// 2. Создаем исходящие metadata для отправки клиенту
		// Эти headers отправляются ДО начала обработки запроса
		outgoingMD := metadata.Pairs(
			serverVersionKey, serverVersion,
			responseTimestampKey, startTime.Format(time.RFC3339),
			processedByKey, processedByValue,
		)

		// 3. Отправляем headers клиенту (до вызова handler)
		// SendHeader отправляет headers немедленно и «закрывает» их на запись:
		// любой последующий SetHeader/SendHeader вернет ошибку.
		// Поэтому интерцепторы, которые тоже хотят добавить свои headers
		// (например, ServerIDUnaryServerInterceptor), должны стоять в цепочке ДО этого.
		err := grpc.SendHeader(ctx, outgoingMD)
		if err != nil {
			log.Printf("[metadata] failed to send header: %v", err)
		}

		// 4. Вызываем обработчик запроса
		resp, handlerErr := handler(ctx, req)

		// 5. Вычисляем время обработки
		processingDuration := time.Since(startTime)

		// 6. Отправляем trailers (metadata в конце ответа)
		// Trailers содержат информацию, которая известна только ПОСЛЕ обработки
		trailer := metadata.Pairs(
			processingDurationUsKey, strconv.FormatInt(processingDuration.Microseconds(), 10),
		)
		if setErr := grpc.SetTrailer(ctx, trailer); setErr != nil {
			log.Printf("[metadata] failed to set trailer: %v", setErr)
		}

		// Длительность здесь только уезжает в trailer — логирует ее
		// ResponseTimeUnaryServerInterceptor, чтобы не было двух одинаковых строк в логе.
		return resp, handlerErr
	}
}

// logIncomingMetadata логирует входящие metadata от клиента
func logIncomingMetadata(method string, md metadata.MD) {
	log.Printf("[metadata] method=%s incoming headers:", method)

	// Логируем request-id для трейсинга
	if requestIDs := md.Get(requestIDKey); len(requestIDs) > 0 {
		log.Printf("  - %s: %s", requestIDKey, requestIDs[0])
	}

	// Логируем user-id для аудита
	if userIDs := md.Get(userIDKey); len(userIDs) > 0 {
		log.Printf("  - %s: %s", userIDKey, userIDs[0])
	}

	// Логируем authorization (частично, для безопасности)
	if authTokens := md.Get(authTokenKey); len(authTokens) > 0 {
		// В продакшене не логируем полный токен!
		token := authTokens[0]
		if len(token) > 20 {
			token = token[:20] + "..."
		}
		log.Printf("  - %s: %s", authTokenKey, token)
	}

	// Логируем версию клиента
	if clientVersions := md.Get(clientVersionKey); len(clientVersions) > 0 {
		log.Printf("  - %s: %s", clientVersionKey, clientVersions[0])
	}

	// Дополнительно: логируем все остальные headers
	for key, values := range md {
		// Пропускаем уже залогированные
		if key == requestIDKey || key == userIDKey || key == authTokenKey || key == clientVersionKey {
			continue
		}
		// Системные headers начинаются с ':'
		if len(key) > 0 && key[0] != ':' {
			log.Printf("  - %s: %v", key, values)
		}
	}
}
