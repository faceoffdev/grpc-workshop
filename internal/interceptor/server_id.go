package interceptor

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const serverIDKey = "server-id"

// ServerIDUnaryServerInterceptor создает unary interceptor, который добавляет server ID в metadata
// и логирует информацию о том, какой сервер обработал запрос.
// Это полезно для демонстрации клиентской балансировки нагрузки.
//
// ВАЖНО: SetHeader только накапливает headers, но не отправляет их. Реально они уходят
// клиенту при первом SendHeader или вместе с ответом. Поэтому SetHeader обязан быть вызван
// ДО того, как любой другой интерцептор сделает SendHeader — иначе headers уже отправлены,
// SetHeader вернет ошибку, и server-id никогда не доедет до клиента.
// По этой же причине этот интерцептор стоит первым в цепочке (см. initGRPCServer).
func ServerIDUnaryServerInterceptor(serverID string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Добавляем server ID в outgoing metadata ДО вызова обработчика.
		// Клиент сможет прочитать это из response headers.
		if err := grpc.SetHeader(ctx, metadata.Pairs(serverIDKey, serverID)); err != nil {
			log.Printf("[server-id] Failed to set header: %v", err)
		}

		// Логируем информацию о запросе
		log.Printf("[server-id=%s] method=%s", serverID, info.FullMethod)

		// Выполняем обработчик и возвращаем его результат КАК ЕСТЬ:
		// ошибку хендлера нельзя терять или подменять ошибкой отправки headers.
		return handler(ctx, req)
	}
}
