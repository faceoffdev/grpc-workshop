package interceptor

import (
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// wrappedServerStream оборачивает grpc.ServerStream, чтобы перехватить RecvMsg.
//
// У стримов нет «одного запроса», который можно проверить до вызова хендлера:
// сообщения приходят по ходу работы. Поэтому единственная точка, куда можно
// вклиниться, — это подмена самого стрима. Встраивание grpc.ServerStream дает
// всю остальную реализацию бесплатно, переопределяем только нужный метод.
type wrappedServerStream struct {
	grpc.ServerStream

	onRecv func(msg any) error
}

// RecvMsg вызывает хук после успешного получения сообщения
func (w *wrappedServerStream) RecvMsg(msg any) error {
	if err := w.ServerStream.RecvMsg(msg); err != nil {
		return err
	}

	return w.onRecv(msg)
}

// ValidationStreamServerInterceptor валидирует каждое сообщение, приходящее в стрим.
// Unary-версия проверяет один req до хендлера, здесь же проверка навешивается
// на RecvMsg — иначе сообщения стрима вообще не проходят валидацию.
func ValidationStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		_ *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		wrapped := &wrappedServerStream{
			ServerStream: ss,
			onRecv: func(msg any) error {
				v, ok := msg.(validator)
				if !ok {
					return nil
				}

				if err := v.Validate(); err != nil {
					return status.Errorf(codes.InvalidArgument, "validation error: %v", err)
				}

				return nil
			},
		}

		return handler(srv, wrapped)
	}
}

// MetadataStreamServerInterceptor логирует входящие metadata стрима и отправляет
// клиенту свои headers. Stream-аналог MetadataUnaryServerInterceptor.
func MetadataStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if md, ok := metadata.FromIncomingContext(ss.Context()); ok {
			logIncomingMetadata(info.FullMethod, md)
		}

		// У стрима headers уходят либо явным SendHeader, либо вместе с первым
		// сообщением. Здесь шлем сразу: клиент видит их, не дожидаясь первого события.
		err := ss.SendHeader(metadata.Pairs(
			serverVersionKey, serverVersion,
			responseTimestampKey, time.Now().Format(time.RFC3339),
			processedByKey, processedByValue,
		))
		if err != nil {
			log.Printf("[metadata] failed to send stream header: %v", err)
		}

		return handler(srv, ss)
	}
}

// ResponseTimeStreamServerInterceptor замеряет, сколько прожил стрим.
// Для стрима это не «время ответа», а длительность подписки — тоже полезная метрика.
func ResponseTimeStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()
		err := handler(srv, ss)

		log.Printf("[timing] stream=%s duration=%s", info.FullMethod, time.Since(start))

		return err
	}
}

// ServerIDStreamServerInterceptor кладет server ID в headers стрима.
// Как и в unary-цепочке, должен стоять ДО MetadataStreamServerInterceptor,
// который эти headers отправляет.
func ServerIDStreamServerInterceptor(serverID string) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if err := ss.SetHeader(metadata.Pairs(serverIDKey, serverID)); err != nil {
			log.Printf("[server-id] failed to set stream header: %v", err)
		}

		log.Printf("[server-id=%s] stream=%s", serverID, info.FullMethod)

		return handler(srv, ss)
	}
}
