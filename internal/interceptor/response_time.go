package interceptor

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
)

func ResponseTimeUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		res, err := handler(ctx, req)
		duration := time.Since(start)

		log.Printf("[timing] method=%s duration=%s", info.FullMethod, duration)

		return res, err
	}
}
