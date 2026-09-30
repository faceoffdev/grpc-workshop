package interceptor

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errInvalid = errors.New("поле обязательно")

// validatableMsg — сообщение с методом Validate(), как их генерирует protoc-gen-validate
type validatableMsg struct {
	err error
}

func (m validatableMsg) Validate() error { return m.err }

// plainMsg — сообщение без Validate(); интерцептор должен пропускать такие как есть
type plainMsg struct{}

func TestValidationUnaryServerInterceptor(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		req         any
		wantCode    codes.Code
		wantHandler bool
	}{
		{
			name:        "валидное сообщение доходит до хендлера",
			req:         validatableMsg{},
			wantCode:    codes.OK,
			wantHandler: true,
		},
		{
			name:        "невалидное отсекается с InvalidArgument",
			req:         validatableMsg{err: errInvalid},
			wantCode:    codes.InvalidArgument,
			wantHandler: false,
		},
		{
			name:        "сообщение без Validate() пропускается",
			req:         plainMsg{},
			wantCode:    codes.OK,
			wantHandler: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var handlerCalled bool

			handler := func(_ context.Context, _ any) (any, error) {
				handlerCalled = true

				return "ok", nil
			}

			_, err := ValidationUnaryServerInterceptor()(
				t.Context(),
				tc.req,
				&grpc.UnaryServerInfo{FullMethod: "/test/Method"},
				handler,
			)

			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("код = %s, ожидали %s", got, tc.wantCode)
			}

			if handlerCalled != tc.wantHandler {
				t.Errorf("хендлер вызван = %v, ожидали %v", handlerCalled, tc.wantHandler)
			}
		})
	}
}

// fakeServerStream — минимальная заглушка grpc.ServerStream.
// Настоящий gRPC отдает в RecvMsg указатель на конкретное сообщение (которое уже
// умеет Validate), поэтому заглушке достаточно ничего не делать: состояние
// сообщения задает сам тест.
type fakeServerStream struct {
	grpc.ServerStream

	ctx context.Context //nolint:containedctx // заглушка обязана хранить ctx: он приходит из интерфейса ServerStream
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func (f *fakeServerStream) RecvMsg(any) error { return nil }

func TestValidationStreamServerInterceptor(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// newMsg отдает указатель на сообщение — ровно так его создает
		// сгенерированный gRPC-код перед вызовом RecvMsg
		newMsg   func() any
		wantCode codes.Code
	}{
		{
			name:     "валидное сообщение проходит",
			newMsg:   func() any { return &validatableMsg{} },
			wantCode: codes.OK,
		},
		{
			name:     "невалидное отсекается",
			newMsg:   func() any { return &validatableMsg{err: errInvalid} },
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "сообщение без Validate() пропускается",
			newMsg:   func() any { return &plainMsg{} },
			wantCode: codes.OK,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := &fakeServerStream{ctx: t.Context()}

			// Хендлер имитирует чтение сообщения из стрима: проверка навешена
			// на RecvMsg, поэтому ошибка валидации приходит именно отсюда.
			handler := func(_ any, ss grpc.ServerStream) error {
				return ss.RecvMsg(tc.newMsg())
			}

			err := ValidationStreamServerInterceptor()(
				nil,
				stream,
				&grpc.StreamServerInfo{FullMethod: "/test/Stream"},
				handler,
			)

			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("код = %s, ожидали %s", got, tc.wantCode)
			}
		})
	}
}
