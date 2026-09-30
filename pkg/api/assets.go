// Package api содержит статику Swagger UI и сгенерированную OpenAPI-спецификацию.
package api

import "embed"

// FS — вшитая в бинарь статика Swagger UI.
//
// Раньше сервер отдавал ее через http.Dir("pkg/api"), то есть по пути относительно
// текущего каталога: `go run ./cmd/grpc_server` из корня репозитория работал,
// а собранный бинарь, запущенный откуда угодно еще, молча отдавал 404.
// embed вшивает файлы в бинарь на этапе сборки — рабочий каталог больше не важен.
//
//go:embed swagger-ui.html swagger/swagger.swagger.json
var FS embed.FS
