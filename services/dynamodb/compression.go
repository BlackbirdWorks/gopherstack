package dynamodb

import (
	"github.com/blackbirdworks/gopherstack/pkgs/compress"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// CompressionMiddleware gzips responses for Accept-Encoding: gzip clients (EnableAcceptEncodingGzip),
// recomputing X-Amz-Crc32 over the compressed bytes as the SDK verifies.
func CompressionMiddleware() service.Middleware {
	return compress.New(compress.Config{
		Encodings:    []compress.Encoding{compress.Gzip},
		RewriteCRC32: true,
	}).EchoMiddleware()
}
