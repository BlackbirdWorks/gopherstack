package dashboard

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/compress"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// CompressionMiddleware compresses dashboard assets and JSON responses at
// runtime; embedded assets are compressed once per coding and cached.
func CompressionMiddleware() service.Middleware {
	return compress.New(compress.Config{
		AlwaysVary: true,
		Cacheable:  immutableAsset,
	}).EchoMiddleware()
}

func immutableAsset(r *http.Request) bool {
	p := r.URL.Path

	return strings.HasPrefix(p, "/dashboard/static/") ||
		strings.HasPrefix(p, "/dashboard/_app/") ||
		p == "/dashboard/"
}
