package gcpartifactregistry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func sortStrings(s []string) { sort.Strings(s) }

func removeAll(path string) { _ = os.RemoveAll(path) }

func timeFromNanos(n int64) time.Time { return time.Unix(0, n).UTC() }

func marshalJSON(v any) ([]byte, error) { return json.Marshal(v, jsonx.V1()) }

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:])
}

const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

func requestScheme(r *http.Request) string {
	if r.TLS != nil {
		return schemeHTTPS
	}

	return schemeHTTP
}
