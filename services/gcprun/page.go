package gcprun

import (
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// collect maps the values whose key has prefix, in key order.
func collect[V, T any](m map[string]V, prefix string, view func(V) T) []T {
	keys := sortedKeys(m, prefix)
	out := make([]T, 0, len(keys))

	for _, k := range keys {
		out = append(out, view(m[k]))
	}

	return out
}

func protoEqual(a, b proto.Message) bool { return proto.Equal(a, b) }

func paginate[T any](all []T, token string, size int32) ([]T, string, error) {
	if size < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	if err := page.ValidateToken(token); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	pg := page.New(all, token, int(size), defaultPageSize)

	return pg.Data, pg.Next, nil
}
