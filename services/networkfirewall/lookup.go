package networkfirewall

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	maxPort          = 65535
	listenerHTTP     = "HTTP"
	listenerHTTPS    = "HTTPS"
	analysisTLSSNI   = "TLS_SNI"
	analysisHTTPHost = "HTTP_HOST"
)

// arnOrName is the identifier pair most describe, update and delete calls accept.
type arnOrName struct {
	arn  string
	name string
}

// keyFor resolves an ARN-or-name pair to the table key of a kind.
func (b *InMemoryBackend) keyFor(kind, arnStr, name string) (string, error) {
	if err := targetNeeded(arnStr, name); err != nil {
		return "", err
	}

	if arnStr != "" {
		return arnStr, nil
	}

	return b.arnFor(kind, name), nil
}

func getOrNotFound[V any](t *store.Table[V], label, key string) (*V, error) {
	r, ok := t.Get(key)
	if !ok {
		return nil, notFound(label, strings.TrimPrefix(key, "arn:"))
	}

	return r, nil
}

func lookup[V any](b *InMemoryBackend, t *store.Table[V], kind, label string, id arnOrName) (*V, error) {
	key, err := b.keyFor(kind, id.arn, id.name)
	if err != nil {
		return nil, err
	}

	return getOrNotFound(t, label, key)
}

func clampPos(pos *int32, n int) int {
	if pos == nil || int(*pos) >= n {
		return n
	}

	return max(int(*pos), 0)
}

func insertAt[T any](s []T, pos *int32, v T) []T {
	return slices.Insert(s, clampPos(pos, len(s)), v)
}

// moveTo relocates s[from] to pos, clamped to the slice.
func moveTo[T any](s []T, from int, pos *int32) []T {
	v := s[from]
	s = slices.Delete(s, from, from+1)

	return slices.Insert(s, clampPos(pos, len(s)), v)
}

func listMetadata[V any](
	all []*V, meta func(*V) ResourceMetadata, nextToken string, maxResults *int32,
) ([]ResourceMetadata, string, error) {
	out := make([]ResourceMetadata, 0, len(all))
	for _, r := range all {
		out = append(out, meta(r))
	}

	slices.SortFunc(out, func(a, c ResourceMetadata) int { return strings.Compare(a.Name+a.Arn, c.Name+c.Arn) })

	pg, err := paginate(out, nextToken, maxResults)
	if err != nil {
		return nil, "", err
	}

	if pg.Data == nil {
		pg.Data = []ResourceMetadata{}
	}

	return pg.Data, pg.Next, nil
}
