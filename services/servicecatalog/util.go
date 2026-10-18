package servicecatalog

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func validateTags(m map[string]string) error {
	if len(m) > maxTags {
		return fail(ErrLimitExceeded, "at most %d tags are allowed", maxTags)
	}

	for k, v := range m {
		if k == "" || len(k) > 128 || len(v) > 256 || strings.HasPrefix(strings.ToLower(k), "aws:") {
			return invalid("tag %q is invalid", k)
		}
	}

	return nil
}

func mergeTags(cur map[string]string, add map[string]string, remove []string) (map[string]string, error) {
	out := maps.Clone(cur)
	if out == nil {
		out = map[string]string{}
	}

	maps.Copy(out, add)

	for _, k := range remove {
		delete(out, k)
	}

	if err := validateTags(out); err != nil {
		return nil, err
	}

	return out, nil
}

func strOr(s, def string) string {
	if s == "" {
		return def
	}

	return s
}

func containsFold(hay, needle string) bool {
	return strings.Contains(strings.ToLower(hay), strings.ToLower(needle))
}

// idemCreate replays a create made earlier with the same IdempotencyToken and parameters.
func idemCreate[T any](
	b *InMemoryBackend, opName, token string, req any,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	res, err := idempotency.Create(b.idem, opName, token, idempotency.Fingerprint(req), idOf, get, create)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, invalid("%v", err)
	}

	return res, err
}

// idemReplay replays the response of an earlier call made with the same token and parameters.
func idemReplay[T any](b *InMemoryBackend, opName, token string, req any, do func() (*T, error)) (*T, error) {
	res, err := idempotency.Replay(b.idem, opName, token, idempotency.Fingerprint(req), do)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, invalid("%v", err)
	}

	return res, err
}

func jsonUnmarshal(s string, v any) error { return json.Unmarshal([]byte(s), v) }

func awsmetaCaller(ctx context.Context) string { return awsmeta.CallerArn(ctx) }
