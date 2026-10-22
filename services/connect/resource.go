package connect

import (
	"context"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func resourceConflictErr(format string, args ...any) error {
	return apiErr(http.StatusConflict, "ResourceConflictException", format, args...)
}

// readInstance runs fn under the read lock with the resolved instance ID.
func readInstance[T any](b *Backend, op, ref string, fn func(inst string) (T, error)) (T, error) {
	b.mu.RLock(op)
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		var zero T

		return zero, err
	}

	return fn(r.Instance.ID)
}

// writeInstance runs fn under the write lock with the resolved instance ID.
func writeInstance[T any](b *Backend, op, ref string, fn func(inst string) (T, error)) (T, error) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		var zero T

		return zero, err
	}

	return fn(r.Instance.ID)
}

// listIn pages a family's records of one instance after mapping each through conv (nil keeps it).
func listIn[V, S any](
	b *Backend, op, ref string, f family[V], conv func(*V) (S, bool), token string, maxResults int32,
) ([]S, string, error) {
	all, err := readInstance(b, op, ref, func(inst string) ([]S, error) {
		var out []S

		for _, v := range f.in(inst) {
			if s, ok := conv(v); ok {
				out = append(out, s)
			}
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	return paged(all, token, maxResults)
}

func (b *Backend) callerARN(ctx context.Context) string {
	account, region := b.scope(ctx)

	return arn.BuildGlobal("iam", region, account, "root")
}

func parseInt32(s string) (int32, error) {
	n, err := strconv.ParseInt(s, 10, 32)

	return int32(n), err
}

func itoa32(n int32) string { return strconv.FormatInt(int64(n), 10) }
