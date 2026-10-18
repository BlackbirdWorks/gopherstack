package pods_test

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/pods"
)

func newDirService(t *testing.T) *pods.Service {
	t.Helper()

	st, err := pods.NewDirStore(t.TempDir())
	require.NoError(t, err)

	return pods.New(st)
}

func newS3Service(t *testing.T) *pods.Service {
	t.Helper()

	var (
		mu   sync.Mutex
		objs = map[string][]byte{}
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		key := strings.TrimPrefix(r.URL.Path, "/bkt/")

		switch {
		case r.Method == http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			objs[key] = b
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete:
			delete(objs, key)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			prefix := r.URL.Query().Get("prefix")

			type content struct {
				Key string `xml:"Key"`
			}

			var res struct {
				XMLName  xml.Name  `xml:"ListBucketResult"`
				Contents []content `xml:"Contents"`
				KeyCount int       `xml:"KeyCount"`
			}

			var ks []string
			for k := range objs {
				if strings.HasPrefix(k, prefix) {
					ks = append(ks, k)
				}
			}

			sort.Strings(ks)

			for _, k := range ks {
				res.Contents = append(res.Contents, content{Key: k})
			}

			res.KeyCount = len(ks)
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(res)
		case r.Method == http.MethodGet:
			b, ok := objs[key]
			if !ok {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>x</Message></Error>`))

				return
			}

			_, _ = w.Write(b)
		}
	}))
	t.Cleanup(srv.Close)

	st, err := pods.NewS3Store(t.Context(), pods.S3Config{
		Bucket: "bkt", Prefix: "pods", Endpoint: srv.URL, AccessKey: "a", SecretKey: "b",
	})
	require.NoError(t, err)

	return pods.New(st)
}

func TestPodLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mk   func(*testing.T) *pods.Service
		name string
	}{
		{name: "dir", mk: newDirService},
		{name: "s3", mk: newS3Service},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			svc := tt.mk(t)

			m1, err := svc.Save(ctx, "demo", "first", map[string][]byte{"s3": []byte(`{"a":1}`), "sqs": []byte(`{"b":2}`)})
			require.NoError(t, err)
			assert.Equal(t, 1, m1.Version)
			assert.Equal(t, []string{"s3", "sqs"}, m1.Services)
			assert.Positive(t, m1.Size)

			m2, err := svc.Save(ctx, "demo", "", map[string][]byte{"s3": []byte(`{"a":3}`)})
			require.NoError(t, err)
			assert.Equal(t, 2, m2.Version)

			_, snaps, err := svc.Load(ctx, "demo", 0, nil)
			require.NoError(t, err)
			assert.JSONEq(t, `{"a":3}`, string(snaps["s3"]))

			_, snaps, err = svc.Load(ctx, "demo", 1, []string{"sqs"})
			require.NoError(t, err)
			assert.Len(t, snaps, 1)
			assert.JSONEq(t, `{"b":2}`, string(snaps["sqs"]))

			_, _, err = svc.Load(ctx, "demo", 2, []string{"sqs"})
			require.ErrorIs(t, err, pods.ErrNotFound)

			ins, err := svc.Inspect(ctx, "demo", 1)
			require.NoError(t, err)
			assert.Equal(t, "first", ins.Message)

			vers, err := svc.Versions(ctx, "demo")
			require.NoError(t, err)
			assert.Len(t, vers, 2)

			list, err := svc.List(ctx)
			require.NoError(t, err)
			require.Len(t, list, 1)
			assert.Equal(t, 2, list[0].Latest)
			assert.Equal(t, 2, list[0].Versions)

			require.NoError(t, svc.Delete(ctx, "demo", 1))
			_, err = svc.Inspect(ctx, "demo", 1)
			require.ErrorIs(t, err, pods.ErrNotFound)

			require.NoError(t, svc.Delete(ctx, "demo", 0))
			_, err = svc.Versions(ctx, "demo")
			require.ErrorIs(t, err, pods.ErrNotFound)
		})
	}
}

func TestPodErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(*pods.Service) error
		want error
		name string
	}{
		{name: "bad name save", want: pods.ErrInvalidName, call: func(s *pods.Service) error {
			_, err := s.Save(context.Background(), "../x", "", nil)

			return err
		}},
		{name: "missing pod load", want: pods.ErrNotFound, call: func(s *pods.Service) error {
			_, _, err := s.Load(context.Background(), "nope", 0, nil)

			return err
		}},
		{name: "missing pod delete", want: pods.ErrNotFound, call: func(s *pods.Service) error {
			return s.Delete(context.Background(), "nope", 0)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tt.call(newDirService(t)), tt.want)
		})
	}
}
