package gcpstorage_test

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/storage"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

const sdkProject = "test-project"

type sdkEnv struct {
	client  *storage.Client
	backend *gcpstorage.InMemoryBackend
	url     string
}

func newSDK(t *testing.T) *sdkEnv {
	t.Helper()

	backend := gcpstorage.NewInMemoryBackend()
	e := echo.New()
	e.Any("/*", gcpstorage.NewHandler(backend).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	client, err := storage.NewClient(t.Context(),
		option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &sdkEnv{client: client, backend: backend, url: srv.URL}
}

func (s *sdkEnv) bucket(t *testing.T, name string, attrs *storage.BucketAttrs) *storage.BucketHandle {
	t.Helper()

	b := s.client.Bucket(name)
	require.NoError(t, b.Create(t.Context(), sdkProject, attrs))

	return b
}

func write(
	t *testing.T,
	o *storage.ObjectHandle,
	data []byte,
	chunk int,
	mod func(*storage.Writer),
) *storage.ObjectAttrs {
	t.Helper()

	w := o.NewWriter(t.Context())
	w.ChunkSize = chunk

	if mod != nil {
		mod(w)
	}

	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return w.Attrs()
}

func readAll(t *testing.T, o *storage.ObjectHandle) []byte {
	t.Helper()

	r, err := o.NewReader(t.Context())
	require.NoError(t, err)

	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)

	return data
}

func apiCode(err error) int {
	if gerr, ok := errors.AsType[*googleapi.Error](err); ok {
		return gerr.Code
	}

	return 0
}

func TestSDK_BucketLifecycle(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()

	attrs := &storage.BucketAttrs{
		Location:          "europe-west1",
		StorageClass:      "NEARLINE",
		Labels:            map[string]string{"env": "test"},
		VersioningEnabled: true,
		Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{{
			Action:    storage.LifecycleAction{Type: storage.DeleteAction},
			Condition: storage.LifecycleCondition{AgeInDays: 30},
		}}},
	}
	b := s.bucket(t, "lifecycle-bucket", attrs)

	got, err := b.Attrs(ctx)
	require.NoError(t, err)
	assert.Equal(t, "EUROPE-WEST1", got.Location)
	assert.Equal(t, "NEARLINE", got.StorageClass)
	assert.Equal(t, map[string]string{"env": "test"}, got.Labels)
	assert.True(t, got.VersioningEnabled)
	require.Len(t, got.Lifecycle.Rules, 1)
	assert.Equal(t, int64(30), got.Lifecycle.Rules[0].Condition.AgeInDays)
	assert.EqualValues(t, 1, got.MetaGeneration)

	upd := storage.BucketAttrsToUpdate{}
	upd.SetLabel("team", "core")
	upd.DeleteLabel("env")
	got, err = b.Update(ctx, upd)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "core"}, got.Labels)
	assert.EqualValues(t, 2, got.MetaGeneration)

	err = s.client.Bucket("lifecycle-bucket").Create(ctx, sdkProject, nil)
	assert.Equal(t, http.StatusConflict, apiCode(err))

	it := s.client.Buckets(ctx, sdkProject)
	it.Prefix = "lifecycle"
	first, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-bucket", first.Name)

	_, err = it.Next()
	assert.Equal(t, iterator.Done, err)

	write(t, b.Object("x"), []byte("x"), 0, nil)
	assert.Equal(t, http.StatusConflict, apiCode(b.Delete(ctx)))

	require.NoError(t, b.Object("x").Delete(ctx))
	assert.Equal(t, http.StatusConflict, apiCode(b.Delete(ctx)), "noncurrent version remains")

	vit := b.Objects(ctx, &storage.Query{Versions: true})
	for {
		oa, nextErr := vit.Next()
		if errors.Is(nextErr, iterator.Done) {
			break
		}

		require.NoError(t, nextErr)
		require.NoError(t, b.Object(oa.Name).Generation(oa.Generation).Delete(ctx))
	}

	require.NoError(t, b.Delete(ctx))

	_, err = b.Attrs(ctx)
	assert.ErrorIs(t, err, storage.ErrBucketNotExist)
}

func TestSDK_InvalidBucketName(t *testing.T) {
	t.Parallel()

	s := newSDK(t)

	for _, name := range []string{"UPPER", "ab", "goog-bucket", "-lead"} {
		err := s.client.Bucket(name).Create(t.Context(), sdkProject, nil)
		assert.Equal(t, http.StatusBadRequest, apiCode(err), name)
	}
}

func TestSDK_ObjectRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		size  int
		chunk int
	}{
		{name: "empty", size: 0, chunk: 0},
		{name: "small multipart", size: 1000, chunk: 0},
		{name: "resumable chunks", size: 1 << 20, chunk: 256 << 10},
		{name: "resumable ragged", size: 700 << 10, chunk: 256 << 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSDK(t)
			b := s.bucket(t, "roundtrip", nil)
			data := bytes.Repeat([]byte("0123456789abcdef"), tt.size/16+1)[:tt.size]

			attrs := write(t, b.Object("dir/obj.txt"), data, tt.chunk, func(w *storage.Writer) {
				w.ContentType = "text/plain"
				w.Metadata = map[string]string{"k": "v"}
				w.CacheControl = "no-cache"
			})

			assert.Equal(t, int64(tt.size), attrs.Size)
			assert.Equal(t, crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)), attrs.CRC32C)
			assert.Len(t, attrs.MD5, 16)
			assert.Equal(t, "text/plain", attrs.ContentType)
			assert.Equal(t, map[string]string{"k": "v"}, attrs.Metadata)
			assert.EqualValues(t, 1, attrs.Metageneration)
			assert.Positive(t, attrs.Generation)

			assert.Equal(t, data, readAll(t, b.Object("dir/obj.txt")))

			got, err := b.Object("dir/obj.txt").Attrs(t.Context())
			require.NoError(t, err)
			assert.Equal(t, attrs.Generation, got.Generation)
			assert.Equal(t, "no-cache", got.CacheControl)
		})
	}
}

func TestSDK_RangeRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		want   string
		offset int64
		length int64
	}{
		{name: "middle", offset: 3, length: 4, want: "3456"},
		{name: "tail", offset: 7, length: -1, want: "789"},
		{name: "suffix", offset: -2, length: -1, want: "89"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSDK(t)
			b := s.bucket(t, "range-bucket", nil)
			write(t, b.Object("o"), []byte("0123456789"), 0, nil)

			r, err := b.Object("o").NewRangeReader(t.Context(), tt.offset, tt.length)
			require.NoError(t, err)

			defer r.Close()

			got, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
			assert.EqualValues(t, 10, r.Attrs.Size)
		})
	}
}

func TestSDK_Preconditions(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "cond-bucket", nil)
	o := b.Object("o")

	first := write(t, o, []byte("one"), 0, nil)

	w := o.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	_, _ = w.Write([]byte("two"))
	assert.Equal(t, http.StatusPreconditionFailed, apiCode(w.Close()))

	w = o.If(storage.Conditions{GenerationMatch: first.Generation}).NewWriter(ctx)
	_, _ = w.Write([]byte("three"))
	require.NoError(t, w.Close())

	err := o.If(storage.Conditions{GenerationMatch: first.Generation}).Delete(ctx)
	assert.Equal(t, http.StatusPreconditionFailed, apiCode(err))

	_, err = o.If(storage.Conditions{MetagenerationMatch: 99}).Attrs(ctx)
	assert.Equal(t, http.StatusPreconditionFailed, apiCode(err))

	require.NoError(t, o.If(storage.Conditions{MetagenerationMatch: 1}).Delete(ctx))

	_, err = o.Attrs(ctx)
	assert.ErrorIs(t, err, storage.ErrObjectNotExist)
}

func TestSDK_Versioning(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "ver-bucket", &storage.BucketAttrs{VersioningEnabled: true})
	o := b.Object("o")

	g1 := write(t, o, []byte("v1"), 0, nil).Generation
	g2 := write(t, o, []byte("v2"), 0, nil).Generation
	assert.Greater(t, g2, g1)

	assert.Equal(t, "v1", string(readAll(t, o.Generation(g1))))
	assert.Equal(t, "v2", string(readAll(t, o)))

	count := 0
	it := b.Objects(ctx, &storage.Query{Versions: true})

	for {
		_, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		count++
	}

	assert.Equal(t, 2, count)

	require.NoError(t, o.Delete(ctx))

	_, err := o.Attrs(ctx)
	require.ErrorIs(t, err, storage.ErrObjectNotExist)
	assert.Equal(t, "v2", string(readAll(t, o.Generation(g2))), "archived after delete")

	require.NoError(t, o.Generation(g1).Delete(ctx))

	_, err = o.Generation(g1).Attrs(ctx)
	assert.ErrorIs(t, err, storage.ErrObjectNotExist)
}

func TestSDK_UpdateAttrs(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "upd-bucket", nil)
	o := b.Object("o")

	write(t, o, []byte("hello"), 0, func(w *storage.Writer) {
		w.Metadata = map[string]string{"a": "1", "b": "2"}
	})

	got, err := o.Update(ctx, storage.ObjectAttrsToUpdate{
		ContentType: "application/json",
		Metadata:    map[string]string{"a": "9"},
	})
	require.NoError(t, err)
	assert.Equal(t, "application/json", got.ContentType)
	assert.Equal(t, "9", got.Metadata["a"])
	assert.EqualValues(t, 2, got.Metageneration)

	assert.Equal(t, "hello", string(readAll(t, o)))
}

func TestSDK_ListDelimiterAndPaging(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "list-bucket", nil)

	for _, n := range []string{"a/1", "a/2", "b/1", "c", "d"} {
		write(t, b.Object(n), []byte(n), 0, nil)
	}

	var names, prefixes []string

	it := b.Objects(ctx, &storage.Query{Delimiter: "/"})
	it.PageInfo().MaxSize = 2

	for {
		oa, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		if oa.Prefix != "" {
			prefixes = append(prefixes, oa.Prefix)
		} else {
			names = append(names, oa.Name)
		}
	}

	assert.Equal(t, []string{"a/", "b/"}, prefixes)
	assert.Equal(t, []string{"c", "d"}, names)

	it = b.Objects(ctx, &storage.Query{Prefix: "a/"})
	n := 0

	for {
		_, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		n++
	}

	assert.Equal(t, 2, n)
}

func TestSDK_CopyAndCompose(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "copy-bucket", nil)
	other := s.bucket(t, "other-bucket", nil)

	write(t, b.Object("src"), []byte("payload"), 0, func(w *storage.Writer) {
		w.ContentType = "text/plain"
		w.Metadata = map[string]string{"orig": "1"}
	})

	attrs, err := other.Object("dst").CopierFrom(b.Object("src")).Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", attrs.ContentType)
	assert.Equal(t, map[string]string{"orig": "1"}, attrs.Metadata)
	assert.Equal(t, "payload", string(readAll(t, other.Object("dst"))))

	cp := b.Object("src2").CopierFrom(b.Object("src"))
	cp.ContentType = "application/x-new"
	cp.Metadata = map[string]string{"new": "2"}
	attrs, err = cp.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, "application/x-new", attrs.ContentType)
	assert.Equal(t, map[string]string{"new": "2"}, attrs.Metadata)

	for i, part := range []string{"AA", "BB", "CC"} {
		write(t, b.Object(fmt.Sprintf("part%d", i)), []byte(part), 0, nil)
	}

	comp := b.Object("composed").ComposerFrom(b.Object("part0"), b.Object("part1"), b.Object("part2"))
	comp.ContentType = "text/plain"
	attrs, err = comp.Run(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, attrs.ComponentCount)
	assert.EqualValues(t, 6, attrs.Size)
	assert.Equal(t, "AABBCC", string(readAll(t, b.Object("composed"))))

	_, err = b.Object("missing").CopierFrom(b.Object("nope")).Run(ctx)
	assert.ErrorIs(t, err, storage.ErrObjectNotExist)
}

func TestSDK_IAMPolicy(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	ctx := t.Context()
	b := s.bucket(t, "iam-bucket", nil)

	pol, err := b.IAM().V3().Policy(ctx)
	require.NoError(t, err)
	assert.Empty(t, pol.Bindings)

	pol.Bindings = append(
		pol.Bindings,
		&iampb.Binding{Role: "roles/storage.objectViewer", Members: []string{"allUsers"}},
	)
	require.NoError(t, b.IAM().V3().SetPolicy(ctx, pol))

	got, err := b.IAM().V3().Policy(ctx)
	require.NoError(t, err)
	require.Len(t, got.Bindings, 1)
	assert.Equal(t, "roles/storage.objectViewer", got.Bindings[0].GetRole())

	stale := pol
	assert.Equal(t, http.StatusPreconditionFailed, apiCode(b.IAM().V3().SetPolicy(ctx, stale)))

	perms, err := b.IAM().TestPermissions(ctx, []string{"storage.objects.get"})
	require.NoError(t, err)
	assert.Equal(t, []string{"storage.objects.get"}, perms)
}

func TestSDK_ChecksumRejected(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	b := s.bucket(t, "sum-bucket", nil)

	w := b.Object("o").NewWriter(t.Context())
	w.CRC32C = 12345
	w.SendCRC32C = true
	_, _ = w.Write([]byte("data"))
	assert.Equal(t, http.StatusBadRequest, apiCode(w.Close()))
}
