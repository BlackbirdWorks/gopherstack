package persistence_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// --- NullStore ---

func TestNullStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(t *testing.T, s persistence.Store)
		name    string
		wantErr bool
	}{
		{
			name: "save_no_error",
			run: func(t *testing.T, s persistence.Store) {
				t.Helper()
				require.NoError(t, s.Save("svc", "k", []byte(`{}`)))
			},
		},
		{
			name:    "load_returns_key_not_found",
			wantErr: true,
			run: func(t *testing.T, s persistence.Store) {
				t.Helper()
				_, err := s.Load("svc", "k")
				require.ErrorIs(t, err, persistence.ErrKeyNotFound)
			},
		},
		{
			name: "delete_no_error",
			run: func(t *testing.T, s persistence.Store) {
				t.Helper()
				require.NoError(t, s.Delete("svc", "k"))
			},
		},
		{
			name: "list_keys_returns_empty",
			run: func(t *testing.T, s persistence.Store) {
				t.Helper()
				keys, err := s.ListKeys("svc")
				require.NoError(t, err)
				assert.Empty(t, keys)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var s persistence.NullStore
			tt.run(t, s)
		})
	}
}

// --- FileStore ---

func newTempFileStore(t *testing.T) *persistence.FileStore {
	t.Helper()

	dir := t.TempDir()
	s, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	return s
}

func TestFileStore_SaveLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		service string
		key     string
		data    []byte
		wantErr bool
	}{
		{
			name:    "basic_json",
			service: "sqs",
			key:     "snapshot",
			data:    []byte(`{"queues":{}}`),
		},
		{
			name:    "binary_data",
			service: "s3",
			key:     "snapshot",
			data:    []byte{0x00, 0x01, 0x02, 0xFF},
		},
		{
			name:    "empty_data",
			service: "sts",
			key:     "snapshot",
			data:    []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newTempFileStore(t)
			require.NoError(t, s.Save(tt.service, tt.key, tt.data))

			got, err := s.Load(tt.service, tt.key)
			require.NoError(t, err)
			assert.Equal(t, tt.data, got)
		})
	}
}

func TestFileStore_LoadNotFound(t *testing.T) {
	t.Parallel()

	s := newTempFileStore(t)
	_, err := s.Load("missing", "key")
	require.ErrorIs(t, err, persistence.ErrKeyNotFound)
}

func TestFileStore_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		preload bool
	}{
		{name: "existing_key", preload: true},
		{name: "missing_key", preload: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newTempFileStore(t)

			if tt.preload {
				require.NoError(t, s.Save("svc", "k", []byte(`{}`)))
			}

			require.NoError(t, s.Delete("svc", "k"))

			_, err := s.Load("svc", "k")
			require.ErrorIs(t, err, persistence.ErrKeyNotFound)
		})
	}
}

func TestFileStore_ListKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		saveKeys []string
		service  string
		wantKeys []string
	}{
		{
			name:     "empty_service",
			service:  "empty",
			wantKeys: nil,
		},
		{
			name:     "single_key",
			service:  "ssm",
			saveKeys: []string{"snapshot"},
			wantKeys: []string{"snapshot"},
		},
		{
			name:     "multiple_keys",
			service:  "ddb",
			saveKeys: []string{"a", "b", "c"},
			wantKeys: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newTempFileStore(t)

			for _, k := range tt.saveKeys {
				require.NoError(t, s.Save(tt.service, k, []byte(`{}`)))
			}

			got, err := s.ListKeys(tt.service)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantKeys, got)
		})
	}
}

func TestFileStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	s := newTempFileStore(t)
	const n = 50

	var wg sync.WaitGroup

	errs := make(chan error, n)

	for i := range n {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			data, _ := json.Marshal(map[string]int{"i": i})
			if err := s.Save("concurrent", "snap", data); err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	// Must be able to load the final value without error.
	data, err := s.Load("concurrent", "snap")
	require.NoError(t, err)

	var out map[string]int
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Contains(t, out, "i")
}

func TestFileStore_CorruptFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := persistence.NewFileStore(dir)
	require.NoError(t, err)

	// Write a non-JSON file directly to the store path.
	p := filepath.Join(dir, "svc", "snapshot.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o700))
	require.NoError(t, os.WriteFile(p, []byte("NOT JSON"), 0o600))

	// Load must succeed (raw bytes returned regardless of content).
	data, err := s.Load("svc", "snapshot")
	require.NoError(t, err)
	assert.Equal(t, []byte("NOT JSON"), data)
}

func TestFileStore_PathTraversal(t *testing.T) {
	t.Parallel()

	s := newTempFileStore(t)

	// Attempt path traversal in service and key names.
	require.NoError(t, s.Save("../evil", "../secret", []byte(`{}`)))

	// Must be loadable under the sanitised key.
	_, err := s.Load("../evil", "../secret")
	require.NoError(t, err)

	// Keys with path separators should be sanitised, not escape baseDir.
	keys, err := s.ListKeys("../evil")
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.NotContains(t, keys[0], string(os.PathSeparator))
}
