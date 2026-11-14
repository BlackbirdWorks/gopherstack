package lambda

import (
	"archive/zip"
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

type archiveRuntime struct {
	container.Runtime
	specs    []container.Spec
	delivers bool
}

func (r *archiveRuntime) DeliversArchives() bool { return r.delivers }

func (r *archiveRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.specs = append(r.specs, spec)

	return "pod-1", nil
}

func archiveZip(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	f, err := zw.Create("index.py")
	require.NoError(t, err)

	_, err = f.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestStartContainer_ArchiveRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(*testing.T, container.Spec)
		fn      func([]byte) *FunctionConfiguration
		name    string
		wantErr bool
	}{
		{
			name: "zip function",
			fn: func(z []byte) *FunctionConfiguration {
				return &FunctionConfiguration{
					FunctionName: "my_fn", PackageType: PackageTypeZip, Runtime: "python3.12",
					Handler: "index.handler", MemorySize: 256, ZipData: z,
				}
			},
			check: func(t *testing.T, s container.Spec) {
				t.Helper()
				assert.Equal(t, baseImageForRuntime("python3.12"), s.Image)
				assert.Equal(t, []string{"index.handler"}, s.Cmd)
				assert.Equal(t, int64(256<<20), s.MemoryBytes)
				assert.Empty(t, s.Mounts)
				require.Len(t, s.Archives, 1)
				assert.Equal(t, "/var/task", s.Archives[0].Dest)
			},
		},
		{
			name: "custom runtime",
			fn: func(z []byte) *FunctionConfiguration {
				return &FunctionConfiguration{
					FunctionName: "fn",
					PackageType:  PackageTypeZip,
					Runtime:      "provided.al2023",
					MemorySize:   128,
					ZipData:      z,
				}
			},
			check: func(t *testing.T, s container.Spec) {
				t.Helper()
				assert.Equal(t, []string{"/var/task/bootstrap"}, s.Entrypoint)
				assert.Empty(t, s.Cmd)
			},
		},
		{
			name: "image function",
			fn: func([]byte) *FunctionConfiguration {
				return &FunctionConfiguration{
					FunctionName: "fn",
					PackageType:  PackageTypeImage,
					ImageURI:     "repo/img:1",
					MemorySize:   128,
				}
			},
			check: func(t *testing.T, s container.Spec) {
				t.Helper()
				assert.Equal(t, "repo/img:1", s.Image)
				assert.Empty(t, s.Archives)
			},
		},
		{
			name:    "no code",
			wantErr: true,
			fn: func([]byte) *FunctionConfiguration {
				return &FunctionConfiguration{
					FunctionName: "fn",
					PackageType:  PackageTypeZip,
					Runtime:      "python3.12",
					MemorySize:   128,
				}
			},
		},
		{
			name:    "unknown runtime",
			wantErr: true,
			fn: func(z []byte) *FunctionConfiguration {
				return &FunctionConfiguration{
					FunctionName: "fn",
					PackageType:  PackageTypeZip,
					Runtime:      "nope1",
					MemorySize:   128,
					ZipData:      z,
				}
			},
		},
		{
			name:    "efs refused",
			wantErr: true,
			fn: func(z []byte) *FunctionConfiguration {
				return &FunctionConfiguration{
					FunctionName: "fn",
					PackageType:  PackageTypeZip,
					Runtime:      "python3.12",
					MemorySize:   128,
					ZipData:      z,
					FileSystemConfigs: []*FileSystemConfig{
						{Arn: "arn:aws:elasticfilesystem:::access-point/x", LocalMountPath: "/mnt/x"},
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &archiveRuntime{delivers: true}
			b := NewInMemoryBackend(rt, nil, DefaultSettings(), "000000000000", "us-east-1")
			t.Cleanup(func() { b.Close(context.Background()) })

			zipDir, layerDirs, id, err := b.startContainer(t.Context(), tt.fn(archiveZip(t)), []string{"K=V"}, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.Empty(t, rt.specs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "pod-1", id)
			assert.Empty(t, zipDir)
			assert.Empty(t, layerDirs)
			require.Len(t, rt.specs, 1)
			assert.Equal(t, []string{"K=V"}, rt.specs[0].Env)
			tt.check(t, rt.specs[0])
		})
	}
}

func TestDeliversArchives(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rt   container.Runtime
		name string
		want bool
	}{
		{name: "nil runtime", rt: nil},
		{name: "plain runtime", rt: &archiveRuntime{}},
		{name: "archive runtime", rt: &archiveRuntime{delivers: true}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend(tt.rt, nil, DefaultSettings(), "000000000000", "us-east-1")
			t.Cleanup(func() { b.Close(context.Background()) })

			assert.Equal(t, tt.want, b.deliversArchives())
		})
	}
}
