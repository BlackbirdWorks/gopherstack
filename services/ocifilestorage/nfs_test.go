package ocifilestorage_test

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"

	svc "github.com/blackbirdworks/gopherstack/services/ocifilestorage"
)

func newNFSEnv(t *testing.T) *env {
	t.Helper()

	plane, err := svc.NewNFSPlane(svc.NFSConfig{Dir: t.TempDir()})
	require.NoError(t, err)

	return newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) { b.SetDataPlane(plane) }})
}

func (e *env) endpoint(t *testing.T, mtID *string) string {
	t.Helper()

	addr, ok := e.backend.NFSEndpoints()[*mtID]
	require.True(t, ok, "mount target %s has no NFS server", *mtID)

	return addr
}

// mount mounts an export path as an unprivileged NFSv3 client.
func mount(t *testing.T, addr, path string) (*nfsc.Target, error) {
	t.Helper()

	var (
		c   *rpc.Client
		err error
	)

	for range 50 { // the client picks a random source port and may collide with a parallel test
		if c, err = rpc.DialTCP("tcp", addr, false); err == nil {
			break
		}
	}

	require.NoError(t, err)
	t.Cleanup(c.Close)

	m := &nfsc.Mount{Client: c}

	target, err := m.Mount(path, rpc.AuthNull)
	if err == nil {
		t.Cleanup(func() { _ = m.Unmount() })
	}

	return target, err
}

func writeFile(t *testing.T, tg *nfsc.Target, path, content string) {
	t.Helper()

	_, err := tg.Create(path, 0o644)
	require.NoError(t, err)

	f, err := tg.OpenFile(path, 0o644)
	require.NoError(t, err)

	_, err = f.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func readFile(t *testing.T, tg *nfsc.Target, path string) (string, error) {
	t.Helper()

	f, err := tg.Open(path)
	if err != nil {
		return "", err
	}

	defer f.Close()

	b, err := io.ReadAll(f)

	return string(b), err
}

func names(t *testing.T, tg *nfsc.Target, dir string) []string {
	t.Helper()

	ents, err := tg.ReadDirPlus(dir)
	require.NoError(t, err)

	var out []string

	for _, en := range ents {
		if en.FileName != "." && en.FileName != ".." {
			out = append(out, en.FileName)
		}
	}

	return out
}

func TestNFSReadWriteAndSnapshots(t *testing.T) {
	t.Parallel()

	e := newNFSEnv(t)
	mt := e.createMT(t, nil)
	fs := e.createFS(t, nil)
	e.createExport(t, mt.ExportSetId, fs.Id, "/data", nil)

	tg, err := mount(t, e.endpoint(t, mt.Id), "/data")
	require.NoError(t, err)

	writeFile(t, tg, "/hello.txt", "version one")
	require.NoError(t, mkdir(tg, "/dir"))
	writeFile(t, tg, "/dir/nested.txt", "nested")

	got, err := readFile(t, tg, "/hello.txt")
	require.NoError(t, err)
	assert.Equal(t, "version one", got)
	assert.ElementsMatch(t, []string{"hello.txt", "dir"}, names(t, tg, "/"), ".snapshot stays hidden")

	assert.Positive(t, *e.getFS(t, fs.Id).MeteredBytes, "metered bytes come from the data")

	e.createSnapshot(t, fs.Id, "s1")
	writeFile(t, tg, "/hello.txt", "version two!")
	require.NoError(t, tg.Remove("/dir/nested.txt"))

	live, err := readFile(t, tg, "/hello.txt")
	require.NoError(t, err)
	assert.Equal(t, "version two!", live)

	old, err := readFile(t, tg, "/.snapshot/s1/hello.txt")
	require.NoError(t, err)
	assert.Equal(t, "version one", old, "the snapshot keeps the data as it was")

	nested, err := readFile(t, tg, "/.snapshot/s1/dir/nested.txt")
	require.NoError(t, err)
	assert.Equal(t, "nested", nested)
	assert.Equal(t, []string{"s1"}, names(t, tg, "/.snapshot"))

	_, err = tg.Create("/.snapshot/s1/new.txt", 0o644)
	require.Error(t, err, "snapshots are read-only")
	require.Error(t, tg.Remove("/.snapshot/s1/hello.txt"))

	sn := e.createSnapshot(t, fs.Id, "s2")
	assert.ElementsMatch(t, []string{"s1", "s2"}, names(t, tg, "/.snapshot"))

	_, err = e.client.DeleteSnapshot(t.Context(), filestorage.DeleteSnapshotRequest{SnapshotId: sn.Id})
	require.NoError(t, err)
	assert.Equal(t, []string{"s1"}, names(t, tg, "/.snapshot"))
}

func mkdir(tg *nfsc.Target, path string) error {
	_, err := tg.Mkdir(path, 0o755)

	return err
}

func TestNFSExportOptions(t *testing.T) {
	t.Parallel()

	no, yes := false, true
	tests := []struct {
		name      string
		opts      []filestorage.ClientOptions
		mountErr  bool
		writeable bool
	}{
		{name: "default", opts: nil, writeable: true},
		{
			name: "read-only",
			opts: []filestorage.ClientOptions{
				{
					Source:                      new("127.0.0.1/32"),
					Access:                      filestorage.ClientOptionsAccessOnly,
					RequirePrivilegedSourcePort: &no,
				},
			},
		},
		{
			name:      "read-write-cidr",
			opts:      []filestorage.ClientOptions{{Source: new("127.0.0.0/8"), RequirePrivilegedSourcePort: &no}},
			writeable: true,
		},
		{
			name:     "foreign-source",
			opts:     []filestorage.ClientOptions{{Source: new("10.0.0.0/8"), RequirePrivilegedSourcePort: &no}},
			mountErr: true,
		},
		{
			name:     "privileged-port",
			opts:     []filestorage.ClientOptions{{Source: new("127.0.0.1"), RequirePrivilegedSourcePort: &yes}},
			mountErr: true,
		},
		{name: "hidden", opts: []filestorage.ClientOptions{}, mountErr: true},
		{name: "first-match-wins", opts: []filestorage.ClientOptions{
			{Source: new("127.0.0.0/8"), Access: filestorage.ClientOptionsAccessOnly, RequirePrivilegedSourcePort: &no},
			{Source: new("127.0.0.1"), RequirePrivilegedSourcePort: &no},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newNFSEnv(t)
			mt := e.createMT(t, nil)
			fs := e.createFS(t, nil)
			e.createExport(t, mt.ExportSetId, fs.Id, "/x", tc.opts)

			tg, err := mount(t, e.endpoint(t, mt.Id), "/x")
			if tc.mountErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = tg.Create("/f", 0o644)
			if tc.writeable {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
		})
	}
}

func TestNFSExportChanges(t *testing.T) {
	t.Parallel()

	e := newNFSEnv(t)
	mt := e.createMT(t, nil)
	fs := e.createFS(t, nil)
	x := e.createExport(t, mt.ExportSetId, fs.Id, "/x", nil)
	addr := e.endpoint(t, mt.Id)

	_, err := mount(t, addr, "/nope")
	require.Error(t, err, "unknown export path")

	_, err = e.client.UpdateExport(t.Context(), filestorage.UpdateExportRequest{
		ExportId: x.Id, ExportOptions: []filestorage.ClientOptions{},
	})
	require.NoError(t, err)

	_, err = mount(t, addr, "/x")
	require.Error(t, err, "options are re-evaluated on every mount")

	_, err = e.client.DeleteExport(t.Context(), filestorage.DeleteExportRequest{ExportId: x.Id})
	require.NoError(t, err)

	_, err = mount(t, addr, "/x")
	require.Error(t, err)

	_, err = e.client.DeleteMountTarget(t.Context(), filestorage.DeleteMountTargetRequest{MountTargetId: mt.Id})
	require.NoError(t, err)
	assert.NotContains(t, e.backend.NFSEndpoints(), *mt.Id, "the NFS server stops with its mount target")

	conn, derr := net.DialTimeout("tcp", addr, time.Second)
	if derr == nil {
		_ = conn.Close()
	}

	require.Error(t, derr)
}

func TestNFSClone(t *testing.T) {
	t.Parallel()

	e := newNFSEnv(t)
	mt := e.createMT(t, nil)
	parent := e.createFS(t, nil)
	e.createExport(t, mt.ExportSetId, parent.Id, "/parent", nil)

	tg, err := mount(t, e.endpoint(t, mt.Id), "/parent")
	require.NoError(t, err)
	writeFile(t, tg, "/data.txt", "original")

	sn := e.createSnapshot(t, parent.Id, "base")
	writeFile(t, tg, "/data.txt", "changed!!")

	clone := e.createFS(t, func(d *filestorage.CreateFileSystemDetails) { d.SourceSnapshotId = sn.Id })
	e.createExport(t, mt.ExportSetId, clone.Id, "/clone", nil)

	ctg, err := mount(t, e.endpoint(t, mt.Id), "/clone")
	require.NoError(t, err)

	got, err := readFile(t, ctg, "/data.txt")
	require.NoError(t, err)
	assert.Equal(t, "original", got, "a clone starts from the snapshot")

	writeFile(t, ctg, "/clone-only.txt", "x")
	assert.NotContains(t, names(t, tg, "/"), "clone-only.txt", "clones diverge from their parent")

	assert.Empty(t, names(t, ctg, "/.snapshot"), "snapshots are not inherited")
}

func TestNFSReplicationCopiesData(t *testing.T) {
	t.Parallel()

	e := newNFSEnv(t)
	mt := e.createMT(t, nil)
	src := e.createFS(t, nil)
	dst := e.createFS(t, func(d *filestorage.CreateFileSystemDetails) { d.AvailabilityDomain = new(otherAD) })
	e.createExport(t, mt.ExportSetId, src.Id, "/src", nil)

	tg, err := mount(t, e.endpoint(t, mt.Id), "/src")
	require.NoError(t, err)
	writeFile(t, tg, "/replicated.txt", "payload")

	e.createReplication(t, src.Id, dst.Id)
	e.settle()

	for range 3 {
		e.clock.Advance(3 * time.Second)
		e.backend.Sweep()
	}

	mt2 := e.createMT(t, func(d *filestorage.CreateMountTargetDetails) {
		d.AvailabilityDomain = new(otherAD)
		d.DisplayName = new("mt2")
	})
	e.createExport(t, mt2.ExportSetId, dst.Id, "/dst", nil)

	dtg, err := mount(t, e.endpoint(t, mt2.Id), "/dst")
	require.NoError(t, err)

	got, err := readFile(t, dtg, "/replicated.txt")
	require.NoError(t, err)
	assert.Equal(t, "payload", got, "the target holds the replicated tree")

	_, err = dtg.Create("/local.txt", 0o644)
	require.Error(t, err, "a replication target is read-only")
}
