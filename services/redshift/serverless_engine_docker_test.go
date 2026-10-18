package redshift_test

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/redshift"
	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

var errNoSuchKey = errors.New("NoSuchKey")

type memObjects struct {
	objs map[string][]byte
	mu   sync.Mutex
}

func (m *memObjects) GetObject(_ context.Context, bucket, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.objs[bucket+"/"+key]
	if !ok {
		return nil, errNoSuchKey
	}

	return b, nil
}

func (m *memObjects) PutObject(_ context.Context, bucket, key string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.objs[bucket+"/"+key] = body

	return nil
}

func (m *memObjects) DeleteObject(_ context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objs, bucket+"/"+key)

	return nil
}

func (m *memObjects) ListKeys(_ context.Context, bucket, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var keys []string

	for k := range m.objs {
		if rest, ok := strings.CutPrefix(k, bucket+"/"); ok && strings.HasPrefix(rest, prefix) {
			keys = append(keys, rest)
		}
	}

	sort.Strings(keys)

	return keys, nil
}

type workgroupEngines struct{ b *redshift.InMemoryBackend }

func (w workgroupEngines) WorkgroupEngineLogin(_, name string) (redshiftdata.EngineLogin, bool) {
	l, ok := w.b.WorkgroupEngineLogin(name)

	return redshiftdata.EngineLogin(l), ok
}

func TestWorkgroupEngineDockerCopyUnload(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(redshift.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(redshift.EngineConfig{Runtime: er})
	t.Cleanup(b.Close)

	_, err = b.CreateNamespace(redshift.CreateNamespaceParams{
		NamespaceName: "ns-docker", AdminUsername: "admin", AdminUserPassword: "Passw0rdXyz", DBName: "dev",
	})
	require.NoError(t, err)

	_, err = b.CreateWorkgroup("wg-docker", "ns-docker", redshift.WorkgroupParams{}, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return workgroupStatus(b, "wg-docker") == "AVAILABLE" },
		dockerEngineWait, 250*time.Millisecond)

	store := &memObjects{objs: map[string][]byte{}}

	var pq bytes.Buffer

	require.NoError(t, parquetio.Write(&pq,
		[]parquetio.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}},
		[][]any{{int64(30), "pq-a"}, {int64(31), nil}}, ""))

	store.objs["lake/in/part1.csv"] = []byte("id,name\n1,ada\n2,\n")
	store.objs["lake/in/part2.csv"] = []byte("id,name\n3,\"x,y\"\n")
	store.objs["lake/json/a.json"] = []byte(`{"id":10,"name":"j1"}` + "\n" + `{"ID":11}`)
	store.objs["lake/pq/a.parquet"] = pq.Bytes()

	data := redshiftdata.NewInMemoryBackend("000000000000", "us-east-1")
	data.SetWorkgroupEngines(workgroupEngines{b: b})
	data.SetObjectStore(store)

	run := func(sql string) *redshiftdata.Statement {
		t.Helper()

		stmt, execErr := data.ExecuteStatement(t.Context(), sql, "", "wg-docker", "dev", "", "", "", false, "", nil, "")
		require.NoError(t, execErr)

		return stmt
	}

	require.Equal(t, "FINISHED", run("CREATE TABLE people (id int, name varchar(20))").Status)

	for _, sql := range []string{
		"COPY people FROM 's3://lake/in/' IAM_ROLE 'arn:aws:iam::000000000000:role/r' CSV IGNOREHEADER 1",
		"COPY people (id, name) FROM 's3://lake/json/' IAM_ROLE DEFAULT FORMAT AS JSON 'auto'",
		"COPY people FROM 's3://lake/pq/' IAM_ROLE 'arn:aws:iam::000000000000:role/r' FORMAT AS PARQUET",
	} {
		st := run(sql)
		require.Equal(t, "FINISHED", st.Status, st.Error)
	}

	count := run("SELECT count(*), sum(id) FROM people")
	require.Equal(t, "FINISHED", count.Status, count.Error)

	rs, found, err := data.EngineResult(t.Context(), count.ID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, int64(7), *rs.Rows[0][0].LongValue)
	assert.Equal(t, int64(1+2+3+10+11+30+31), *rs.Rows[0][1].LongValue)

	missing := run("COPY people FROM 's3://lake/nothing/' CSV")
	assert.Equal(t, "FAILED", missing.Status)
	assert.Contains(t, missing.Error, "does not exist")

	csvOut := run("UNLOAD ('SELECT id, name FROM people WHERE id < 4 ORDER BY id') TO 's3://lake/out/csv_' " +
		"IAM_ROLE 'arn:aws:iam::000000000000:role/r' CSV HEADER MANIFEST")
	require.Equal(t, "FINISHED", csvOut.Status, csvOut.Error)
	assert.Equal(t, "id,name\n1,ada\n2,\n3,\"x,y\"\n", string(store.objs["lake/out/csv_0000_part_00.csv"]))
	assert.Contains(t, string(store.objs["lake/out/csv_manifest"]), "s3://lake/out/csv_0000_part_00.csv")

	again := run("UNLOAD ('SELECT 1') TO 's3://lake/out/csv_'")
	assert.Equal(t, "FAILED", again.Status)
	assert.Contains(t, again.Error, "not empty")

	pqOut := run("UNLOAD ('SELECT id, name FROM people WHERE id >= 30 ORDER BY id') TO 's3://lake/out/pq_' " +
		"FORMAT PARQUET")
	require.Equal(t, "FINISHED", pqOut.Status, pqOut.Error)

	f, err := parquetio.Open(store.objs["lake/out/pq_0000_part_00.parquet"])
	require.NoError(t, err)
	assert.Equal(t, int64(2), f.NumRows())
}
