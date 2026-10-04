package rdsdata_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/blackbirdworks/gopherstack/services/rdsdata"
	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

const (
	dockerClusterWait = 5 * time.Minute
	dockerAccount     = "000000000000"
	dockerRegion      = "us-east-1"
	dockerMasterPass  = "masterpw123"
)

type dockerDataAPI struct {
	t          *testing.T
	h          *rdsdata.Handler
	clusterARN string
	secretARN  string
}

func newDockerDataAPI(t *testing.T, engine string) *dockerDataAPI {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(rds.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	rdsBk := rds.NewInMemoryBackend(dockerAccount, dockerRegion)
	rdsBk.EnableEngine(rds.EngineConfig{Runtime: er})
	t.Cleanup(rdsBk.Close)

	cluster, err := rdsBk.CreateDBCluster("datacluster", engine, "master", "appdb", "", 0, nil,
		rds.DBClusterOptions{MasterUserPassword: dockerMasterPass, EnableHTTPEndpoint: true})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := rdsBk.DescribeDBClusters("datacluster")
		require.NoError(t, derr)
		require.NotEqual(t, "failed", got[0].Status)

		return got[0].Status == "available"
	}, dockerClusterWait, 500*time.Millisecond)

	smBk := secretsmanager.NewInMemoryBackendWithConfig(dockerAccount, dockerRegion)
	secret, err := smBk.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
		Name:         "datacluster-creds",
		SecretString: `{"username":"master","password":"` + dockerMasterPass + `"}`,
	})
	require.NoError(t, err)

	dataBk := rdsdata.NewInMemoryBackend(dockerAccount, dockerRegion).WithRealEngine(rdsBk, smBk)
	t.Cleanup(dataBk.Close)

	return &dockerDataAPI{
		t: t, h: rdsdata.NewHandler(dataBk), clusterARN: cluster.DBClusterArn, secretARN: secret.ARN,
	}
}

func (d *dockerDataAPI) call(path string, body map[string]any) (int, map[string]any) {
	d.t.Helper()

	body["resourceArn"] = d.clusterARN
	if _, ok := body["secretArn"]; !ok {
		body["secretArn"] = d.secretARN
	}

	rec := doRDSDataRequest(d.t, d.h, path, body)

	out := map[string]any{}
	if rec.Body.Len() > 0 {
		require.NoError(d.t, json.Unmarshal(rec.Body.Bytes(), &out))
	}

	return rec.Code, out
}

func (d *dockerDataAPI) exec(sql string, extra map[string]any) map[string]any {
	d.t.Helper()

	body := map[string]any{"sql": sql}
	maps.Copy(body, extra)

	code, out := d.call("/Execute", body)
	require.Equal(d.t, http.StatusOK, code, "%s: %v", sql, out)

	return out
}

func (d *dockerDataAPI) count(txID string) float64 {
	d.t.Helper()

	extra := map[string]any{}
	if txID != "" {
		extra["transactionId"] = txID
	}

	out := d.exec("SELECT count(*) AS n FROM items", extra)
	rec := out["records"].([]any)[0].([]any)[0].(map[string]any)

	return rec["longValue"].(float64)
}

func sv(name, typeHint, v string) map[string]any {
	p := map[string]any{"name": name, "value": map[string]any{"stringValue": v}}
	if typeHint != "" {
		p["typeHint"] = typeHint
	}

	return p
}

func lv(name string, v int) map[string]any {
	return map[string]any{"name": name, "value": map[string]any{"longValue": v}}
}

const insertItem = "INSERT INTO items (id, name, born, price, meta) VALUES (:id, :name, :born, :price, :meta)"

func itemParams(id int, name string) []any {
	return []any{
		lv("id", id), sv("name", "", name), sv("born", "DATE", "2024-01-02"),
		sv("price", "DECIMAL", "12.50"), sv("meta", "JSON", `{"k": 1}`),
	}
}

func TestRealDockerDataAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		engine   string
		arraySQL string
		genDDL   string
	}{
		{name: "postgres", engine: "aurora-postgresql", arraySQL: "SELECT ARRAY[1,2,3] AS a"},
		{name: "mysql", engine: "aurora-mysql", genDDL: "CREATE TABLE gen (id INT AUTO_INCREMENT PRIMARY KEY, v INT)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := newDockerDataAPI(t, tt.engine)

			d.exec("CREATE TABLE items (id int primary key, name text, born date, price numeric(10,2), meta json)", nil)

			out := d.exec(insertItem, map[string]any{"parameters": itemParams(1, "alpha")})
			assert.InDelta(t, 1, out["numberOfRecordsUpdated"], 0)

			out = d.exec("SELECT id, name, born, price, meta FROM items WHERE name = :name AND born = :born",
				map[string]any{
					"parameters":            []any{sv("name", "", "alpha"), sv("born", "DATE", "2024-01-02")},
					"includeResultMetadata": true,
				})

			row := out["records"].([]any)[0].([]any)
			assert.InDelta(t, 1, row[0].(map[string]any)["longValue"], 0)
			assert.Equal(t, "alpha", row[1].(map[string]any)["stringValue"])
			assert.Equal(t, "2024-01-02", row[2].(map[string]any)["stringValue"])
			assert.Equal(t, "12.50", row[3].(map[string]any)["stringValue"])
			assert.JSONEq(t, `{"k": 1}`, row[4].(map[string]any)["stringValue"].(string))

			cols := out["columnMetadata"].([]any)
			require.Len(t, cols, 5)
			assert.Equal(t, "id", cols[0].(map[string]any)["name"])
			assert.InDelta(t, 4, cols[0].(map[string]any)["type"], 0)

			out = d.exec("SELECT id, name FROM items", map[string]any{"formatRecordsAs": "JSON"})
			assert.JSONEq(t, `[{"id":1,"name":"alpha"}]`, out["formattedRecords"].(string))

			assertTransactions(t, d)
			assertBatchAndErrors(t, d)

			if tt.genDDL != "" {
				d.exec(tt.genDDL, nil)

				out = d.exec("INSERT INTO gen (v) VALUES (:v)", map[string]any{"parameters": []any{lv("v", 7)}})
				gen := out["generatedFields"].([]any)
				require.Len(t, gen, 1)
				assert.InDelta(t, 1, gen[0].(map[string]any)["longValue"], 0)
			}

			if tt.arraySQL != "" {
				out = d.exec(tt.arraySQL, nil)
				arr := out["records"].([]any)[0].([]any)[0].(map[string]any)["arrayValue"].(map[string]any)
				assert.Equal(t, []any{float64(1), float64(2), float64(3)}, arr["longValues"])
			}
		})
	}
}

func assertTransactions(t *testing.T, d *dockerDataAPI) {
	t.Helper()

	tests := []struct {
		name      string
		finish    string
		id        int
		wantDelta float64
	}{
		{name: "commit", finish: "/CommitTransaction", id: 2, wantDelta: 1},
		{name: "rollback", finish: "/RollbackTransaction", id: 3, wantDelta: 0},
	}

	for _, tt := range tests {
		code, out := d.call("/BeginTransaction", map[string]any{})
		require.Equal(t, http.StatusOK, code, tt.name)

		txID := out["transactionId"].(string)
		base := d.count("")

		d.exec(insertItem, map[string]any{"parameters": itemParams(tt.id, tt.name), "transactionId": txID})
		assert.InDelta(t, base+1, d.count(txID), 0, "%s: own writes visible in the transaction", tt.name)
		assert.InDelta(t, base, d.count(""), 0, "%s: uncommitted rows invisible outside", tt.name)

		code, _ = d.call(tt.finish, map[string]any{"transactionId": txID})
		require.Equal(t, http.StatusOK, code, tt.name)
		assert.InDelta(t, base+tt.wantDelta, d.count(""), 0, tt.name)

		code, out = d.call("/Execute", map[string]any{"sql": "SELECT 1", "transactionId": txID})
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "TransactionNotFoundException", out["__type"])
	}
}

func assertBatchAndErrors(t *testing.T, d *dockerDataAPI) {
	t.Helper()

	sets := []any{
		[]any{lv("id", 10), sv("name", "", "b1")},
		[]any{lv("id", 11), sv("name", "", "b2")},
		[]any{lv("id", 12), sv("name", "", "b3")},
	}

	code, out := d.call("/BatchExecute", map[string]any{
		"sql": "INSERT INTO items (id, name) VALUES (:id, :name)", "parameterSets": sets,
	})
	require.Equal(t, http.StatusOK, code)
	assert.Len(t, out["updateResults"], 3)
	assert.InDelta(t, 5, d.count(""), 0)

	code, out = d.call("/Execute", map[string]any{
		"sql":        "INSERT INTO items (id, name) VALUES (:id, :name)",
		"parameters": []any{lv("id", 10), sv("name", "", "dup")},
	})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "DatabaseErrorException", out["__type"])

	badSecret := d.secretARN + "-missing"
	code, out = d.call("/Execute", map[string]any{"sql": "SELECT 1", "secretArn": badSecret})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "SecretsErrorException", out["__type"])
}
