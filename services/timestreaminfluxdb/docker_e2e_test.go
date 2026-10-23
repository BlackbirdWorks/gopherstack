package timestreaminfluxdb_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
)

const (
	e2eWait  = 3 * time.Minute
	e2eTick  = 500 * time.Millisecond
	e2ePoint = "m,t=a v=7i 1700000000"
)

func dockerEngineOrSkip(t *testing.T) *timestreaminfluxdb.DockerEngine {
	t.Helper()

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("no container runtime: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	cr, ok := rt.(timestreaminfluxdb.ContainerRuntime)
	if !ok {
		t.Skip("container runtime cannot restart containers")
	}

	if has, herr := rt.HasImage(t.Context(), timestreaminfluxdb.Image); herr != nil || !has {
		t.Skipf("%s is not pulled", timestreaminfluxdb.Image)
	}

	return &timestreaminfluxdb.DockerEngine{Runtime: cr}
}

func influxCall(ctx context.Context, t *testing.T, method, url, token, body string) string {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Token "+token)
	req.Header.Set("Content-Type", "application/json")

	res, err := httpclient.New(30 * time.Second).Do(req)
	require.NoError(t, err)

	defer res.Body.Close()

	out, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Less(t, res.StatusCode, 300, string(out))

	return string(out)
}

func (e *opsEnv) baseURL(t *testing.T, id *string) string {
	t.Helper()

	got, err := e.client.GetDbInstance(t.Context(), &sdk.GetDbInstanceInput{Identifier: id})
	require.NoError(t, err)

	return "http://" + aws.ToString(got.Endpoint) + ":" + strconv.Itoa(int(aws.ToInt32(got.Port)))
}

func (e *opsEnv) point(t *testing.T, id *string, token string) string {
	t.Helper()

	q := `{"type":"flux","query":"from(bucket:\"bucket\") |> range(start: 2023-11-01T00:00:00Z) ` +
		`|> filter(fn:(r)=>r._measurement==\"m\")"}`

	return influxCall(t.Context(), t, http.MethodPost, e.baseURL(t, id)+"/api/v2/query?org=organization", token, q)
}

func TestDockerRebootKeepsData(t *testing.T) {
	t.Parallel()

	engine := dockerEngineOrSkip(t)
	e := newOpsEnvWith(t, engine)

	out, err := e.client.CreateDbInstance(t.Context(), instanceInput("e2e-db"))
	require.NoError(t, err)

	waitAvail := func(id *string) {
		require.Eventually(
			t,
			func() bool { return status(t, e.fixture, id) == types.StatusAvailable },
			e2eWait,
			e2eTick,
		)
	}

	waitAvail(out.Id)

	token := secretToken(t, e, aws.ToString(out.InfluxAuthParametersSecretArn))
	influxCall(
		t.Context(),
		t,
		http.MethodPost,
		e.baseURL(t, out.Id)+"/api/v2/write?org=organization&bucket=bucket&precision=s",
		token,
		e2ePoint,
	)
	assert.Contains(t, e.point(t, out.Id, token), ",m,")

	_, err = e.client.RebootDbInstance(t.Context(), &sdk.RebootDbInstanceInput{Identifier: out.Id})
	require.NoError(t, err)
	waitAvail(out.Id)
	assert.Contains(t, e.point(t, out.Id, token), ",m,", "the point survives the reboot")

	bk, err := e.client.CreateDbBackup(
		t.Context(),
		&sdk.CreateDbBackupInput{DbResourceId: out.Id, Name: aws.String("e2e-backup")},
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return e.backupStatus(t, bk.Id) == types.DbBackupStatusCompleted
	}, e2eWait, e2eTick)

	restored, err := e.client.RestoreFromDbBackup(t.Context(), &sdk.RestoreFromDbBackupInput{
		DbBackupId: bk.Id, Name: aws.String("e2e-restored"),
	})
	require.NoError(t, err)
	waitAvail(restored.RestoredDbResourceId)
	assert.Contains(t, e.point(t, restored.RestoredDbResourceId, token), ",m,", "the restored instance has the data")

	for _, id := range []*string{restored.RestoredDbResourceId, out.Id} {
		_, err = e.client.DeleteDbInstance(t.Context(), &sdk.DeleteDbInstanceInput{Identifier: id})
		require.NoError(t, err)
	}
}

func secretToken(t *testing.T, e *opsEnv, arn string) string {
	t.Helper()

	body := e.secrets.all()[arn]
	_, after, ok := strings.Cut(body, `"token":"`)
	require.True(t, ok)

	tok, _, _ := strings.Cut(after, `"`)

	return tok
}

func TestDockerEngineCapturesLogs(t *testing.T) {
	t.Parallel()

	eng := dockerEngineOrSkip(t)
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(root+"/data", 0o750))
	require.NoError(t, os.MkdirAll(root+"/backups", 0o750))

	h, err := eng.Start(t.Context(), timestreaminfluxdb.EngineSpec{
		Name: "logs", Username: "admin", Password: "Passw0rdPassw0rd", Organization: "o", Bucket: "b", Token: "tok",
		DataDir: root + "/data", BackupsDir: root + "/backups",
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = eng.Stop(context.WithoutCancel(t.Context()), h) })

	var got string

	require.Eventually(t, func() bool {
		got += string(eng.DrainLogs(h))

		return strings.Contains(got, "Listening")
	}, e2eWait, e2eTick)

	_, err = eng.Restart(t.Context(), h)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got += string(eng.DrainLogs(h))

		return strings.Count(got, "msg=Listening") >= 2
	}, e2eWait, e2eTick, "the restart logs a second start without replaying the first")
	assert.Equal(t, 2, strings.Count(got, "msg=Listening"))
}
