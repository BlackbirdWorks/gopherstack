package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/replicator"
)

func fakeReplicate(rep *replicator.Report, err error, seen *replicator.Config) replicateFunc {
	return func(_ context.Context, cfg replicator.Config) (*replicator.Report, error) {
		*seen = cfg

		for _, r := range rep.Results {
			if cfg.Progress != nil {
				cfg.Progress(r)
			}
		}

		return rep, err
	}
}

func replicateDo(e *echo.Echo, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func TestReplicateAPIJobLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runErr     error
		name       string
		body       string
		wantStatus string
		wantCode   int
	}{
		{
			name:       "completes",
			body:       `{"source":{"region":"eu-west-1"},"resource_types":["sqs"]}`,
			wantCode:   202,
			wantStatus: replicateDone,
		},
		{name: "run error", body: `{}`, wantCode: 202, runErr: assert.AnError, wantStatus: replicateFailed},
		{name: "unknown type", body: `{"resource_types":["ec2"]}`, wantCode: 400},
		{name: "bad json", body: `{`, wantCode: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var seen replicator.Config

			rep := &replicator.Report{Counts: map[string]int{"created": 1}, Results: []replicator.Result{
				{Type: "sqs", ID: "q", Action: "created"},
			}}

			e := echo.New()
			a := newReplicateAPI(t.Context(), fakeReplicate(rep, tt.runErr, &seen))
			e.POST(replicateBasePath, a.start)
			e.GET(replicateBasePath+"/:id", a.status)

			rec := replicateDo(e, http.MethodPost, replicateBasePath, tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusAccepted {
				return
			}

			var started map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &started))

			var job replicateJob

			require.Eventually(t, func() bool {
				r := replicateDo(e, http.MethodGet, replicateBasePath+"/"+started["id"], "")
				job = replicateJob{}

				return r.Code == http.StatusOK && json.Unmarshal(r.Body.Bytes(), &job) == nil &&
					job.Status != replicateRunning
			}, 5*time.Second, 5*time.Millisecond)

			assert.Equal(t, tt.wantStatus, job.Status)
			assert.Equal(t, 1, job.Counts["created"])
			assert.Len(t, job.Results, 1)
			assert.NotEmpty(t, seen.Target.URL, "target defaults to this server")
		})
	}
}

func TestReplicateAPIUnknownJob(t *testing.T) {
	t.Parallel()

	e := echo.New()
	a := newReplicateAPI(t.Context(), nil)
	e.GET(replicateBasePath+"/:id", a.status)

	assert.Equal(t, http.StatusNotFound, replicateDo(e, http.MethodGet, replicateBasePath+"/nope", "").Code)
}

func TestReplicateAPIDoesNotEchoCredentials(t *testing.T) {
	t.Parallel()

	var seen replicator.Config

	e := echo.New()
	a := newReplicateAPI(t.Context(), fakeReplicate(&replicator.Report{Counts: map[string]int{}}, nil, &seen))
	e.POST(replicateBasePath, a.start)
	e.GET(replicateBasePath+"/:id", a.status)

	rec := replicateDo(e, http.MethodPost, replicateBasePath,
		`{"source":{"access_key_id":"AKIASECRET","secret_access_key":"topsecret"}}`)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var started map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &started))

	got := replicateDo(e, http.MethodGet, replicateBasePath+"/"+started["id"], "")
	assert.NotContains(t, got.Body.String(), "topsecret")
	assert.NotContains(t, got.Body.String(), "AKIASECRET")
	assert.NotContains(t, rec.Body.String(), "topsecret")
}

func TestReplicateCmdReport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rep     *replicator.Report
		name    string
		wantOut string
		cmd     ReplicateCmd
		wantErr bool
	}{
		{
			name: "table", cmd: ReplicateCmd{Region: "us-east-1", Target: "http://t"},
			rep: &replicator.Report{
				Counts:  map[string]int{"created": 1},
				Results: []replicator.Result{{Type: "sqs", ID: "q", Action: "created"}},
			},
			wantOut: "created=1 updated=0 skipped=0 failed=0",
		},
		{
			name: "dry run banner", cmd: ReplicateCmd{DryRun: true, Region: "us-east-1"},
			rep:     &replicator.Report{DryRun: true, Counts: map[string]int{}},
			wantOut: "DRY RUN",
		},
		{
			name: "json", cmd: ReplicateCmd{JSON: true, Region: "us-east-1"},
			rep:     &replicator.Report{Counts: map[string]int{"created": 2}},
			wantOut: `"created": 2`,
		},
		{
			name: "failure exits non-zero", cmd: ReplicateCmd{Region: "us-east-1"},
			rep: &replicator.Report{
				Counts:  map[string]int{"failed": 1},
				Results: []replicator.Result{{Type: "s3", ID: "b", Action: "failed", Reason: "boom"}},
			},
			wantOut: "boom", wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				seen replicator.Config
				out  bytes.Buffer
			)

			err := tt.cmd.run(t.Context(), &out, fakeReplicate(tt.rep, nil, &seen))
			if tt.wantErr {
				require.ErrorIs(t, err, errReplicateFailed)
			} else {
				require.NoError(t, err)
			}

			assert.Contains(t, out.String(), tt.wantOut)
			assert.Equal(t, tt.cmd.Region, seen.Source.Region)
			assert.Equal(t, tt.cmd.DryRun, seen.DryRun)
		})
	}
}

func TestReplicateCmdTargetRegionDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantTargetRg string
		cmd          ReplicateCmd
	}{
		{name: "inherits source", cmd: ReplicateCmd{Region: "eu-west-1"}, wantTargetRg: "eu-west-1"},
		{
			name:         "explicit",
			cmd:          ReplicateCmd{Region: "eu-west-1", TargetRegion: "us-east-1"},
			wantTargetRg: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantTargetRg, tt.cmd.config().Target.Region)
		})
	}
}

func TestReplicateCmdParses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(*testing.T, ReplicateCmd)
		name  string
		args  []string
	}{
		{
			name: "flags",
			args: []string{
				"replicate", "--source-profile", "prod", "--region", "eu-west-1", "--target", "http://x:1",
				"--resource-type", "s3", "--resource-type", "sqs", "--identifier", "b", "--dry-run", "--include-data",
			},
			check: func(t *testing.T, c ReplicateCmd) {
				t.Helper()
				assert.Equal(t, "prod", c.SourceProfile)
				assert.Equal(t, "eu-west-1", c.Region)
				assert.Equal(t, "http://x:1", c.Target)
				assert.Equal(t, []string{"s3", "sqs"}, c.ResourceType)
				assert.True(t, c.DryRun)
				assert.True(t, c.IncludeData)
			},
		},
		{
			name: "defaults",
			args: []string{"replicate"},
			check: func(t *testing.T, c ReplicateCmd) {
				t.Helper()
				assert.Equal(t, "http://localhost:8000", c.Target)
				assert.EqualValues(t, 64<<20, c.MaxObjectBytes)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var root rootCLI

			parser, err := kong.New(&root, kong.Name("gopherstack"), kong.Writers(nil, nil))
			require.NoError(t, err)

			kctx, err := parser.Parse(tt.args)
			require.NoError(t, err)
			assert.Equal(t, "replicate", kctx.Command())
			tt.check(t, root.Replicate)
		})
	}
}
