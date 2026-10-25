package azuremonitor_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/monitor/ingestion/azlogs"
	queryazlogs "github.com/Azure/azure-sdk-for-go/sdk/monitor/query/azlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

//nolint:gochecknoglobals // shared fixed clock
var testNow = time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

const sampleRecords = `[
 {"Time":"2024-03-01T10:00:00Z","Computer":"web1","Level":"Info","Count":1,"Value":1.5,"Ok":true,"Extra":{"a":1}},
 {"Time":"2024-03-01T10:30:00Z","Computer":"web1","Level":"Error","Count":2,"Value":4,"Ok":false,"Extra":[1,2]},
 {"Time":"2024-03-01T11:00:00Z","Computer":"web2","Level":"Info","Count":3,"Value":2.5,"Ok":true}
]`

func statusOf(err error) (int, string) {
	if re, ok := errors.AsType[*azcore.ResponseError](err); ok {
		return re.StatusCode, re.ErrorCode
	}

	return 0, ""
}

func (e *env) upload(t *testing.T, s seeded, body string) error {
	t.Helper()

	_, err := e.ingest.Upload(t.Context(), s.ruleID, "Custom-AppEvents", []byte(body), nil)

	return err
}

func (e *env) run(t *testing.T, s seeded, q string) queryazlogs.QueryWorkspaceResponse {
	t.Helper()

	res, err := e.query.QueryWorkspace(t.Context(), s.customerID, queryazlogs.QueryBody{Query: new(q)}, nil)
	require.NoError(t, err)
	require.Len(t, res.Tables, 1)

	return res
}

func TestIngestThenQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		transform string
		query     string
		wantRows  string
		wantCols  []string
	}{
		{
			name: "passthrough with time mapping", transform: "source | extend TimeGenerated = Time",
			query:    "AppEvents_CL | project Computer, Count | order by Count asc",
			wantCols: []string{"Computer", "Count"}, wantRows: `[["web1",1],["web1",2],["web2",3]]`,
		},
		{
			name: "transform filters rows", transform: "source | where Level == 'Error' | extend TimeGenerated = Time",
			query:    "AppEvents_CL | project Computer, Level",
			wantCols: []string{"Computer", "Level"}, wantRows: `[["web1","Error"]]`,
		},
		{
			name:      "transform projects and renames",
			transform: "source | extend TimeGenerated = Time, Level = toupper(Level)",
			query:     "AppEvents_CL | summarize n = count() by Level | order by Level asc",
			wantCols:  []string{"Level", "n"},
			wantRows:  `[["ERROR",1],["INFO",2]]`,
		},
		{
			name: "typed columns", transform: "source | extend TimeGenerated = Time",
			query:    "AppEvents_CL | where Ok == true | summarize s = sum(Value), c = sum(Count)",
			wantCols: []string{"s", "c"}, wantRows: `[[4,4]]`,
		},
		{
			name: "dynamic column", transform: "source | extend TimeGenerated = Time",
			query:    "AppEvents_CL | where Count == 1 | project a = Extra.a",
			wantCols: []string{"a"}, wantRows: `[["1"]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, azuremonitor.DefaultSettings())
			e.handler.Backend.SetClock(func() time.Time { return testNow })
			s := e.seed(t, tt.transform)
			require.NoError(t, e.upload(t, s, sampleRecords))

			res := e.run(t, s, tt.query)

			cols := make([]string, len(res.Tables[0].Columns))
			for i, c := range res.Tables[0].Columns {
				cols[i] = *c.Name
			}

			assert.Equal(t, tt.wantCols, cols)

			raw, err := json.Marshal(res.Tables[0].Rows)
			require.NoError(t, err)
			assert.JSONEq(t, tt.wantRows, string(raw))
		})
	}
}

func TestIngestTimeGeneratedDefaultsToIngestionTime(t *testing.T) {
	t.Parallel()

	e := newEnv(t, azuremonitor.DefaultSettings())
	e.handler.Backend.SetClock(func() time.Time { return testNow })
	s := e.seed(t, "source")
	require.NoError(t, e.upload(t, s, sampleRecords))

	res := e.run(t, s, "AppEvents_CL | project TimeGenerated | take 1")
	assert.Equal(t, "2024-03-01T12:00:00Z", res.Tables[0].Rows[0][0])
	assert.Equal(t, queryazlogs.ColumnTypeDatetime, *res.Tables[0].Columns[0].Type)
}

func TestIngestGzipAndErrors(t *testing.T) {
	t.Parallel()

	gz := func(s string) []byte {
		var buf bytes.Buffer

		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write([]byte(s))
		_ = zw.Close()

		return buf.Bytes()
	}

	tests := []struct {
		encoding   *string
		name       string
		rule       string
		stream     string
		wantCode   string
		body       []byte
		wantStatus int
	}{
		{name: "gzip accepted", body: gz(sampleRecords), encoding: new("gzip"), wantStatus: 0},
		{name: "unknown rule", rule: "dcr-00000000000000000000000000000000", body: []byte(sampleRecords),
			wantStatus: http.StatusNotFound, wantCode: "NotFound"},
		{name: "unknown stream", stream: "Custom-Nope", body: []byte(sampleRecords),
			wantStatus: http.StatusBadRequest, wantCode: "InvalidStream"},
		{name: "not an array", body: []byte(`{"a":1}`), wantStatus: http.StatusBadRequest, wantCode: "InvalidPayload"},
		{name: "wrong type", body: []byte(`[{"Time":"2024-03-01T10:00:00Z","Count":"many"}]`),
			wantStatus: http.StatusBadRequest, wantCode: "InvalidPayload"},
		{name: "bad datetime", body: []byte(`[{"Time":"yesterday"}]`),
			wantStatus: http.StatusBadRequest, wantCode: "InvalidPayload"},
		{name: "bad gzip", body: []byte("plain"), encoding: new("gzip"),
			wantStatus: http.StatusBadRequest, wantCode: "InvalidPayload"},
		{name: "unsupported encoding", body: []byte(sampleRecords), encoding: new("br"),
			wantStatus: http.StatusBadRequest, wantCode: "InvalidContentEncoding"},
		{name: "too large", body: bytes.Repeat([]byte(" "), 1<<20+1),
			wantStatus: http.StatusRequestEntityTooLarge, wantCode: "ContentLengthLimitExceeded"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, azuremonitor.Settings{StrictIngest: true})
			s := e.seed(t, "source | extend TimeGenerated = Time")

			rule, stream := tt.rule, tt.stream
			if rule == "" {
				rule = s.ruleID
			}

			if stream == "" {
				stream = "Custom-AppEvents"
			}

			_, err := e.ingest.Upload(
				t.Context(),
				rule,
				stream,
				tt.body,
				&azlogs.UploadOptions{ContentEncoding: tt.encoding},
			)
			if tt.wantStatus == 0 {
				require.NoError(t, err)

				return
			}

			status, code := statusOf(err)
			assert.Equal(t, tt.wantStatus, status)
			assert.Equal(t, tt.wantCode, code)
		})
	}
}

func TestIngestBadRecords(t *testing.T) {
	t.Parallel()

	body := `[{"Time":"2024-03-01T10:00:00Z","Computer":"ok","Count":1},{"Time":"2024-03-01T10:00:00Z","Count":"bad"}]`

	tests := []struct {
		name     string
		strict   bool
		wantRows float64
		wantCode string
	}{
		{name: "default drops bad records", wantRows: 1},
		{name: "strict rejects the upload", strict: true, wantCode: "InvalidPayload"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, azuremonitor.Settings{StrictIngest: tt.strict})
			s := e.seed(t, "source | extend TimeGenerated = Time")

			err := e.upload(t, s, body)
			if tt.wantCode != "" {
				_, code := statusOf(err)
				assert.Equal(t, tt.wantCode, code)

				return
			}

			require.NoError(t, err)

			res := e.run(t, s, "AppEvents_CL | count")
			assert.InDelta(t, tt.wantRows, res.Tables[0].Rows[0][0], 0)
		})
	}
}

func TestQueryErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		workspace string
		wantInner string
		wantCode  string
		wantHTTP  int
	}{
		{name: "syntax error", query: "AppEvents_CL | where |", wantHTTP: http.StatusBadRequest,
			wantCode: "BadArgumentError", wantInner: "SyntaxError"},
		{name: "unknown column", query: "AppEvents_CL | where Nope == 1", wantHTTP: http.StatusBadRequest,
			wantCode: "BadArgumentError", wantInner: "SemanticError"},
		{name: "unknown table", query: "Nope | take 1", wantHTTP: http.StatusBadRequest,
			wantCode: "BadArgumentError", wantInner: "SemanticError"},
		{name: "unsupported operator", query: "AppEvents_CL | join AppEvents_CL on Computer",
			wantHTTP: http.StatusBadRequest, wantCode: "BadArgumentError", wantInner: "SemanticError"},
		{name: "missing workspace", query: "Heartbeat | take 1", workspace: "00000000-0000-0000-0000-0000000000ff",
			wantHTTP: http.StatusNotFound, wantCode: "WorkspaceNotFoundError"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, azuremonitor.DefaultSettings())
			s := e.seed(t, "source")

			ws := s.customerID
			if tt.workspace != "" {
				ws = tt.workspace
			}

			_, err := e.query.QueryWorkspace(t.Context(), ws, queryazlogs.QueryBody{Query: new(tt.query)}, nil)
			require.Error(t, err)

			status, code := statusOf(err)
			assert.Equal(t, tt.wantHTTP, status)
			assert.Equal(t, tt.wantCode, code)

			if tt.wantInner != "" {
				assert.Contains(t, err.Error(), tt.wantInner)
			}
		})
	}
}

func TestQueryTimespanFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval string
		want     float64
	}{
		{name: "window around two rows", interval: "2024-03-01T10:15:00Z/2024-03-01T11:30:00Z", want: 2},
		{name: "all rows", interval: "2024-03-01T00:00:00Z/2024-03-02T00:00:00Z", want: 3},
		{name: "before everything", interval: "2024-02-01T00:00:00Z/2024-02-02T00:00:00Z", want: 0},
		{name: "start and duration", interval: "2024-03-01T10:00:00Z/PT45M", want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, azuremonitor.DefaultSettings())
			e.handler.Backend.SetClock(func() time.Time { return testNow })
			s := e.seed(t, "source | extend TimeGenerated = Time")
			require.NoError(t, e.upload(t, s, sampleRecords))

			iv := azlogs2Interval(tt.interval)
			res, err := e.query.QueryWorkspace(t.Context(), s.customerID,
				queryazlogs.QueryBody{Query: new("AppEvents_CL | count"), Timespan: &iv}, nil)
			require.NoError(t, err)
			assert.InDelta(t, tt.want, res.Tables[0].Rows[0][0], 0)
		})
	}
}

func azlogs2Interval(s string) queryazlogs.TimeInterval { return queryazlogs.TimeInterval(s) }

func TestQueryBatchAndResource(t *testing.T) {
	t.Parallel()

	e := newEnv(t, azuremonitor.DefaultSettings())
	e.handler.Backend.SetClock(func() time.Time { return testNow })
	s := e.seed(t, "source | extend TimeGenerated = Time")
	require.NoError(t, e.upload(t, s, sampleRecords))

	batch, err := e.query.QueryBatch(t.Context(), queryazlogs.BatchRequest{Requests: []queryazlogs.BatchQueryRequest{
		{ID: new("1"), Method: new("POST"), Path: new("/query"), WorkspaceID: new(s.customerID),
			Body: &queryazlogs.QueryBody{Query: new("AppEvents_CL | count")}},
		{ID: new("2"), Method: new("POST"), Path: new("/query"), WorkspaceID: new(s.customerID),
			Body: &queryazlogs.QueryBody{Query: new("AppEvents_CL | where")}},
		{ID: new("3"), Method: new("POST"), Path: new("/query"), WorkspaceID: new("missing"),
			Body: &queryazlogs.QueryBody{Query: new("Heartbeat")}},
	}}, nil)
	require.NoError(t, err)
	require.Len(t, batch.Responses, 3)

	assert.EqualValues(t, http.StatusOK, *batch.Responses[0].Status)
	assert.InDelta(t, 3, batch.Responses[0].Body.Tables[0].Rows[0][0], 0)
	assert.EqualValues(t, http.StatusBadRequest, *batch.Responses[1].Status)
	assert.Equal(t, "BadArgumentError", batch.Responses[1].Body.Error.Code)
	assert.EqualValues(t, http.StatusNotFound, *batch.Responses[2].Status)

	res, err := e.query.QueryResource(t.Context(), strings.TrimPrefix(wsID("testws"), "/"),
		queryazlogs.QueryBody{Query: new("AppEvents_CL | count")}, nil)
	require.NoError(t, err)
	assert.InDelta(t, 3, res.Tables[0].Rows[0][0], 0)
}

func TestQueryWorkspaceByNameAndStatistics(t *testing.T) {
	t.Parallel()

	e := newEnv(t, azuremonitor.DefaultSettings())
	e.handler.Backend.SetClock(func() time.Time { return testNow })
	s := e.seed(t, "source | extend TimeGenerated = Time")
	require.NoError(t, e.upload(t, s, sampleRecords))

	res, err := e.query.QueryWorkspace(
		t.Context(),
		"testws",
		queryazlogs.QueryBody{Query: new("AppEvents_CL | count")},
		&queryazlogs.QueryWorkspaceOptions{Options: &queryazlogs.QueryOptions{Statistics: new(true)}},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, res.Statistics)
}

func TestBuiltinTablesAreQueryable(t *testing.T) {
	t.Parallel()

	e := newEnv(t, azuremonitor.DefaultSettings())
	s := e.seed(t, "source")

	res := e.run(t, s, "Heartbeat | getschema | where ColumnName == 'Computer' | project ColumnType")
	assert.Equal(t, []queryazlogs.Row{{"string"}}, res.Tables[0].Rows)
}

func TestAuthRequired(t *testing.T) {
	t.Parallel()

	e := newEnv(t, azuremonitor.DefaultSettings())

	tests := []struct {
		name string
		url  string
		auth string
		want int
	}{
		{name: "query without token", url: "/v1/workspaces/x/query", want: http.StatusUnauthorized},
		{name: "ingest without token", url: "/dataCollectionRules/dcr-x/streams/s?api-version=2023-01-01",
			want: http.StatusUnauthorized},
		{name: "unknown path", url: "/nope", auth: "Bearer a.b.c", want: http.StatusNotFound},
		{name: "unknown workspace with token", url: "/v1/workspaces/x/query", auth: "Bearer a.b.c",
			want: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.srv.URL+tt.url,
				strings.NewReader(`{"query":"Heartbeat"}`))
			require.NoError(t, err)

			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}

			resp, err := e.srv.Client().Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

func TestValidateTokensRejectsMalformed(t *testing.T) {
	t.Parallel()

	e := newEnv(t, azuremonitor.Settings{ValidateTokens: true})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.srv.URL+"/v1/workspaces/x/query",
		strings.NewReader(`{"query":"Heartbeat"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer not-a-jwt")

	resp, err := e.srv.Client().Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestSnapshotRestoreKeepsRows(t *testing.T) {
	t.Parallel()

	e := newEnv(t, azuremonitor.DefaultSettings())
	e.handler.Backend.SetClock(func() time.Time { return testNow })
	s := e.seed(t, "source | extend TimeGenerated = Time")
	require.NoError(t, e.upload(t, s, sampleRecords))

	snap := e.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	e2 := newEnv(t, azuremonitor.DefaultSettings())
	require.NoError(t, e2.handler.Restore(t.Context(), snap))

	res := e2.run(t, s, "AppEvents_CL | order by Count asc | project Count, Value, Ok, Extra")
	raw, err := json.Marshal(res.Tables[0].Rows)
	require.NoError(t, err)
	assert.JSONEq(t, `[[1,1.5,true,"{\"a\":1}"],[2,4,false,"[1,2]"],[3,2.5,true,null]]`, string(raw))
	assert.Equal(t, queryazlogs.ColumnTypeLong, *res.Tables[0].Columns[0].Type)

	// the restored DCR still accepts uploads
	require.NoError(t, e2.upload(t, s, sampleRecords))
}
