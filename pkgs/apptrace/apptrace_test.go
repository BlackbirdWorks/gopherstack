package apptrace_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/apptrace"
)

type fakeAPI struct {
	hdr    map[string]string
	body   string
	status int
}

func serve(t *testing.T, tr *apptrace.Tracer, api fakeAPI, req *http.Request) (*httptest.ResponseRecorder, string) {
	t.Helper()

	e := echo.New()
	e.Use(apptrace.Middleware(tr))

	var seenBody string

	e.Any("/*", func(c *echo.Context) error {
		b, _ := io.ReadAll(c.Request().Body)
		seenBody = string(b)

		apptrace.FromContext(c.Request().Context()).SetOperation("sqs", "SendMessage", "q1")

		for k, v := range api.hdr {
			c.Response().Header().Set(k, v)
		}

		return c.String(api.status, api.body)
	})

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec, seenBody
}

func TestMiddlewareRecordsMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		api      fakeAPI
		status   int
	}{
		{name: "ok", api: fakeAPI{status: 200, body: `{}`}, status: 200},
		{
			name: "json type", status: 400, wantCode: "QueueDoesNotExist",
			api: fakeAPI{status: 400, body: `{"__type":"com.amazonaws#QueueDoesNotExist","message":"x"}`},
		},
		{
			name: "xml code", status: 404, wantCode: "NoSuchKey",
			api: fakeAPI{status: 404, body: `<Error><Code>NoSuchKey</Code></Error>`},
		},
		{
			name: "header", status: 500, wantCode: "Boom",
			api: fakeAPI{status: 500, body: `oops`, hdr: map[string]string{"X-Amzn-Errortype": "Boom:http://x"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr := apptrace.New(apptrace.Config{Capacity: 8, Enabled: true})
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("hello"))
			rec, seen := serve(t, tr, tc.api, req)

			assert.Equal(t, "hello", seen)
			assert.Equal(t, tc.status, rec.Code)

			res := tr.List(apptrace.Filter{})
			require.Len(t, res.Items, 1)

			got := res.Items[0]
			assert.Equal(t, "sqs", got.Service)
			assert.Equal(t, "SendMessage", got.Operation)
			assert.Equal(t, tc.status, got.Status)
			assert.Equal(t, tc.wantCode, got.ErrorCode)
			assert.Equal(t, int64(5), got.RequestBytes)
			assert.Equal(t, int64(len(tc.api.body)), got.ResponseBytes)
			assert.Nil(t, got.Detail)
			assert.False(t, got.HasDetail)
		})
	}
}

func TestMiddlewareSkipsAndDisabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		enabled bool
		want    int
	}{
		{"api", "/x", true, 1},
		{"dashboard", "/dashboard/api", true, 0},
		{"internal", "/_gopherstack/apptrace/requests", true, 0},
		{"disabled", "/x", false, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr := apptrace.New(apptrace.Config{Capacity: 4, Enabled: tc.enabled})
			serve(t, tr, fakeAPI{status: 200}, httptest.NewRequest(http.MethodGet, tc.path, nil))

			assert.Len(t, tr.List(apptrace.Filter{}).Items, tc.want)
		})
	}
}

func TestDetailCaptureRedacts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		reqBody     string
		respBody    string
		wantReq     string
		notInReq    string
		notInResp   string
		maxBody     int
		wantReqTrnc bool
	}{
		{
			name:      "json secrets",
			reqBody:   `{"SecretString":"hunter2","Name":"db","Password":"p@ss"}`,
			respBody:  `{"SecretAccessKey":"abc123","NextToken":"keepme"}`,
			maxBody:   1024,
			wantReq:   `"Name":"db"`,
			notInReq:  "hunter2",
			notInResp: "abc123",
		},
		{
			name:     "form secrets",
			reqBody:  "Action=CreateLoginProfile&Password=sekrit&UserName=bob",
			respBody: `<SessionToken>tok</SessionToken>`,
			maxBody:  1024, wantReq: "UserName=bob", notInReq: "sekrit", notInResp: ">tok<",
		},
		{
			name:    "truncated mid secret",
			reqBody: `{"Name":"aaaaaaaaaaaaaaaa","Password":"supersecretvalue"}`,
			maxBody: 40, wantReq: `"Name"`, notInReq: "supersecret", wantReqTrnc: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr := apptrace.New(apptrace.Config{Capacity: 4, Enabled: true, Detail: true, MaxBody: tc.maxBody})
			req := httptest.NewRequest(
				http.MethodPost, "/?X-Amz-Signature=sig&Action=Go", strings.NewReader(tc.reqBody),
			)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AK/x, Signature=zzz")
			req.Header.Set("X-Amz-Security-Token", "tok-secret")
			req.Header.Set("X-Custom", "visible")

			_, seen := serve(t, tr, fakeAPI{status: 200, body: tc.respBody}, req)
			assert.Equal(t, tc.reqBody, seen)

			span := tr.List(apptrace.Filter{}).Items[0]
			assert.True(t, span.HasDetail)

			full, ok := tr.Get(span.ID)
			require.True(t, ok)
			require.NotNil(t, full.Detail)

			d := full.Detail
			assert.Equal(t, apptrace.Redacted, d.RequestHeaders["Authorization"])
			assert.Equal(t, apptrace.Redacted, d.RequestHeaders["X-Amz-Security-Token"])
			assert.Equal(t, "visible", d.RequestHeaders["X-Custom"])
			assert.NotContains(t, d.Query, "sig")
			assert.Contains(t, d.Query, "Action=Go")
			assert.Contains(t, d.RequestBody, tc.wantReq)
			assert.NotContains(t, d.RequestBody, tc.notInReq)
			assert.Equal(t, tc.wantReqTrnc, d.RequestTrunc)

			if tc.notInResp != "" {
				assert.NotContains(t, d.ResponseBody, tc.notInResp)
			}

			if strings.Contains(tc.respBody, "NextToken") {
				assert.Contains(t, d.ResponseBody, "keepme")
			}
		})
	}
}

func TestChildSpansLinkToParent(t *testing.T) {
	t.Parallel()

	tr := apptrace.New(apptrace.Config{Capacity: 8, Enabled: true})
	e := echo.New()
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			c.Response().Header().Set("X-Amz-Request-Id", "req-1")

			return next(c)
		}
	})
	e.Use(apptrace.Middleware(tr))
	e.POST("/*", func(c *echo.Context) error {
		apptrace.FromContext(c.Request().Context()).SetOperation("sns", "Publish", "t")

		ctx, child := apptrace.StartChild(
			c.Request().Context(), apptrace.ChildInfo{Service: "sqs", Operation: "SendMessage"},
		)
		_, grand := apptrace.StartChild(ctx, apptrace.ChildInfo{Service: "lambda", Operation: "Invoke"})
		grand.Finish("")
		child.Finish("QueueDoesNotExist")

		return c.NoContent(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	e.ServeHTTP(httptest.NewRecorder(), req)

	all := tr.Trace("req-1")
	require.Len(t, all, 3)

	byOp := map[string]apptrace.Span{}
	for _, s := range all {
		byOp[s.Operation] = s
	}

	assert.Equal(t, "req-1", byOp["SendMessage"].ParentID)
	assert.Equal(t, byOp["SendMessage"].ID, byOp["Invoke"].ParentID)
	assert.Equal(t, "QueueDoesNotExist", byOp["SendMessage"].ErrorCode)
	assert.Equal(t, apptrace.KindInternal, byOp["Invoke"].Kind)

	_, none := apptrace.StartChild(context.Background(), apptrace.ChildInfo{})
	assert.Nil(t, none)
	none.Finish("x")
}

func TestRingAndFilter(t *testing.T) {
	t.Parallel()

	tr := apptrace.New(apptrace.Config{Capacity: 5, Enabled: true})

	for i := range 8 {
		status, svc := 200, "s3"

		if i%2 == 1 {
			status, svc = 404, "sqs"
		}

		tr.Record(&apptrace.Span{
			ID: "id" + strconv.Itoa(i), TraceID: "id" + strconv.Itoa(i), Service: svc,
			Operation: "Op" + strconv.Itoa(i), Status: status, Region: "us-east-1",
		})
	}

	tests := []struct {
		name   string
		first  string
		filter apptrace.Filter
		total  int
	}{
		{name: "all retained", filter: apptrace.Filter{}, total: 5, first: "id7"},
		{name: "service", filter: apptrace.Filter{Service: "SQS"}, total: 3, first: "id7"},
		{name: "errors", filter: apptrace.Filter{ErrorsOnly: true}, total: 3, first: "id7"},
		{name: "operation substring", filter: apptrace.Filter{Operation: "op4"}, total: 1, first: "id4"},
		{name: "query", filter: apptrace.Filter{Query: "id5"}, total: 1, first: "id5"},
		{name: "after seq", filter: apptrace.Filter{AfterSeq: 7}, total: 1, first: "id7"},
		{name: "none", filter: apptrace.Filter{Region: "eu-west-1"}, total: 0, first: ""},
		{name: "page", filter: apptrace.Filter{Limit: 2, Offset: 1}, total: 5, first: "id6"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res := tr.List(tc.filter)
			assert.Equal(t, tc.total, res.Total)

			if tc.first != "" {
				require.NotEmpty(t, res.Items)
				assert.Equal(t, tc.first, res.Items[0].ID)
			}
		})
	}

	assert.Equal(t, uint64(8), tr.Recorded())
}

func TestClear(t *testing.T) {
	t.Parallel()

	tr := apptrace.New(apptrace.Config{Capacity: 3, Enabled: true})
	tr.Record(&apptrace.Span{ID: "a"})
	tr.Clear()
	assert.Empty(t, tr.List(apptrace.Filter{}).Items)

	tr.Record(&apptrace.Span{ID: "b"})
	assert.Len(t, tr.List(apptrace.Filter{}).Items, 1)
}
