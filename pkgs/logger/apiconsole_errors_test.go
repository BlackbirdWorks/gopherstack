package logger_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestExtractErrorFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode string
		wantMsg  string
	}{
		{
			"json_type_prefixed",
			`{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Table not found"}`,
			"ResourceNotFoundException",
			"Table not found",
		},
		{"json_type_capital_message", `{"__type":"ValidationException","Message":"bad"}`, "ValidationException", "bad"},
		{"json_code_lower", `{"code":"Throttling","message":"slow down"}`, "Throttling", "slow down"},
		{"json_code_upper", `{"Code":"AccessDenied","Message":"no"}`, "AccessDenied", "no"},
		{"json_escaped_message", `{"__type":"X","message":"say \"hi\"\n"}`, "X", "say \"hi\"\n"},
		{"json_truncated", `{"__type":"InternalFailure","message":"long mess`, "InternalFailure", ""},
		{"json_digit_code", `{"__type":"S3Error404","message":"m"}`, "S3Error404", "m"},
		{"json_suffix_stripped", `{"__type":"Foo:http://internal.amazon.com/","message":"m"}`, "Foo", "m"},
		{
			"xml_s3",
			`<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>The key does not exist</Message></Error>`,
			"NoSuchKey",
			"The key does not exist",
		},
		{"xml_query", "<ErrorResponse><Error><Type>Sender</Type><Code>InvalidParameterValue</Code>" +
			"<Message>a &amp; b</Message></Error></ErrorResponse>", "InvalidParameterValue", "a & b"},
		{"xml_digit_code", `<Error><Code>Route53Error2</Code><Message>m</Message></Error>`, "Route53Error2", "m"},
		{
			"xml_digit_code_ec2",
			`<Response><Errors><Error><Code>InvalidVpcID.NotFound</Code><Message>m</Message></Error></Errors></Response>`,
			"InvalidVpcID.NotFound",
			"m",
		},
		{"plain_text", `Not Found`, "", ""},
		{"empty", ``, "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, msg := logger.ExtractErrorFields([]byte(tt.body))
			assert.Equal(t, tt.wantCode, code)
			assert.Equal(t, tt.wantMsg, msg)
		})
	}
}

func TestAPIConsoleMiddleware_CapturesEnrichment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		handler     echo.HandlerFunc
		name        string
		path        string
		query       string
		auth        string
		wantService string
		wantQuery   string
		wantCode    string
		wantMsg     string
		wantStatus  int
	}{
		{
			name: "json_error_sigv4_service", path: "/enrich/json", query: "a=1",
			auth: "AWS4-HMAC-SHA256 Credential=AKIA1/20260101/us-east-1/dynamodb/aws4_request, " +
				"SignedHeaders=host, Signature=abc",
			wantService: "dynamodb", wantQuery: "a=1", wantStatus: 400,
			wantCode: "ResourceNotFoundException", wantMsg: "gone",
			handler: func(c *echo.Context) error {
				return c.JSON(
					http.StatusBadRequest,
					map[string]string{"__type": "x#ResourceNotFoundException", "message": "gone"},
				)
			},
		},
		{
			name: "xml_error_router_tag", path: "/enrich/xml",
			wantService: "S3", wantStatus: 404, wantCode: "NoSuchKey2", wantMsg: "missing",
			handler: func(c *echo.Context) error {
				c.SetRequest(c.Request().WithContext(logger.WithService(c.Request().Context(), "S3")))

				return c.XMLBlob(
					http.StatusNotFound,
					[]byte(`<Error><Code>NoSuchKey2</Code><Message>missing</Message></Error>`),
				)
			},
		},
		{
			name: "success_no_error", path: "/enrich/ok", wantStatus: 200,
			handler: func(c *echo.Context) error {
				return c.JSON(http.StatusOK, map[string]string{"__type": "ignored", "message": "ignored"})
			},
		},
		{
			name:        "presigned_query_redacted",
			path:        "/enrich/presign",
			query:       "X-Amz-Signature=deadbeef&X-Amz-Credential=AKIA%2F20260101%2Fus-east-1%2Fs3%2Faws4_request&keep=1",
			wantQuery:   "X-Amz-Signature=[REDACTED]&X-Amz-Credential=[REDACTED]&keep=1",
			wantService: "s3",
			wantStatus:  200,
			handler:     func(c *echo.Context) error { return c.String(http.StatusOK, "ok") },
		},
		{
			name: "handler_error", path: "/enrich/err", wantStatus: 500, wantMsg: "boom",
			handler: func(*echo.Context) error { return assert.AnError },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			e.Use(logger.APIConsoleMiddleware())
			e.GET(tt.path, tt.handler)

			target := tt.path
			if tt.query != "" {
				target += "?" + tt.query
			}

			req := httptest.NewRequest(http.MethodGet, target, nil)
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}

			e.ServeHTTP(httptest.NewRecorder(), req)

			var got *logger.CapturedRequest

			for _, r := range logger.GlobalRingBuffer.GetAll() {
				if r.Path == tt.path {
					got = r
				}
			}

			require.NotNil(t, got)
			assert.Equal(t, tt.wantStatus, got.Status)
			assert.Equal(t, tt.wantQuery, got.Query)
			assert.Equal(t, tt.path, got.Path)
			assert.Equal(t, tt.wantCode, got.ErrorCode)

			if tt.name == "handler_error" {
				assert.Contains(t, got.ErrorMessage, assert.AnError.Error())
			} else {
				assert.Equal(t, tt.wantMsg, got.ErrorMessage)
			}

			if tt.wantService != "" {
				assert.Equal(t, tt.wantService, got.Service)
			}
		})
	}
}

func TestRequestRingBuffer_RecentAndCopies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantPaths []string
		adds      int
		size      int
		n         int
	}{
		{"partial_newest_first", []string{"/2", "/1", "/0"}, 3, 5, 10},
		{"wrapped_newest_first", []string{"/6", "/5", "/4"}, 7, 3, 3},
		{"limited", []string{"/6", "/5"}, 7, 3, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rb := logger.NewRequestRingBuffer(tt.size)
			for i := range tt.adds {
				rb.Add(newCapturedRequest(http.MethodGet, "/"+strconv.Itoa(i), 200))
			}

			got := rb.Recent(tt.n, nil)
			paths := make([]string, 0, len(got))

			for _, r := range got {
				paths = append(paths, r.Path)
			}

			assert.Equal(t, tt.wantPaths, paths)

			got[0].Headers["Content-Type"] = "mutated"
			got[0].Path = "mutated"

			again := rb.GetAll()
			for _, r := range again {
				assert.Equal(t, "application/json", r.Headers["Content-Type"])
				assert.NotEqual(t, "mutated", r.Path)
			}
		})
	}
}

func TestRequestRingBuffer_AddDoesNotAliasCaller(t *testing.T) {
	t.Parallel()

	rb := logger.NewRequestRingBuffer(2)
	req := newCapturedRequest(http.MethodGet, "/a", 200)
	rb.Add(req)
	req.Headers["Content-Type"] = "changed"

	assert.Equal(t, "application/json", rb.GetAll()[0].Headers["Content-Type"])
}

func TestRequestRingBuffer_ConcurrentUse(t *testing.T) {
	t.Parallel()

	const (
		writers = 4
		readers = 4
		perG    = 200
	)

	rb := logger.NewRequestRingBuffer(16)

	var wg sync.WaitGroup

	for w := range writers {
		wg.Go(func() {
			for i := range perG {
				rb.Add(newCapturedRequest(http.MethodGet, "/w"+strconv.Itoa(w)+"-"+strconv.Itoa(i), 200+i%3))
			}
		})
	}

	for range readers {
		wg.Go(func() {
			ch := rb.Subscribe()
			defer rb.Unsubscribe(ch)

			for range perG {
				for _, r := range rb.GetAll() {
					r.Headers["x"] = "y"
				}

				for _, r := range rb.Recent(8, func(r *logger.CapturedRequest) bool { return r.Status == 200 }) {
					r.Headers["x"] = "y"
				}

				select {
				case r := <-ch:
					r.Headers["x"] = "y"
				default:
				}
			}
		})
	}

	wg.Wait()

	assert.Len(t, rb.GetAll(), 16)
}
