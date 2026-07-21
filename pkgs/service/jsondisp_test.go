package service_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// errDispatchTest is the sentinel returned by the error-path dispatch stub.
var errDispatchTest = errors.New("dispatch error")

func newEchoContext(
	t *testing.T,
	method, path string,
	header http.Header,
	body string,
) (*echo.Context, *httptest.ResponseRecorder) {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, strings.NewReader(body))

	if header != nil {
		req.Header = header
	}

	rec := httptest.NewRecorder()

	return e.NewContext(req, rec), rec
}

func noopHandleErr(_ context.Context, c *echo.Context, _ string, reqErr error) error {
	return c.String(http.StatusBadRequest, reqErr.Error())
}

func TestHandleTarget(t *testing.T) {
	t.Parallel()

	log := slog.Default()
	supportedOps := []string{"GetFoo", "PutFoo"}

	successDispatch := service.DispatchFunc(func(_ context.Context, _ string, _ []byte) ([]byte, error) {
		return []byte("{\"ok\":true}"), nil
	})

	errDispatch := service.DispatchFunc(func(_ context.Context, _ string, _ []byte) ([]byte, error) {
		return nil, errDispatchTest
	})

	type args struct {
		dispatch service.DispatchFunc
		header   http.Header
		method   string
		path     string
		body     string
	}
	type wants struct {
		bodyContain string
		contentType string
		status      int
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "GET / returns supported ops",
			args: args{
				method:   http.MethodGet,
				path:     "/",
				dispatch: successDispatch,
			},
			wants: wants{
				status:      http.StatusOK,
				bodyContain: "GetFoo",
			},
		},
		{
			name: "non-POST returns 405",
			args: args{
				method:   http.MethodPut,
				path:     "/",
				dispatch: successDispatch,
			},
			wants: wants{
				status: http.StatusMethodNotAllowed,
			},
		},
		{
			name: "missing X-Amz-Target returns 400",
			args: args{
				method:   http.MethodPost,
				path:     "/",
				dispatch: successDispatch,
			},
			wants: wants{
				status: http.StatusBadRequest,
			},
		},
		{
			name: "invalid X-Amz-Target returns 400",
			args: args{
				method:   http.MethodPost,
				path:     "/",
				header:   http.Header{"X-Amz-Target": []string{"NoDotsHere"}},
				dispatch: successDispatch,
			},
			wants: wants{
				status: http.StatusBadRequest,
			},
		},
		{
			name: "successful dispatch returns 200 with content-type",
			args: args{
				method:   http.MethodPost,
				path:     "/",
				header:   http.Header{"X-Amz-Target": []string{"TestService.GetFoo"}},
				body:     "{}",
				dispatch: successDispatch,
			},
			wants: wants{
				status:      http.StatusOK,
				bodyContain: "ok",
				contentType: "application/x-amz-json-1.1",
			},
		},
		{
			name: "dispatch error invokes handleErr",
			args: args{
				method:   http.MethodPost,
				path:     "/",
				header:   http.Header{"X-Amz-Target": []string{"TestService.GetFoo"}},
				body:     "{}",
				dispatch: errDispatch,
			},
			wants: wants{
				status:      http.StatusBadRequest,
				bodyContain: "dispatch error",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, rec := newEchoContext(t, tc.args.method, tc.args.path, tc.args.header, tc.args.body)

			err := service.HandleTarget(
				c, log,
				"TestService", "application/x-amz-json-1.1",
				supportedOps,
				tc.args.dispatch,
				noopHandleErr,
			)
			require.NoError(t, err)
			require.Equal(t, tc.wants.status, rec.Code)

			if tc.wants.bodyContain != "" {
				require.Contains(t, rec.Body.String(), tc.wants.bodyContain)
			}

			if tc.wants.contentType != "" {
				require.Contains(t, rec.Header().Get("Content-Type"), tc.wants.contentType)
			}
		})
	}
}

// errHandleJSONFn is the sentinel returned by the error-path fn stub.
var errHandleJSONFn = errors.New("fn error")

type handleJSONTestInput struct {
	Name string `json:"name"`
}

type handleJSONTestOutput struct {
	Greeting string `json:"greeting"`
}

func TestHandleJSON(t *testing.T) {
	t.Parallel()

	greetFn := func(_ context.Context, in *handleJSONTestInput) (*handleJSONTestOutput, error) {
		return &handleJSONTestOutput{Greeting: "hello " + in.Name}, nil
	}

	errFn := func(_ context.Context, _ *handleJSONTestInput) (*handleJSONTestOutput, error) {
		return nil, errHandleJSONFn
	}

	type args struct {
		fn   func(context.Context, *handleJSONTestInput) (*handleJSONTestOutput, error)
		body string
	}
	type wants struct {
		greet string
		err   bool
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "decodes body and calls fn",
			args: args{
				body: `{"name":"world"}`,
				fn:   greetFn,
			},
			wants: wants{
				greet: "hello world",
			},
		},
		{
			name: "empty body uses zero-value input",
			args: args{
				body: "",
				fn:   greetFn,
			},
			wants: wants{
				greet: "hello ",
			},
		},
		{
			name: "malformed JSON returns error",
			args: args{
				body: `{bad json}`,
				fn:   greetFn,
			},
			wants: wants{
				err: true,
			},
		},
		{
			name: "fn error is propagated",
			args: args{
				body: `{"name":"world"}`,
				fn:   errFn,
			},
			wants: wants{
				err: true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := service.HandleJSON(t.Context(), []byte(tc.args.body), tc.args.fn)
			if tc.wants.err {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			out, ok := result.(*handleJSONTestOutput)
			require.True(t, ok)
			require.Equal(t, tc.wants.greet, out.Greeting)
		})
	}
}

func TestWrapOp(t *testing.T) {
	t.Parallel()

	greetFn := func(_ context.Context, in *handleJSONTestInput) (*handleJSONTestOutput, error) {
		return &handleJSONTestOutput{Greeting: "hi " + in.Name}, nil
	}

	type args struct {
		body []byte
	}
	type wants struct {
		err      error
		greeting string
	}

	tests := []struct {
		name  string
		wants wants
		args  args
	}{
		{
			name: "successful wrap",
			args: args{
				body: []byte(`{"name":"there"}`),
			},
			wants: wants{
				greeting: "hi there",
				err:      nil,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			op := service.WrapOp(greetFn)

			result, err := op(t.Context(), tc.args.body)
			if tc.wants.err != nil {
				require.ErrorIs(t, err, tc.wants.err)

				return
			}

			require.NoError(t, err)

			out, ok := result.(*handleJSONTestOutput)
			require.True(t, ok)
			require.Equal(t, tc.wants.greeting, out.Greeting)
		})
	}
}
