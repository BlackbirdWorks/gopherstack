package restroute_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func newRouter() *restroute.Router {
	return restroute.New([]restroute.Route{
		{Method: http.MethodGet, Path: "/cells", Op: "ListCells"},
		{Method: http.MethodPost, Path: "/cells", Op: "CreateCell"},
		{Method: http.MethodGet, Path: "/cells/{CellName}", Op: "GetCell"},
		{Method: http.MethodGet, Path: "/checks/{Name}/resource/{Id}/status", Op: "ResourceStatus"},
	})
}

func TestRouterMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string]string
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "static", method: http.MethodGet, path: "/cells", wantOp: "ListCells"},
		{name: "method selects the op", method: http.MethodPost, path: "/cells", wantOp: "CreateCell"},
		{
			name:   "label",
			method: http.MethodGet,
			path:   "/cells/c1",
			wantOp: "GetCell",
			params: map[string]string{"CellName": "c1"},
		},
		{
			name:   "encoded slash stays one label",
			method: http.MethodGet,
			path:   "/cells/arn%3Aaws%3Ax%2Fy",
			wantOp: "GetCell",
			params: map[string]string{"CellName": "arn:aws:x/y"},
		},
		{
			name:   "two labels",
			method: http.MethodGet,
			path:   "/checks/rc/resource/arn%3Ax%2Fy/status",
			wantOp: "ResourceStatus",
			params: map[string]string{"Name": "rc", "Id": "arn:x/y"},
		},
		{name: "trailing slash", method: http.MethodGet, path: "/cells/", wantOp: "ListCells"},
		{name: "wrong method", method: http.MethodDelete, path: "/cells"},
		{name: "unknown path", method: http.MethodGet, path: "/nothing"},
		{name: "too deep", method: http.MethodGet, path: "/cells/a/b"},
		{name: "bad escape", method: http.MethodGet, path: "/cells/%zz"},
	}

	r := newRouter()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op, params := r.Match(tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)

			if tt.params != nil {
				assert.Equal(t, tt.params, params)
			}
		})
	}
}

func TestRouterHandlesIgnoresMethod(t *testing.T) {
	t.Parallel()

	r := newRouter()
	assert.True(t, r.Handles("/cells"))
	assert.True(t, r.Handles("/cells/x"))
	assert.False(t, r.Handles("/cells/x/y"))
	assert.False(t, r.Handles("/other"))
}

func TestPaginate(t *testing.T) {
	t.Parallel()

	all := []int{1, 2, 3, 4, 5}

	tests := []struct {
		name    string
		token   string
		want    []int
		max     int
		wantErr bool
		hasNext bool
	}{
		{name: "default page", want: all},
		{name: "first page", max: 2, want: []int{1, 2}, hasNext: true},
		{name: "max above limit", max: 2000, wantErr: true},
		{name: "negative max", max: -1, wantErr: true},
		{name: "garbage token", token: "!!", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pg, err := restroute.Paginate(all, tt.token, tt.max, 100, 1000)
			if tt.wantErr {
				var re *restroute.Error

				require.ErrorAs(t, err, &re)
				assert.Equal(t, "ValidationException", re.Code)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, pg.Data)
			assert.Equal(t, tt.hasNext, pg.Next != "")
		})
	}
}

func TestServerDispatchAndErrors(t *testing.T) {
	t.Parallel()

	s := &restroute.Server{
		Router: newRouter(),
		Ops: map[string]restroute.OpFunc{
			"GetCell": func(_ context.Context, req *restroute.Request) (any, int, error) {
				if req.Params["CellName"] == "missing" {
					return nil, 0, restroute.NotFound("Cell %s was not found", "missing")
				}

				return map[string]string{"name": req.Params["CellName"]}, 0, nil
			},
			"CreateCell": func(_ context.Context, req *restroute.Request) (any, int, error) {
				var in struct{ Name string }
				if err := req.Decode(&in); err != nil {
					return nil, 0, err
				}

				return nil, http.StatusCreated, nil
			},
		},
	}

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantCode   string
		wantStatus int
	}{
		{name: "ok", method: http.MethodGet, path: "/cells/c1", wantStatus: http.StatusOK},
		{
			name:       "empty result is an object",
			method:     http.MethodPost,
			path:       "/cells",
			body:       `{"Name":"x"}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "not found",
			method:     http.MethodGet,
			path:       "/cells/missing",
			wantStatus: http.StatusNotFound,
			wantCode:   "ResourceNotFoundException",
		},
		{
			name:       "bad json",
			method:     http.MethodPost,
			path:       "/cells",
			body:       "{",
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationException",
		},
		{
			name:       "unrouted",
			method:     http.MethodGet,
			path:       "/cells",
			wantStatus: http.StatusNotFound,
			wantCode:   "UnknownOperationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			require.NoError(t, s.Handler()(echo.New().NewContext(req, rec)))
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantCode, rec.Header().Get("X-Amzn-Errortype"))

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

			if tt.wantCode != "" {
				assert.NotEmpty(t, body["message"])
				assert.Equal(t, body["message"], body["Message"], "both message keys are present")
			}
		})
	}
}

func TestErrorMessageDropsRepeatedCode(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	c := echo.New().NewContext(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil), rec)
	require.NoError(
		t,
		restroute.WriteError(
			c,
			http.StatusConflict,
			"ConflictException",
			"ConflictException: cell is busy",
			map[string]any{"reason": "R"},
		),
	)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "cell is busy", body["message"])
	assert.Equal(t, "R", body["reason"])
}
