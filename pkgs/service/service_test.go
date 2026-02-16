package service_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"Gopherstack/pkgs/service"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

type testHandler struct{}

func (t testHandler) ServeHTTP(_ http.ResponseWriter, _ *http.Request) {}

func (t testHandler) GetSupportedOperations() []string {
	return []string{"op"}
}

func TestRegistrationTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reg         service.Registration
		name        string
		req         *http.Request
		wantRegName string
		wantOps     []string
		wantMatch   bool
	}{
		{
			reg: service.Registration{
				Match: func(req *http.Request) bool {
					return req.Method == http.MethodGet
				},
				Handler: testHandler{},
				Name:    "test",
			},
			req:         httptest.NewRequest(http.MethodGet, "/", nil),
			wantOps:     []string{"op"},
			name:        "basic registration",
			wantRegName: "test",
			wantMatch:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantRegName, tt.reg.Name)
			assert.Equal(t, tt.wantMatch, tt.reg.Match(tt.req))

			ops := tt.reg.Handler.GetSupportedOperations()
			if diff := cmp.Diff(tt.wantOps, ops); diff != "" {
				t.Errorf("GetSupportedOperations() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
