package service_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"Gopherstack/pkgs/service"
)

type testHandler struct{}

func (t testHandler) ServeHTTP(_ http.ResponseWriter, _ *http.Request) {}

func (t testHandler) GetSupportedOperations() []string {
	return []string{"op"}
}

func TestRegistrationTypes(t *testing.T) {
	t.Parallel()

	r := service.Registration{
		Handler: testHandler{},
		Match: func(req *http.Request) bool {
			return req.Method == http.MethodGet
		},
		Name: "test",
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if !r.Match(req) {
		t.Fatalf("expected matcher to return true")
	}

	if r.Name != "test" {
		t.Fatalf("expected name test, got %s", r.Name)
	}

	ops := r.Handler.GetSupportedOperations()
	if len(ops) != 1 || ops[0] != "op" {
		t.Fatalf("unexpected operations: %v", ops)
	}
}
