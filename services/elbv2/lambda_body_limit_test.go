package elbv2_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataPlane_LambdaBodyLimit(t *testing.T) {
	t.Parallel()

	const limit = 1 << 20

	tests := []struct {
		name       string
		size       int
		wantStatus int
	}{
		{name: "at limit", size: limit, wantStatus: http.StatusOK},
		{name: "over limit", size: limit + 1, wantStatus: http.StatusRequestEntityTooLarge},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inv := &fakeInvoker{response: `{"statusCode":200,"body":"ok"}`}
			f, _ := lambdaFixture(t, false, inv)

			got := f.do(t, dpReq{
				method: http.MethodPost, path: "/",
				headers: map[string]string{"Content-Type": "text/plain"},
				body:    strings.Repeat("a", tc.size),
			})
			assert.Equal(t, tc.wantStatus, got.status)
		})
	}
}
