package lambda_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateFunctionCode_DryRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		wantImage string
		wantCode  int
	}{
		{
			name:      "dry_run_keeps_code",
			body:      `{"ImageUri":"new:v2","DryRun":true}`,
			wantCode:  http.StatusOK,
			wantImage: "x",
		},
		{name: "real_update", body: `{"ImageUri":"new:v2"}`, wantCode: http.StatusOK, wantImage: "new:v2"},
		{name: "dry_run_still_validates", body: `{"DryRun":true}`, wantCode: http.StatusBadRequest, wantImage: "x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, "dry-run-fn")

			rec := callInMemoryHandler(t, h, http.MethodPut, "/2015-03-31/functions/dry-run-fn/code", tt.body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			fn, err := bk.GetFunction("dry-run-fn")
			require.NoError(t, err)
			assert.Equal(t, tt.wantImage, fn.ImageURI)
		})
	}
}
