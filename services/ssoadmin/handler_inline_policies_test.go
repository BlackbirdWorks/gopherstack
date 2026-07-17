package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInlinePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		inlinePolicy string
		wantPolicy   string
	}{
		{
			name:         "put, get, and delete inline policy",
			inlinePolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			wantPolicy:   `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			putRec := doRequest(t, h, "PutInlinePolicyToPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"InlinePolicy":     tt.inlinePolicy,
			})
			require.Equal(t, http.StatusOK, putRec.Code)

			getRec := doRequest(t, h, "GetInlinePolicyForPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, getRec.Code)
			getResp := parseResponse(t, getRec)
			assert.Equal(t, tt.wantPolicy, getResp["InlinePolicy"])

			delRec := doRequest(t, h, "DeleteInlinePolicyFromPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, delRec.Code)

			getRec2 := doRequest(t, h, "GetInlinePolicyForPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, getRec2.Code)
			getResp2 := parseResponse(t, getRec2)
			assert.Empty(t, getResp2["InlinePolicy"])
		})
	}
}
