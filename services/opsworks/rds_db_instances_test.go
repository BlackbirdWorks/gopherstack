package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestRdsDbInstances verifies RDS DB instance registration.
func TestRdsDbInstances(t *testing.T) {
	t.Parallel()

	const testArn = "arn:aws:rds:us-east-1:000000000000:db:mydb"

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "RegisterRdsDbInstance and DescribeRdsDbInstances",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterRdsDbInstance", map[string]any{
					"StackId":          stackID,
					"RdsDbInstanceArn": testArn,
					"DbUser":           "admin",
					"DbPassword":       "secret",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeRdsDbInstances", map[string]any{
					"StackId": stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				rdbs := parseJSON(t, rec.Body.Bytes())["RdsDbInstances"].([]any)
				require.Len(t, rdbs, 1)
				rdb := rdbs[0].(map[string]any)
				assert.Equal(t, testArn, rdb["RdsDbInstanceArn"])
				assert.Equal(t, "admin", rdb["DbUser"])
			},
		},
		{
			name: "UpdateRdsDbInstance changes DbUser",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				doTarget(t, h, "RegisterRdsDbInstance", map[string]any{
					"StackId":          stackID,
					"RdsDbInstanceArn": testArn,
					"DbUser":           "olduser",
				})
				rec := doTarget(t, h, "UpdateRdsDbInstance", map[string]any{
					"RdsDbInstanceArn": testArn,
					"DbUser":           "newuser",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeRdsDbInstances", map[string]any{
					"RdsDbInstanceArns": []string{testArn},
				})
				rdbs := parseJSON(t, rec.Body.Bytes())["RdsDbInstances"].([]any)
				assert.Equal(t, "newuser", rdbs[0].(map[string]any)["DbUser"])
			},
		},
		{
			name: "DeregisterRdsDbInstance removes instance",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				doTarget(t, h, "RegisterRdsDbInstance", map[string]any{
					"StackId":          stackID,
					"RdsDbInstanceArn": testArn,
					"DbUser":           "user",
				})
				rec := doTarget(t, h, "DeregisterRdsDbInstance", map[string]any{
					"RdsDbInstanceArn": testArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeRdsDbInstances", map[string]any{
					"RdsDbInstanceArns": []string{testArn},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}
