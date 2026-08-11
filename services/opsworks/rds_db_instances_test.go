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
				// Real OpsWorks Stacks never echoes the actual password back
				// (aws-sdk-go-v2/service/opsworks@v1.31.0's types.go
				// RdsDbInstance.DbPassword doc comment).
				assert.Equal(t, "*****FILTERED*****", rdb["DbPassword"])
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
					"DbPassword":       "hunter2",
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
					"DbPassword":       "hunter2",
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

// TestRegisterRdsDbInstanceValidation verifies RegisterRdsDbInstance
// rejects requests missing a required member. StackId, RdsDbInstanceArn,
// DbUser, and DbPassword are all "This member is required" on the real
// RegisterRdsDbInstanceInput (confirmed against
// aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_RegisterRdsDbInstance.go).
func TestRegisterRdsDbInstanceValidation(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"StackId":          "placeholder",
		"RdsDbInstanceArn": "arn:aws:rds:us-east-1:000000000000:db:mydb",
		"DbUser":           "admin",
		"DbPassword":       "secret",
	}

	tests := []struct {
		name    string
		missing string
	}{
		{name: "missing StackId", missing: "StackId"},
		{name: "missing RdsDbInstanceArn", missing: "RdsDbInstanceArn"},
		{name: "missing DbUser", missing: "DbUser"},
		{name: "missing DbPassword", missing: "DbPassword"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			stackID := createTestStack(t, h)

			body := make(map[string]any, len(full))
			for k, v := range full {
				if k != tt.missing {
					body[k] = v
				}
			}
			if tt.missing != "StackId" {
				body["StackId"] = stackID
			}

			rec := doTarget(t, h, "RegisterRdsDbInstance", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "ValidationException")
		})
	}
}
