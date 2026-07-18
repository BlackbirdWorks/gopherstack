package cloudformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResourceCreator_SSM_MaintenanceWindow_CreateDelete verifies the maintenance
// window is created in the SSM backend and removed on delete.
func TestResourceCreator_SSM_MaintenanceWindow_CreateDelete(t *testing.T) {
	t.Parallel()

	backends := newExtraServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"Name":     "cfn-test-mw",
		"Schedule": "cron(0 2 ? * SUN *)",
		"Duration": float64(4),
		"Cutoff":   float64(1),
	}

	physID, err := rc.Create(t.Context(), "MyMW", "AWS::SSM::MaintenanceWindow", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)
	assert.Contains(t, physID, "mw-")

	err = rc.Delete(t.Context(), "AWS::SSM::MaintenanceWindow", physID, nil)
	require.NoError(t, err)
}
