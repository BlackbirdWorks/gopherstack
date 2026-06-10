package apprunner_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// TestAddOperation_CapsPerServiceHistory verifies that a service's operation
// history is bounded so repeatedly updating a long-lived service cannot grow
// svc.Operations without limit.
func TestAddOperation_CapsPerServiceHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		updates int
	}{
		{name: "just over cap", updates: apprunner.MaxOperationsPerService + 10},
		{name: "far over cap", updates: apprunner.MaxOperationsPerService * 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := apprunner.NewInMemoryBackend("123456789012", "us-east-1")

			svc, err := b.CreateService("svc", "", "", "public.ecr.aws/x/y:latest", nil)
			require.NoError(t, err)

			for range tc.updates {
				_, err = b.UpdateService(svc.ServiceArn, "2 vCPU", "", "")
				require.NoError(t, err)
			}

			length, capacity := apprunner.ServiceOperationStats(b, svc.ServiceArn)

			require.LessOrEqual(t, length, apprunner.MaxOperationsPerService,
				"per-service operation history must be capped")
			require.LessOrEqual(t, capacity, apprunner.MaxOperationsPerService,
				"trimmed operation slice must not retain an oversized backing array")
		})
	}
}
