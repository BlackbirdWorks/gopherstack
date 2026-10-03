package lambda_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestConfigReadConcurrentWithUpdate proves Get/Create/List config accessors
// must not hand back a live pointer that the matching Update mutates in place.
func TestConfigReadConcurrentWithUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T) (reader func(), mutator func(i int))
		name  string
	}{
		{
			name: "FunctionURLConfig races UpdateFunctionURLConfig",
			setup: func(t *testing.T) (func(), func(int)) {
				t.Helper()

				h, bk := newInMemoryHandler(t)
				fnName := "url-race-fn"
				createFunctionForTest(t, h, fnName)

				_, err := bk.CreateFunctionURLConfig(t.Context(), fnName, "NONE", nil, "BUFFERED")
				require.NoError(t, err)

				reader := func() {
					cfg, getErr := bk.GetFunctionURLConfig(fnName)
					if getErr != nil {
						return
					}

					_ = cfg.AuthType
					_ = cfg.InvokeMode
					_ = cfg.LastModifiedTime
				}

				mutator := func(i int) {
					authType := "NONE"
					if i%2 == 0 {
						authType = "AWS_IAM"
					}

					_, _ = bk.UpdateFunctionURLConfig(fnName, authType, nil, "BUFFERED")
				}

				return reader, mutator
			},
		},
		{
			name: "CapacityProvider races UpdateCapacityProvider",
			setup: func(t *testing.T) (func(), func(int)) {
				t.Helper()

				bk := newCapacityProviderTestBackend(t)

				_, err := bk.CreateCapacityProvider(&lambda.CreateCapacityProviderInput{
					CapacityProviderName: "race-cp",
					PermissionsConfig: &lambda.CapacityProviderPermissionsConfig{
						CapacityProviderOperatorRoleArn: "arn:aws:iam::000000000000:role/cp-role",
					},
					VpcConfig: &lambda.CapacityProviderVpcConfig{
						SubnetIDs: []string{"subnet-1"},
					},
				})
				require.NoError(t, err)

				reader := func() {
					cp, getErr := bk.GetCapacityProvider("race-cp")
					if getErr != nil {
						return
					}

					_ = cp.LastModified
					_ = cp.State
				}

				mutator := func(int) {
					_, _ = bk.UpdateCapacityProvider("race-cp", &lambda.UpdateCapacityProviderInput{
						PropagateTags: &lambda.PropagateTags{Mode: "TASK_DEFINITION"},
					})
				}

				return reader, mutator
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reader, mutator := tt.setup(t)

			const iterations = 500

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					reader()
				}
			}()

			go func() {
				defer wg.Done()

				for i := range iterations {
					mutator(i)
				}
			}()

			wg.Wait()
		})
	}
}
