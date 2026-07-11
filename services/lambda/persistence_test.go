package lambda_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func newLambdaBackend(t *testing.T) *lambda.InMemoryBackend {
	t.Helper()

	return closeBackend(t, lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1"))
}

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *lambda.InMemoryBackend) string
		verify func(t *testing.T, b *lambda.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *lambda.InMemoryBackend) string {
				fn := &lambda.FunctionConfiguration{
					FunctionName: "test-fn",
					Runtime:      "python3.9",
					Role:         "arn:aws:iam::000000000000:role/test",
					Handler:      "index.handler",
				}
				err := b.CreateFunction(fn)
				if err != nil {
					return ""
				}

				return fn.FunctionName
			},
			verify: func(t *testing.T, b *lambda.InMemoryBackend, id string) {
				t.Helper()

				fn, err := b.GetFunction(id)
				require.NoError(t, err)
				assert.Equal(t, id, fn.FunctionName)
				assert.Equal(t, "python3.9", fn.Runtime)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *lambda.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *lambda.InMemoryBackend, _ string) {
				t.Helper()

				fns := b.ListFunctions("", 0)
				assert.Empty(t, fns.Data)
			},
		},
		{
			// Bug fix (parity-sweep-3): the backendSnapshot omitted the
			// permissions map entirely, so every AddPermission call was
			// silently discarded across a persistence Restore even though
			// persistence was enabled for the service — a no-stub-rule
			// violation (state must persist when persistence is on).
			name: "permissions_survive_restore",
			setup: func(b *lambda.InMemoryBackend) string {
				fn := &lambda.FunctionConfiguration{
					FunctionName: "perm-persist-fn",
					Runtime:      "python3.9",
					Role:         "arn:aws:iam::000000000000:role/test",
					Handler:      "index.handler",
				}
				require.NoError(t, b.CreateFunction(fn))

				_, err := b.AddPermission("perm-persist-fn", "", &lambda.AddPermissionInput{
					StatementID: "AllowS3",
					Action:      "lambda:InvokeFunction",
					Principal:   "s3.amazonaws.com",
				})
				require.NoError(t, err)

				return fn.FunctionName
			},
			verify: func(t *testing.T, b *lambda.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.GetPolicy(id, "")
				require.NoError(t, err)
				require.NotNil(t, out.Policy)
				assert.Contains(t, *out.Policy, "AllowS3")
			},
		},
		{
			// Bug fix (parity-sweep-3): versionIndex is a derived lookup
			// built incrementally by PublishVersion and was never rebuilt on
			// Restore, so every published (non-$LATEST) version became
			// unreachable by qualifier after a persistence reload even
			// though the version data itself survived in b.versions.
			name: "published_version_reachable_after_restore",
			setup: func(b *lambda.InMemoryBackend) string {
				fn := &lambda.FunctionConfiguration{
					FunctionName: "ver-persist-fn",
					Runtime:      "python3.9",
					Role:         "arn:aws:iam::000000000000:role/test",
					Handler:      "index.handler",
				}
				require.NoError(t, b.CreateFunction(fn))

				_, err := b.PublishVersion("ver-persist-fn", "")
				require.NoError(t, err)

				return fn.FunctionName
			},
			verify: func(t *testing.T, b *lambda.InMemoryBackend, id string) {
				t.Helper()

				v, err := b.GetFunctionByQualifier(id, "1")
				require.NoError(t, err, "published version 1 must resolve after restore")
				assert.Equal(t, "1", v.Version)
			},
		},
		{
			// Bug fix (parity-sweep-3): esmByFunctionARN is a derived reverse
			// index over eventSourceMappings and was likewise never rebuilt
			// on Restore, so ListEventSourceMappings filtered by FunctionName
			// silently returned an empty page after a persistence reload.
			name: "esm_function_name_filter_survives_restore",
			setup: func(b *lambda.InMemoryBackend) string {
				fn := &lambda.FunctionConfiguration{
					FunctionName: "esm-idx-persist-fn",
					Runtime:      "python3.9",
					Role:         "arn:aws:iam::000000000000:role/test",
					Handler:      "index.handler",
				}
				require.NoError(t, b.CreateFunction(fn))

				_, err := b.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
					EventSourceARN: "arn:aws:sqs:us-east-1:000000000000:test-queue",
					FunctionName:   "esm-idx-persist-fn",
					Enabled:        true,
				})
				require.NoError(t, err)

				return fn.FunctionName
			},
			verify: func(t *testing.T, b *lambda.InMemoryBackend, id string) {
				t.Helper()

				page := b.ListEventSourceMappings(id, "", "", 0)
				assert.Len(t, page.Data, 1)
			},
		},
		{
			// Phase 3.3 (pkgs/store conversion): exercises every store.Table
			// registered on b.registry (functions, functionURLConfigs,
			// eventSourceMappings, eventInvokeConfigs, aliases, permissions)
			// plus every raw-but-still-persisted field (versions,
			// versionCounters, layers, layerVersionCounters, layerPolicies,
			// functionConcurrencies) in one round trip, so a regression that
			// silently drops any of them from Snapshot/Restore fails this test.
			name: "full_backend_state_round_trip",
			setup: func(b *lambda.InMemoryBackend) string {
				const fnName = "full-state-fn"

				fn := &lambda.FunctionConfiguration{
					FunctionName: fnName,
					Runtime:      "python3.9",
					Role:         "arn:aws:iam::000000000000:role/test",
					Handler:      "index.handler",
				}
				require.NoError(t, b.CreateFunction(fn))

				// functionURLConfigs (store.Table)
				_, err := b.CreateFunctionURLConfig(t.Context(), fnName, "NONE", nil, "")
				require.NoError(t, err)

				// eventInvokeConfigs (store.Table)
				maxRetry := 1
				_, err = b.PutFunctionEventInvokeConfig(fnName, &lambda.PutFunctionEventInvokeConfigInput{
					MaximumRetryAttempts: &maxRetry,
				})
				require.NoError(t, err)

				// aliases (store.Table, composite key)
				_, err = b.CreateAlias(fnName, &lambda.CreateAliasInput{
					Name:            "prod",
					FunctionVersion: "$LATEST",
				})
				require.NoError(t, err)

				// permissions (store.Table, composite key)
				_, err = b.AddPermission(fnName, "", &lambda.AddPermissionInput{
					StatementID: "AllowInvoke",
					Action:      "lambda:InvokeFunction",
					Principal:   "s3.amazonaws.com",
				})
				require.NoError(t, err)

				// versions / versionCounters (raw, persisted)
				_, err = b.PublishVersion(fnName, "v1")
				require.NoError(t, err)

				// functionConcurrencies (raw, persisted)
				_, err = b.PutFunctionConcurrency(fnName, 5)
				require.NoError(t, err)

				// layers / layerVersionCounters (raw, persisted)
				_, err = b.PublishLayerVersion(&lambda.PublishLayerVersionInput{
					LayerName: "full-state-layer",
					Content:   &lambda.LayerVersionContentInput{ZipFile: []byte("zip")},
				})
				require.NoError(t, err)

				// layerPolicies (raw, persisted)
				_, err = b.AddLayerVersionPermission("full-state-layer", 1, &lambda.AddLayerVersionPermissionInput{
					StatementID: "LayerAllow",
					Action:      "lambda:GetLayerVersion",
					Principal:   "*",
				})
				require.NoError(t, err)

				return fnName
			},
			verify: func(t *testing.T, b *lambda.InMemoryBackend, id string) {
				t.Helper()

				_, err := b.GetFunctionURLConfig(id)
				require.NoError(t, err, "functionURLConfigs must survive restore")

				eic, err := b.GetFunctionEventInvokeConfig(id)
				if assert.NoError(t, err, "eventInvokeConfigs must survive restore") {
					require.NotNil(t, eic.MaximumRetryAttempts)
					assert.Equal(t, 1, *eic.MaximumRetryAttempts)
				}

				_, err = b.GetAlias(id, "prod")
				require.NoError(t, err, "aliases must survive restore")

				policy, err := b.GetPolicy(id, "")
				if assert.NoError(t, err, "permissions must survive restore") {
					assert.Contains(t, *policy.Policy, "AllowInvoke")
				}

				concurrency, err := b.GetFunctionConcurrency(id)
				if assert.NoError(t, err, "functionConcurrencies must survive restore") {
					assert.Equal(t, 5, concurrency.ReservedConcurrentExecutions)
				}

				versions, err := b.ListVersionsByFunction(id, "", 0)
				if assert.NoError(t, err, "versions/versionCounters must survive restore") {
					var found bool
					for _, v := range versions.Data {
						if v.Version == "1" {
							found = true
						}
					}
					assert.True(t, found, "published version 1 must survive restore")
				}

				policyOut, err := b.GetLayerVersionPolicy("full-state-layer", 1)
				if assert.NoError(t, err, "layerPolicies must survive restore") {
					assert.Contains(t, policyOut.Policy, "LayerAllow")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := newLambdaBackend(t)
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := newLambdaBackend(t)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := newLambdaBackend(t)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}
