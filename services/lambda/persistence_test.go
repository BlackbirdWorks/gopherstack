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
