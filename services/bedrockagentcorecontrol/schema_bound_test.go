package bedrockagentcorecontrol_test

import (
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type boundedRefs struct {
	*fakeInvoker
	asked      *atomic.Int64
	plainReads *atomic.Int32
	tooLarge   bool
}

func (r boundedRefs) ReadObject(bucket, key string) ([]byte, bool) {
	r.plainReads.Add(1)

	return r.fakeInvoker.ReadObject(bucket, key)
}

func (r boundedRefs) ReadObjectUpTo(_, _ string, limit int64) ([]byte, bool, bool) {
	r.asked.Store(limit)

	if r.tooLarge {
		return nil, true, true
	}

	return []byte(`[{"name":"t","description":"d","inputSchema":{"type":"object"}}]`), true, false
}

func TestS3SchemaReadIsBoundedWhileStreaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tooLarge  bool
		wantTools int
	}{
		{name: "within cap", wantTools: 1},
		{name: "over cap", tooLarge: true, wantTools: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newMCPFixture(t)
			base := fakeRefs{functions: map[string]bool{lambdaARN: true}}
			refs := boundedRefs{
				fakeInvoker: &fakeInvoker{fakeRefs: base, calls: m.calls},
				asked:       &atomic.Int64{}, plainReads: &atomic.Int32{}, tooLarge: tt.tooLarge,
			}
			m.backend.SetReferences(refs)

			gw := createGateway(t, m.client, "Bound-Gw")
			gid := aws.ToString(gw.GatewayId)

			cfg := &types.TargetConfigurationMemberMcp{
				Value: &types.McpTargetConfigurationMemberLambda{Value: types.McpLambdaTargetConfiguration{
					LambdaArn: aws.String(lambdaARN),
					ToolSchema: &types.ToolSchemaMemberS3{
						Value: types.S3Configuration{Uri: aws.String("s3://bkt/schemas/tools.json")},
					},
				}},
			}

			if tt.tooLarge {
				_, err := m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
					GatewayIdentifier: aws.String(gid), Name: aws.String("big"), TargetConfiguration: cfg,
					CredentialProviderConfigurations: []types.CredentialProviderConfiguration{
						{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
					},
				})
				require.Error(t, err)
				assert.Equal(t, "ValidationException", apiCode(err))
			} else {
				m.createTarget(t, gid, "big", cfg)

				cs, err := connectMCP(t, m.mcpURL(gid), m.token(t, nil))
				require.NoError(t, err)

				t.Cleanup(func() { _ = cs.Close() })

				list, err := cs.ListTools(t.Context(), nil)
				require.NoError(t, err)
				assert.Len(t, list.Tools, tt.wantTools)
			}

			assert.Equal(t, int64(10<<20), refs.asked.Load())
			assert.Zero(t, refs.plainReads.Load(), "the unbounded reader is not used")
		})
	}
}
