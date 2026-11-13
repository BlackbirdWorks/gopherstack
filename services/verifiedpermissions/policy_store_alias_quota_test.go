package verifiedpermissions_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_PolicyStoreAliasQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		aliases   int
		wantQuota bool
	}{
		{name: "at limit", aliases: 10},
		{name: "over limit", aliases: 11, wantQuota: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			store, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
				ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
			})
			require.NoError(t, err)

			var lastErr error
			for i := range tt.aliases {
				_, lastErr = client.CreatePolicyStoreAlias(ctx, &avpsdk.CreatePolicyStoreAliasInput{
					AliasName:     aws.String(fmt.Sprintf("policy-store-alias/a%d", i)),
					PolicyStoreId: store.PolicyStoreId,
				})
			}

			if !tt.wantQuota {
				require.NoError(t, lastErr)

				return
			}

			var qe *types.ServiceQuotaExceededException
			require.ErrorAs(t, lastErr, &qe)
			assert.Equal(t, "ServiceQuotaExceededException", qe.ErrorCode())
		})
	}
}
