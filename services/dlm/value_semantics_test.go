package dlm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dlmsdk "github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

func TestDefaultPolicy_DocumentedDefaultsAndPartialUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		create       *dlmsdk.CreateLifecyclePolicyInput
		update       *dlmsdk.UpdateLifecyclePolicyInput
		name         string
		wantCreate   int32
		wantRetain   int32
		wantCopyTags bool
	}{
		{
			name:       "omitted members get documented defaults",
			create:     &dlmsdk.CreateLifecyclePolicyInput{},
			update:     &dlmsdk.UpdateLifecyclePolicyInput{RetainInterval: aws.Int32(10)},
			wantCreate: 1, wantRetain: 10,
		},
		{
			name:         "explicit create keeps value on partial update",
			create:       &dlmsdk.CreateLifecyclePolicyInput{CreateInterval: aws.Int32(3), CopyTags: aws.Bool(true)},
			update:       &dlmsdk.UpdateLifecyclePolicyInput{ExtendDeletion: aws.Bool(true)},
			wantCreate:   3,
			wantRetain:   7,
			wantCopyTags: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestDLMClient(t, dlm.NewHandler(dlm.NewInMemoryBackend("000000000000", "us-east-1")))

			tc.create.Description = aws.String("d")
			tc.create.ExecutionRoleArn = aws.String("arn:aws:iam::000000000000:role/r")
			tc.create.State = dlmtypes.SettablePolicyStateValuesEnabled
			tc.create.DefaultPolicy = dlmtypes.DefaultPolicyTypeValuesVolume
			created, err := client.CreateLifecyclePolicy(ctx, tc.create)
			require.NoError(t, err)

			tc.update.PolicyId = created.PolicyId
			_, err = client.UpdateLifecyclePolicy(ctx, tc.update)
			require.NoError(t, err)

			got, err := client.GetLifecyclePolicy(ctx, &dlmsdk.GetLifecyclePolicyInput{PolicyId: created.PolicyId})
			require.NoError(t, err)

			pd := got.Policy.PolicyDetails
			require.NotNil(t, pd)
			assert.Equal(t, tc.wantCreate, aws.ToInt32(pd.CreateInterval))
			assert.Equal(t, tc.wantRetain, aws.ToInt32(pd.RetainInterval))
			assert.Equal(t, tc.wantCopyTags, aws.ToBool(pd.CopyTags))
			require.NotNil(t, pd.ExtendDeletion)
		})
	}
}
