package xray_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutResourcePolicy_RevisionSemantics(t *testing.T) {
	t.Parallel()

	const doc = `{"Version":"2012-10-17","Statement":[]}`

	cases := []struct {
		name         string
		secondRev    *string
		wantRevision string
		wantErr      bool
	}{
		{name: "omitted revision increments", wantRevision: "2"},
		{name: "matching revision increments", secondRev: aws.String("1"), wantRevision: "2"},
		{name: "zero revision conflicts when policy exists", secondRev: aws.String("0"), wantErr: true},
		{name: "stale revision conflicts", secondRev: aws.String("7"), wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestXRayClient(t)

			first, err := c.PutResourcePolicy(t.Context(), &xraysdk.PutResourcePolicyInput{
				PolicyName: aws.String("p"), PolicyDocument: aws.String(doc), PolicyRevisionId: aws.String("0"),
			})
			require.NoError(t, err)
			assert.Equal(t, "1", aws.ToString(first.ResourcePolicy.PolicyRevisionId))

			second, err := c.PutResourcePolicy(t.Context(), &xraysdk.PutResourcePolicyInput{
				PolicyName: aws.String("p"), PolicyDocument: aws.String(doc), PolicyRevisionId: tc.secondRev,
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantRevision, aws.ToString(second.ResourcePolicy.PolicyRevisionId))
		})
	}
}
