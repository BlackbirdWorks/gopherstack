package ec2_test

// uox6-deletetags: DeleteTags declares Tags as optional -- "If you omit this
// parameter, we delete all user-defined tags for the specified resources"
// (ec2@v1.319.1 api_op_DeleteTags.go) -- but the handler treated an omitted
// Tags list as "delete nothing" instead, a silent no-op on a call that real
// AWS turns into a full tag wipe.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteTags_OmittedTagsDeletesAll(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	require.NoError(t, b.CreateTags([]string{vpc.ID}, map[string]string{
		"Name": "uox6-vpc",
		"Team": "platform",
	}))

	_, err = client.DeleteTags(t.Context(), &ec2sdk.DeleteTagsInput{
		Resources: []string{vpc.ID},
	})
	require.NoError(t, err)

	out, err := client.DescribeTags(t.Context(), &ec2sdk.DescribeTagsInput{
		Filters: []types.Filter{{Name: aws.String("resource-id"), Values: []string{vpc.ID}}},
	})
	require.NoError(t, err)
	assert.Empty(t, out.Tags)
}

func TestDeleteTags_ExplicitKeyDeletesOnlyThatKey(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)

	require.NoError(t, b.CreateTags([]string{vpc.ID}, map[string]string{
		"Name": "uox6-vpc",
		"Team": "platform",
	}))

	_, err = client.DeleteTags(t.Context(), &ec2sdk.DeleteTagsInput{
		Resources: []string{vpc.ID},
		Tags:      []types.Tag{{Key: aws.String("Name")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeTags(t.Context(), &ec2sdk.DescribeTagsInput{
		Filters: []types.Filter{{Name: aws.String("resource-id"), Values: []string{vpc.ID}}},
	})
	require.NoError(t, err)
	require.Len(t, out.Tags, 1)
	assert.Equal(t, "Team", aws.ToString(out.Tags[0].Key))
}
