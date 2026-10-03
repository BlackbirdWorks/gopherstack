package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUntagResource_MultipleKeys_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		untag   []string
		wantKey []string
	}{
		{name: "one key", untag: []string{"a"}, wantKey: []string{"b", "c", "d,e"}},
		{name: "two keys", untag: []string{"a", "b"}, wantKey: []string{"c", "d,e"}},
		{name: "key containing comma", untag: []string{"d,e", "c"}, wantKey: []string{"a", "b"}},
		{name: "unknown key ignored", untag: []string{"zzz", "a"}, wantKey: []string{"b", "c", "d,e"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newSESv2TestHandler(t)
			client := newSESv2SDKClient(t, h)
			ctx := t.Context()
			arn := "arn:aws:ses:us-east-1:000000000000:identity/multi@example.com"

			_, err := client.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
				EmailIdentity: aws.String("multi@example.com"),
				Tags: []sesv2types.Tag{
					{Key: aws.String("a"), Value: aws.String("1")},
					{Key: aws.String("b"), Value: aws.String("2")},
					{Key: aws.String("c"), Value: aws.String("3")},
					{Key: aws.String("d,e"), Value: aws.String("4")},
				},
			})
			require.NoError(t, err)

			_, err = client.UntagResource(
				ctx,
				&sesv2sdk.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: tt.untag},
			)
			require.NoError(t, err)

			out, err := client.ListTagsForResource(
				ctx,
				&sesv2sdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
			)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Tags))
			for _, tag := range out.Tags {
				got = append(got, aws.ToString(tag.Key))
			}

			assert.ElementsMatch(t, tt.wantKey, got)
		})
	}
}
