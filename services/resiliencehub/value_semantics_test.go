package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateApp_DescriptionExplicitEmptyClears(t *testing.T) {
	t.Parallel()

	cases := []struct {
		description *string
		name        string
		want        string
	}{
		{name: "omitted keeps", description: nil, want: "original"},
		{name: "explicit empty clears", description: aws.String(""), want: ""},
		{name: "new value replaces", description: aws.String("changed"), want: "changed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			created, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{
				Name: aws.String("app"), Description: aws.String("original"),
			})
			require.NoError(t, err)

			_, err = client.UpdateApp(ctx, &resiliencehubsdk.UpdateAppInput{
				AppArn: created.App.AppArn, Description: tc.description,
			})
			require.NoError(t, err)

			got, err := client.DescribeApp(ctx, &resiliencehubsdk.DescribeAppInput{AppArn: created.App.AppArn})
			require.NoError(t, err)
			assert.Equal(t, tc.want, aws.ToString(got.App.Description))
			assert.Equal(t, "app", aws.ToString(got.App.Name))
		})
	}
}
