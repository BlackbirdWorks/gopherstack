package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestSession_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create  map[string]string
		tag     map[string]string
		want    map[string]string
		name    string
		untag   []string
		restore bool
	}{
		{name: "none"},
		{name: "create_tags", create: map[string]string{"a": "1"}, want: map[string]string{"a": "1"}},
		{
			name: "tag_and_untag", create: map[string]string{"a": "1"}, tag: map[string]string{"b": "2"},
			untag: []string{"a"}, want: map[string]string{"b": "2"},
		},
		{
			name:    "survives_restore",
			create:  map[string]string{"a": "1"},
			want:    map[string]string{"a": "1"},
			restore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestGlueClient(t, glue.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateSession(ctx, &gluesdk.CreateSessionInput{
				Id: aws.String("sess"), Role: aws.String("r"), Tags: tt.create,
				Command: &types.SessionCommand{Name: aws.String("glueetl")},
			})
			require.NoError(t, err)

			arn := aws.String("arn:aws:glue:us-east-1:123456789012:session/sess")

			if tt.tag != nil {
				_, err = client.TagResource(ctx, &gluesdk.TagResourceInput{ResourceArn: arn, TagsToAdd: tt.tag})
				require.NoError(t, err)
			}

			if tt.untag != nil {
				_, err = client.UntagResource(
					ctx,
					&gluesdk.UntagResourceInput{ResourceArn: arn, TagsToRemove: tt.untag},
				)
				require.NoError(t, err)
			}

			if tt.restore {
				restored := glue.NewInMemoryBackend("123456789012", "us-east-1")
				require.NoError(t, restored.Restore(ctx, backend.Snapshot(ctx)))
				client = newTestGlueClient(t, glue.NewHandler(restored))
			}

			got, err := client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: arn})
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Tags)
		})
	}
}
