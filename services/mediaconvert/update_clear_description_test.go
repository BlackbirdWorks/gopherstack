package mediaconvert_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_UpdateDescriptionSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc *string
		name string
		want string
	}{
		{name: "omitted keeps", desc: nil, want: "orig"},
		{name: "empty clears", desc: aws.String(""), want: ""},
		{name: "new value", desc: aws.String("next"), want: "next"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMediaConvertClient(t, newTestHandler(t))
			ctx := t.Context()

			_, err := client.CreateQueue(ctx, &mediaconvertsdk.CreateQueueInput{
				Name: aws.String("q"), Description: aws.String("orig"),
			})
			require.NoError(t, err)
			_, err = client.UpdateQueue(ctx, &mediaconvertsdk.UpdateQueueInput{
				Name: aws.String("q"), Description: tt.desc, Status: types.QueueStatusPaused,
			})
			require.NoError(t, err)
			q, err := client.GetQueue(ctx, &mediaconvertsdk.GetQueueInput{Name: aws.String("q")})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(q.Queue.Description))

			_, err = client.CreatePreset(ctx, &mediaconvertsdk.CreatePresetInput{
				Name: aws.String("p"), Description: aws.String("orig"), Settings: &types.PresetSettings{},
			})
			require.NoError(t, err)
			_, err = client.UpdatePreset(ctx, &mediaconvertsdk.UpdatePresetInput{
				Name: aws.String("p"), Description: tt.desc,
			})
			require.NoError(t, err)
			p, err := client.GetPreset(ctx, &mediaconvertsdk.GetPresetInput{Name: aws.String("p")})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(p.Preset.Description))

			_, err = client.CreateJobTemplate(ctx, &mediaconvertsdk.CreateJobTemplateInput{
				Name: aws.String("jt"), Description: aws.String("orig"), Settings: &types.JobTemplateSettings{},
			})
			require.NoError(t, err)
			_, err = client.UpdateJobTemplate(ctx, &mediaconvertsdk.UpdateJobTemplateInput{
				Name: aws.String("jt"), Description: tt.desc,
			})
			require.NoError(t, err)
			jt, err := client.GetJobTemplate(ctx, &mediaconvertsdk.GetJobTemplateInput{Name: aws.String("jt")})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(jt.JobTemplate.Description))
		})
	}
}
