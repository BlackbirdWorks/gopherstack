package mediaconvert_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	mediaconverttypes "github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListOps_Pagination proves ListQueues/ListJobTemplates/ListPresets
// genuinely paginate via NextToken (real ListQueuesOutput/
// ListJobTemplatesOutput/ListPresetsOutput all carry one -- confirmed against
// aws-sdk-go-v2/service/mediaconvert@v1.97.1's api_op_ListQueues.go/
// api_op_ListJobTemplates.go/api_op_ListPresets.go, each also documenting
// MaxResults as "up to twenty ... at one time"). Before this fix, all three
// truncated to maxResults via limitSlice and never returned a NextToken --
// unlike ListJobs/SearchJobs/ListVersions/DescribeEndpoints, which already
// paginated for real -- so a real client with more than one page's worth of
// queues/templates/presets could never retrieve the remainder.
func TestListOps_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("ListQueues", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		client := newSDKTestClient(t, h)
		ctx := t.Context()

		const total = 25
		for i := range total {
			_, err := client.CreateQueue(ctx, &mediaconvertsdk.CreateQueueInput{
				Name: aws.String(queueName(i)),
			})
			require.NoError(t, err)
		}

		seen := map[string]bool{}

		out, err := client.ListQueues(ctx, &mediaconvertsdk.ListQueuesInput{})
		require.NoError(t, err)
		require.Len(t, out.Queues, 20)
		require.NotNil(t, out.NextToken)
		assert.NotEmpty(t, aws.ToString(out.NextToken))

		for _, q := range out.Queues {
			seen[aws.ToString(q.Name)] = true
		}

		out2, err := client.ListQueues(ctx, &mediaconvertsdk.ListQueuesInput{NextToken: out.NextToken})
		require.NoError(t, err)
		assert.Empty(t, aws.ToString(out2.NextToken))
		assert.Len(t, out2.Queues, total-20)

		for _, q := range out2.Queues {
			seen[aws.ToString(q.Name)] = true
		}

		assert.Len(t, seen, total)
	})

	t.Run("ListJobTemplates", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		client := newSDKTestClient(t, h)
		ctx := t.Context()

		const total = 25
		for i := range total {
			_, err := client.CreateJobTemplate(ctx, &mediaconvertsdk.CreateJobTemplateInput{
				Name:     aws.String(queueName(i)),
				Settings: &mediaconverttypes.JobTemplateSettings{},
			})
			require.NoError(t, err)
		}

		out, err := client.ListJobTemplates(ctx, &mediaconvertsdk.ListJobTemplatesInput{})
		require.NoError(t, err)
		require.Len(t, out.JobTemplates, 20)
		require.NotNil(t, out.NextToken)
		assert.NotEmpty(t, aws.ToString(out.NextToken))

		out2, err := client.ListJobTemplates(ctx, &mediaconvertsdk.ListJobTemplatesInput{NextToken: out.NextToken})
		require.NoError(t, err)
		assert.Len(t, out2.JobTemplates, total-20)
		assert.Empty(t, aws.ToString(out2.NextToken))
	})

	t.Run("ListPresets", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		client := newSDKTestClient(t, h)
		ctx := t.Context()

		const total = 25
		for i := range total {
			_, err := client.CreatePreset(ctx, &mediaconvertsdk.CreatePresetInput{
				Name:     aws.String(queueName(i)),
				Settings: &mediaconverttypes.PresetSettings{},
			})
			require.NoError(t, err)
		}

		out, err := client.ListPresets(ctx, &mediaconvertsdk.ListPresetsInput{})
		require.NoError(t, err)
		require.Len(t, out.Presets, 20)
		require.NotNil(t, out.NextToken)
		assert.NotEmpty(t, aws.ToString(out.NextToken))

		out2, err := client.ListPresets(ctx, &mediaconvertsdk.ListPresetsInput{NextToken: out.NextToken})
		require.NoError(t, err)
		assert.Len(t, out2.Presets, total-20)
		assert.Empty(t, aws.ToString(out2.NextToken))
	})
}

func queueName(i int) string {
	const base = "resource-"

	return base + string(rune('a'+i%26)) + string(rune('0'+i/26))
}
