package mediaconvert_test

import (
	"testing"

	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	mediaconverttypes "github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// TestListOps_ListBy proves ListQueues/ListJobTemplates/ListPresets honor
// the ListBy request field (NAME vs CREATION_DATE), which real AWS
// documents on all three ListXInput shapes (aws-sdk-go-v2/service/
// mediaconvert@v1.97.1 api_op_ListQueues.go/api_op_ListJobTemplates.go/
// api_op_ListPresets.go: "you can choose to list them alphabetically by
// NAME or chronologically by CREATION_DATE"). Seeds resources with
// CreatedAt timestamps that invert their name order, so a CREATION_DATE
// request only passes if the handler actually re-sorts by CreatedAt
// instead of always returning its NAME-sorted default.
func TestListOps_ListBy(t *testing.T) {
	t.Parallel()

	t.Run("ListQueues", func(t *testing.T) {
		t.Parallel()

		b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
		b.AddQueueInternal(&mediaconvert.Queue{
			Name: "zulu", Arn: "arn:aws:mediaconvert:x:y:queues/zulu", CreatedAt: 100,
		})
		b.AddQueueInternal(&mediaconvert.Queue{
			Name: "alpha", Arn: "arn:aws:mediaconvert:x:y:queues/alpha", CreatedAt: 200,
		})
		b.AddQueueInternal(&mediaconvert.Queue{
			Name: "mike", Arn: "arn:aws:mediaconvert:x:y:queues/mike", CreatedAt: 300,
		})

		h := mediaconvert.NewHandler(b)
		client := newSDKTestClient(t, h)

		out, err := client.ListQueues(t.Context(), &mediaconvertsdk.ListQueuesInput{
			ListBy: mediaconverttypes.QueueListByCreationDate,
			Order:  mediaconverttypes.OrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.Queues, 3)
		assert.Equal(t, []string{"zulu", "alpha", "mike"}, queueNames(out.Queues))

		outByName, err := client.ListQueues(t.Context(), &mediaconvertsdk.ListQueuesInput{
			ListBy: mediaconverttypes.QueueListByName,
			Order:  mediaconverttypes.OrderAscending,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"alpha", "mike", "zulu"}, queueNames(outByName.Queues))
	})

	t.Run("ListJobTemplates", func(t *testing.T) {
		t.Parallel()

		b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
		b.AddJobTemplateInternal(&mediaconvert.JobTemplate{
			Name: "zulu", Arn: "arn:aws:mediaconvert:x:y:jobTemplates/zulu", CreatedAt: 100,
		})
		b.AddJobTemplateInternal(&mediaconvert.JobTemplate{
			Name: "alpha", Arn: "arn:aws:mediaconvert:x:y:jobTemplates/alpha", CreatedAt: 200,
		})
		b.AddJobTemplateInternal(&mediaconvert.JobTemplate{
			Name: "mike", Arn: "arn:aws:mediaconvert:x:y:jobTemplates/mike", CreatedAt: 300,
		})

		h := mediaconvert.NewHandler(b)
		client := newSDKTestClient(t, h)

		out, err := client.ListJobTemplates(t.Context(), &mediaconvertsdk.ListJobTemplatesInput{
			ListBy: mediaconverttypes.JobTemplateListByCreationDate,
			Order:  mediaconverttypes.OrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.JobTemplates, 3)
		assert.Equal(t, []string{"zulu", "alpha", "mike"}, jobTemplateNames(out.JobTemplates))

		outByName, err := client.ListJobTemplates(t.Context(), &mediaconvertsdk.ListJobTemplatesInput{
			ListBy: mediaconverttypes.JobTemplateListByName,
			Order:  mediaconverttypes.OrderAscending,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"alpha", "mike", "zulu"}, jobTemplateNames(outByName.JobTemplates))
	})

	t.Run("ListPresets", func(t *testing.T) {
		t.Parallel()

		b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
		b.AddPresetInternal(&mediaconvert.Preset{
			Name: "zulu", Arn: "arn:aws:mediaconvert:x:y:presets/zulu", CreatedAt: 100,
		})
		b.AddPresetInternal(&mediaconvert.Preset{
			Name: "alpha", Arn: "arn:aws:mediaconvert:x:y:presets/alpha", CreatedAt: 200,
		})
		b.AddPresetInternal(&mediaconvert.Preset{
			Name: "mike", Arn: "arn:aws:mediaconvert:x:y:presets/mike", CreatedAt: 300,
		})

		h := mediaconvert.NewHandler(b)
		client := newSDKTestClient(t, h)

		out, err := client.ListPresets(t.Context(), &mediaconvertsdk.ListPresetsInput{
			ListBy: mediaconverttypes.PresetListByCreationDate,
			Order:  mediaconverttypes.OrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.Presets, 3)
		assert.Equal(t, []string{"zulu", "alpha", "mike"}, presetNames(out.Presets))

		outByName, err := client.ListPresets(t.Context(), &mediaconvertsdk.ListPresetsInput{
			ListBy: mediaconverttypes.PresetListByName,
			Order:  mediaconverttypes.OrderAscending,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"alpha", "mike", "zulu"}, presetNames(outByName.Presets))
	})
}

func queueNames(qs []mediaconverttypes.Queue) []string {
	names := make([]string, len(qs))
	for i, q := range qs {
		names[i] = *q.Name
	}

	return names
}

func jobTemplateNames(jts []mediaconverttypes.JobTemplate) []string {
	names := make([]string, len(jts))
	for i, jt := range jts {
		names[i] = *jt.Name
	}

	return names
}

func presetNames(ps []mediaconverttypes.Preset) []string {
	names := make([]string, len(ps))
	for i, p := range ps {
		names[i] = *p.Name
	}

	return names
}
