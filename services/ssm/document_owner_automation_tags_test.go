package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestListDocuments_OwnerFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		owner string
		want  []string
	}{
		{name: "amazon", owner: "Amazon", want: []string{"AWS-RunPowerShellScript", "AWS-RunShellScript"}},
		{name: "self", owner: "Self", want: []string{"my-doc"}},
		{name: "third_party", owner: "ThirdParty", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			_, err := c.CreateDocument(t.Context(), &ssmsdk.CreateDocumentInput{
				Name:    aws.String("my-doc"),
				Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
			})
			require.NoError(t, err)

			out, err := c.ListDocuments(t.Context(), &ssmsdk.ListDocumentsInput{
				Filters: []ssmtypes.DocumentKeyValuesFilter{{Key: aws.String("Owner"), Values: []string{tt.owner}}},
			})
			require.NoError(t, err)

			got := make([]string, 0)
			for _, d := range out.DocumentIdentifiers {
				got = append(got, aws.ToString(d.Name))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDocumentOwnerOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		doc  string
		want string
	}{
		{name: "builtin", doc: "AWS-RunShellScript", want: "Amazon"},
		{name: "custom", doc: "my-doc", want: "123456789012"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			_, err := c.CreateDocument(t.Context(), &ssmsdk.CreateDocumentInput{
				Name:    aws.String("my-doc"),
				Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
			})
			require.NoError(t, err)

			desc, err := c.DescribeDocument(t.Context(), &ssmsdk.DescribeDocumentInput{Name: aws.String(tt.doc)})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(desc.Document.Owner))

			list, err := c.ListDocuments(t.Context(), &ssmsdk.ListDocumentsInput{
				Filters: []ssmtypes.DocumentKeyValuesFilter{{Key: aws.String("Name"), Values: []string{tt.doc}}},
			})
			require.NoError(t, err)
			require.Len(t, list.DocumentIdentifiers, 1)
			assert.Equal(t, tt.want, aws.ToString(list.DocumentIdentifiers[0].Owner))
		})
	}
}

func TestStartAutomationExecution_AppliesTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{name: "two_tags", tags: map[string]string{"env": "dev", "team": "a"}},
		{name: "no_tags", tags: map[string]string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))

			in := &ssmsdk.StartAutomationExecutionInput{DocumentName: aws.String("AWS-RunShellScript")}
			for k, v := range tt.tags {
				in.Tags = append(in.Tags, ssmtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
			}

			started, err := c.StartAutomationExecution(t.Context(), in)
			require.NoError(t, err)

			out, err := c.ListTagsForResource(t.Context(), &ssmsdk.ListTagsForResourceInput{
				ResourceType: ssmtypes.ResourceTypeForTaggingAutomation,
				ResourceId:   started.AutomationExecutionId,
			})
			require.NoError(t, err)

			got := make(map[string]string, len(out.TagList))
			for _, tg := range out.TagList {
				got[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}

			assert.Equal(t, tt.tags, got)
		})
	}
}
