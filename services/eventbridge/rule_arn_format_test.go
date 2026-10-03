package eventbridge_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

const (
	ruleARNPrefix = "arn:aws:events:us-east-1:123456789012:rule/"
	rulePattern   = `{"source":["x"]}`
)

func TestRuleARN_Format(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bus     string
		wantARN string
	}{
		{name: "default bus omitted", bus: "", wantARN: ruleARNPrefix + "r1"},
		{name: "default bus explicit", bus: "default", wantARN: ruleARNPrefix + "r1"},
		{name: "custom bus includes bus", bus: "custom", wantARN: ruleARNPrefix + "custom/r1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))
			if tt.bus == "custom" {
				_, err := client.CreateEventBus(t.Context(), &ebsdk.CreateEventBusInput{Name: aws.String(tt.bus)})
				require.NoError(t, err)
			}

			put, err := client.PutRule(t.Context(), &ebsdk.PutRuleInput{
				Name: aws.String("r1"), EventPattern: aws.String(rulePattern), EventBusName: aws.String(tt.bus),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantARN, aws.ToString(put.RuleArn))

			desc, err := client.DescribeRule(t.Context(), &ebsdk.DescribeRuleInput{
				Name: aws.String("r1"), EventBusName: aws.String(tt.bus),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantARN, aws.ToString(desc.Arn))

			list, err := client.ListRules(t.Context(), &ebsdk.ListRulesInput{EventBusName: aws.String(tt.bus)})
			require.NoError(t, err)
			require.Len(t, list.Rules, 1)
			assert.Equal(t, tt.wantARN, aws.ToString(list.Rules[0].Arn))
		})
	}
}

func TestRuleARN_TagsAcceptBothForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagARN   string
		listARN  string
		wantTags int
	}{
		{name: "aws form both", tagARN: ruleARNPrefix + "r1", listARN: ruleARNPrefix + "r1", wantTags: 1},
		{name: "legacy tag aws list", tagARN: ruleARNPrefix + "default/r1", listARN: ruleARNPrefix + "r1", wantTags: 1},
		{name: "aws tag legacy list", tagARN: ruleARNPrefix + "r1", listARN: ruleARNPrefix + "default/r1", wantTags: 1},
		{name: "legacy both", tagARN: ruleARNPrefix + "default/r1", listARN: ruleARNPrefix + "default/r1", wantTags: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))
			_, err := client.PutRule(t.Context(), &ebsdk.PutRuleInput{
				Name: aws.String("r1"), EventPattern: aws.String(rulePattern),
			})
			require.NoError(t, err)

			_, err = client.TagResource(t.Context(), &ebsdk.TagResourceInput{
				ResourceARN: aws.String(tt.tagARN),
				Tags:        []ebtypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})
			require.NoError(t, err)

			got, err := client.ListTagsForResource(t.Context(), &ebsdk.ListTagsForResourceInput{
				ResourceARN: aws.String(tt.listARN),
			})
			require.NoError(t, err)
			assert.Len(t, got.Tags, tt.wantTags)

			_, err = client.UntagResource(t.Context(), &ebsdk.UntagResourceInput{
				ResourceARN: aws.String(tt.listARN), TagKeys: []string{"k"},
			})
			require.NoError(t, err)

			got, err = client.ListTagsForResource(t.Context(), &ebsdk.ListTagsForResourceInput{
				ResourceARN: aws.String(tt.tagARN),
			})
			require.NoError(t, err)
			assert.Empty(t, got.Tags)
		})
	}
}

func TestRuleARN_LegacySnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "legacy arns rewritten"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := eventbridge.NewHandler(newBackend())
			client := newTestEventBridgeClient(t, src)
			_, err := client.PutRule(t.Context(), &ebsdk.PutRuleInput{
				Name: aws.String("r1"), EventPattern: aws.String(rulePattern),
			})
			require.NoError(t, err)
			_, err = client.TagResource(t.Context(), &ebsdk.TagResourceInput{
				ResourceARN: aws.String(ruleARNPrefix + "r1"),
				Tags:        []ebtypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})
			require.NoError(t, err)

			legacy := legacySnapshot(t, src.Snapshot(t.Context()))

			dst := eventbridge.NewHandler(newBackend())
			require.NoError(t, dst.Restore(t.Context(), legacy))
			restored := newTestEventBridgeClient(t, dst)

			desc, err := restored.DescribeRule(t.Context(), &ebsdk.DescribeRuleInput{Name: aws.String("r1")})
			require.NoError(t, err)
			assert.Equal(t, ruleARNPrefix+"r1", aws.ToString(desc.Arn))

			for _, arn := range []string{ruleARNPrefix + "r1", ruleARNPrefix + "default/r1"} {
				got, listErr := restored.ListTagsForResource(t.Context(), &ebsdk.ListTagsForResourceInput{
					ResourceARN: aws.String(arn),
				})
				require.NoError(t, listErr)
				assert.Len(t, got.Tags, 1, arn)
			}
		})
	}
}

func legacySnapshot(t *testing.T, data []byte) []byte {
	t.Helper()

	var snap struct {
		Tags    map[string]map[string]string `json:"tags,omitempty"`
		Backend []byte                       `json:"backend"`
	}
	require.NoError(t, json.Unmarshal(data, &snap))

	modern := ruleARNPrefix + "r1"
	legacy := ruleARNPrefix + "default/r1"
	require.Contains(t, string(snap.Backend), modern)
	snap.Backend = []byte(strings.ReplaceAll(string(snap.Backend), modern, legacy))

	tags := make(map[string]map[string]string, len(snap.Tags))
	for k, v := range snap.Tags {
		tags[strings.ReplaceAll(k, modern, legacy)] = v
	}
	snap.Tags = tags

	out, err := json.Marshal(snap)
	require.NoError(t, err)

	return out
}
