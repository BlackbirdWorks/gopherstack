package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gsebe "github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestPutRule_Tags_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		bus  string
		tags []ebtypes.Tag
	}{
		{name: "default_bus", tags: []ebtypes.Tag{
			{Key: aws.String("env"), Value: aws.String("dev")},
			{Key: aws.String("team"), Value: aws.String("core")},
		}},
		{name: "custom_bus", bus: "tagged-bus", tags: []ebtypes.Tag{{Key: aws.String("a"), Value: aws.String("b")}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, gsebe.NewHandler(gsebe.NewInMemoryBackend()))
			in := &eventbridge.PutRuleInput{
				Name:         aws.String("tagged-rule"),
				EventPattern: aws.String(`{"source":["x"]}`),
				Tags:         tt.tags,
			}
			if tt.bus != "" {
				_, err := client.CreateEventBus(t.Context(), &eventbridge.CreateEventBusInput{Name: aws.String(tt.bus)})
				require.NoError(t, err)
				in.EventBusName = aws.String(tt.bus)
			}

			out, err := client.PutRule(t.Context(), in)
			require.NoError(t, err)

			got, err := client.ListTagsForResource(t.Context(), &eventbridge.ListTagsForResourceInput{
				ResourceARN: out.RuleArn,
			})
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.tags, got.Tags)
		})
	}
}
