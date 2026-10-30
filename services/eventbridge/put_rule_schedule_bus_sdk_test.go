package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestPutRule_ScheduleOnCustomBus_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bus      string
		lenient  bool
		wantFail bool
	}{
		{name: "default bus implicit", bus: "", wantFail: false},
		{name: "default bus explicit", bus: "default", wantFail: false},
		{name: "custom bus rejected", bus: "custom", wantFail: true},
		{name: "custom bus lenient opt-in", bus: "custom", lenient: true, wantFail: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			b.SetSettings(eventbridge.Settings{AllowScheduleOnCustomBus: tt.lenient})
			client := newTestEventBridgeClient(t, eventbridge.NewHandler(b))

			_, err := client.CreateEventBus(t.Context(), &ebsdk.CreateEventBusInput{Name: aws.String("custom")})
			require.NoError(t, err)

			in := &ebsdk.PutRuleInput{Name: aws.String("r"), ScheduleExpression: aws.String("rate(5 minutes)")}
			if tt.bus != "" {
				in.EventBusName = aws.String(tt.bus)
			}

			_, err = client.PutRule(t.Context(), in)
			if !tt.wantFail {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
			assert.Equal(t, "ScheduleExpression is supported only on the default event bus.", apiErr.ErrorMessage())
		})
	}
}
