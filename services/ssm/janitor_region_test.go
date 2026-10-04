package ssm_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

type regionRecordingNotifier struct {
	regions []string
	mu      sync.Mutex
}

func (n *regionRecordingNotifier) NotifyParameterPolicyAction(ctx context.Context, _, _ string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.regions = append(n.regions, awsmeta.Region(ctx))

	return nil
}

func TestSSMJanitor_PolicyNotificationCarriesParameterRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home", region: "us-east-1"},
		{name: "other", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			n := &regionRecordingNotifier{}
			b.SetParameterPolicyNotifier(n)

			_, err := b.PutParameter(ssm.WithRegion(t.Context(), tc.region), &ssm.PutParameterInput{
				Name:  "/app/p",
				Type:  "String",
				Value: "v",
				Tier:  "Advanced",
				Policies: `[{"Type":"NoChangeNotification","Version":"1.0",` +
					`"Attributes":{"After":"0","Unit":"Hours"}}]`,
			})
			require.NoError(t, err)

			ssm.NewJanitor(b, time.Minute).SweepOnce(t.Context())

			n.mu.Lock()
			defer n.mu.Unlock()

			assert.Equal(t, []string{tc.region}, n.regions)
		})
	}
}
