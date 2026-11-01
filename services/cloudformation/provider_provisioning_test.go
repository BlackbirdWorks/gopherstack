package cloudformation_test

import (
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

type delayConfig struct {
	mockConfigProvider

	delay time.Duration
}

func (c *delayConfig) GetCloudFormationProvisioningDelay() time.Duration { return c.delay }

func TestProvider_Init_ProvisioningDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		config     any
		wantStatus string
	}{
		{name: "delay_set", config: &delayConfig{delay: 300 * time.Millisecond}, wantStatus: "CREATE_IN_PROGRESS"},
		{name: "delay_zero", config: &delayConfig{}, wantStatus: "CREATE_COMPLETE"},
		{name: "no_delay_config", config: &mockConfigProvider{}, wantStatus: "CREATE_COMPLETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				appCtx := &service.AppContext{Logger: slog.Default(), Config: tt.config}
				svc, err := (&cloudformation.Provider{}).Init(appCtx)
				require.NoError(t, err)

				h, ok := svc.(*cloudformation.Handler)
				require.True(t, ok)

				b, ok := h.Backend.(*cloudformation.InMemoryBackend)
				require.True(t, ok)
				t.Cleanup(b.WaitForStackProvisioning)

				client := newTestClientForBackend(t, b)
				_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
					StackName: aws.String("prov-delay"), TemplateBody: aws.String(asyncTemplate),
				})
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, stackStatus(t, client, "prov-delay"))

				time.Sleep(time.Second)
				synctest.Wait()
				assert.Equal(t, "CREATE_COMPLETE", stackStatus(t, client, "prov-delay"))
			})
		})
	}
}
