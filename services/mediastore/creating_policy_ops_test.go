package mediastore_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediastoresdk "github.com/aws/aws-sdk-go-v2/service/mediastore"
	"github.com/aws/aws-sdk-go-v2/service/mediastore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

func TestPolicyOps_RejectedWhileCreating(t *testing.T) {
	t.Parallel()

	name := aws.String("slow")
	tests := []struct {
		call func(*mediastoresdk.Client) error
		name string
	}{
		{name: "put_policy", call: func(c *mediastoresdk.Client) error {
			_, err := c.PutContainerPolicy(t.Context(), &mediastoresdk.PutContainerPolicyInput{
				ContainerName: name, Policy: aws.String(`{"Version":"2012-10-17"}`),
			})

			return err
		}},
		{name: "put_cors", call: func(c *mediastoresdk.Client) error {
			_, err := c.PutCorsPolicy(t.Context(), &mediastoresdk.PutCorsPolicyInput{
				ContainerName: name,
				CorsPolicy:    []types.CorsRule{{AllowedOrigins: []string{"*"}, AllowedHeaders: []string{"*"}}},
			})

			return err
		}},
		{name: "start_logging", call: func(c *mediastoresdk.Client) error {
			_, err := c.StartAccessLogging(t.Context(), &mediastoresdk.StartAccessLoggingInput{ContainerName: name})

			return err
		}},
		{name: "get_lifecycle", call: func(c *mediastoresdk.Client) error {
			_, err := c.GetLifecyclePolicy(t.Context(), &mediastoresdk.GetLifecyclePolicyInput{ContainerName: name})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediastore.NewInMemoryBackend()
			b.SetActivationDelay(time.Hour)
			h := mediastore.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion
			client := newTestMediaStoreClient(t, h)

			_, err := client.CreateContainer(t.Context(), &mediastoresdk.CreateContainerInput{ContainerName: name})
			require.NoError(t, err)

			var inUse *types.ContainerInUseException
			require.ErrorAs(t, tt.call(client), &inUse)
			assert.NotEmpty(t, inUse.ErrorMessage())
		})
	}
}
