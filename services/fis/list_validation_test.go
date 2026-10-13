package fis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	fistypes "github.com/aws/aws-sdk-go-v2/service/fis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestListOps_RejectBadPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(c *fissdk.Client) error
		name string
	}{
		{name: "actions token", call: func(c *fissdk.Client) error {
			_, err := c.ListActions(t.Context(), &fissdk.ListActionsInput{NextToken: aws.String("%%bad")})

			return err
		}},
		{name: "templates token", call: func(c *fissdk.Client) error {
			_, err := c.ListExperimentTemplates(t.Context(),
				&fissdk.ListExperimentTemplatesInput{NextToken: aws.String("%%bad")})

			return err
		}},
		{name: "experiments token", call: func(c *fissdk.Client) error {
			_, err := c.ListExperiments(t.Context(), &fissdk.ListExperimentsInput{NextToken: aws.String("%%bad")})

			return err
		}},
		{name: "experiments max", call: func(c *fissdk.Client) error {
			_, err := c.ListExperiments(t.Context(), &fissdk.ListExperimentsInput{MaxResults: aws.Int32(101)})

			return err
		}},
		{name: "target types max", call: func(c *fissdk.Client) error {
			_, err := c.ListTargetResourceTypes(t.Context(),
				&fissdk.ListTargetResourceTypesInput{MaxResults: aws.Int32(500)})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := fis.NewInMemoryBackend("123456789012", "us-east-1")
			t.Cleanup(backend.Close)
			client, _ := newTestFISClient(t, fis.NewHandler(backend))

			var ve *fistypes.ValidationException
			require.ErrorAs(t, tt.call(client), &ve)
		})
	}
}

func TestNotFoundMessages_ReadNaturally(t *testing.T) {
	t.Parallel()

	backend := fis.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(backend.Close)
	client, _ := newTestFISClient(t, fis.NewHandler(backend))

	_, err := client.GetExperiment(t.Context(), &fissdk.GetExperimentInput{Id: aws.String("EXPnope")})

	var nf *fistypes.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.Equal(t, "Experiment EXPnope does not exist", aws.ToString(nf.Message))
	assert.NotContains(t, aws.ToString(nf.Message), "NotFound")
}
