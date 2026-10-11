package ec2_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestSSMPublicParameterAMIsResolve(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("../ssm/public_parameters.json")
	require.NoError(t, err)

	var catalog struct {
		Parameters []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"parameters"`
	}

	require.NoError(t, json.Unmarshal(raw, &catalog))

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))

	out, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{})
	require.NoError(t, err)

	images := map[string]types.Image{}
	for _, img := range out.Images {
		images[aws.ToString(img.ImageId)] = img
	}

	checked := 0

	for _, p := range catalog.Parameters {
		if !strings.HasPrefix(p.Value, "ami-") {
			continue
		}

		checked++

		t.Run(p.Name, func(t *testing.T) {
			t.Parallel()

			img, ok := images[p.Value]
			require.True(t, ok, "%s is not in the EC2 image catalog", p.Value)
			assert.NotEmpty(t, aws.ToString(img.Name))
			assert.Equal(t, strings.Contains(p.Name, "ami-windows-latest"), string(img.Platform) == "windows")

			wantArm := strings.Contains(p.Name, "arm64")
			assert.Equal(t, wantArm, img.Architecture == types.ArchitectureValuesArm64, p.Name)

			_, runErr := b.RunInstances(p.Value, "t3.micro", "", 1)
			require.NoError(t, runErr)
		})
	}

	assert.Positive(t, checked)
}
