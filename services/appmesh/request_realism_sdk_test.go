package appmesh_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appmeshsdk "github.com/aws/aws-sdk-go-v2/service/appmesh"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestRealism_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call        func(c *appmeshsdk.Client) error
		name        string
		wantCode    string
		wantMessage string
	}{
		{
			name: "bad next token",
			call: func(c *appmeshsdk.Client) error {
				_, err := c.ListMeshes(t.Context(), &appmeshsdk.ListMeshesInput{NextToken: aws.String("!!!")})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "limit too large",
			call: func(c *appmeshsdk.Client) error {
				_, err := c.ListMeshes(t.Context(), &appmeshsdk.ListMeshesInput{Limit: aws.Int32(101)})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "name too long",
			call: func(c *appmeshsdk.Client) error {
				_, err := c.CreateMesh(
					t.Context(),
					&appmeshsdk.CreateMeshInput{MeshName: aws.String(strings.Repeat("a", 256))},
				)

				return err
			},
			wantCode:    "BadRequestException",
			wantMessage: "between 1 and 255",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newTestHandlerAndClient(t))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMessage)
		})
	}
}
