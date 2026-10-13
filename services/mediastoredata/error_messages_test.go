//go:build !integration

package mediastoredata_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	msdsdk "github.com/aws/aws-sdk-go-v2/service/mediastoredata"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

func TestErrors_SDKCodeAndMessage(t *testing.T) {
	t.Parallel()

	put := func(path string) func(*msdsdk.Client) error {
		return func(c *msdsdk.Client) error {
			_, err := c.PutObject(t.Context(), &msdsdk.PutObjectInput{
				Path: aws.String(path), Body: bytes.NewReader([]byte("x")),
			})

			return err
		}
	}

	tests := []struct {
		call    func(*msdsdk.Client) error
		name    string
		code    string
		message string
	}{
		{
			name: "get_missing", code: "ObjectNotFoundException", message: `object "/a/nope" not found`,
			call: func(c *msdsdk.Client) error {
				_, err := c.GetObject(t.Context(), &msdsdk.GetObjectInput{Path: aws.String("a/nope")})

				return err
			},
		},
		{name: "dotdot", code: "ValidationException", message: "path cannot contain '..'", call: put("a/../b")},
		{
			name: "trailing_slash", code: "ValidationException",
			message: "path must not contain empty folder names", call: put("a/b/"),
		},
		{
			name: "double_slash", code: "ValidationException",
			message: "path must not contain empty folder names", call: put("a//b"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMSDSDKClient(t, mediastoredata.NewHandler(mediastoredata.NewInMemoryBackend("us-east-1")))

			err := tt.call(client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.code, apiErr.ErrorCode())
			assert.Equal(t, tt.message, apiErr.ErrorMessage())
		})
	}
}
