package mediastoredata_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	msdsdk "github.com/aws/aws-sdk-go-v2/service/mediastoredata"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

// The Go SDK serializes Path "/x" as URL "//x"; terraform sends it that way.
func TestPutObject_LeadingSlashPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		get  string
	}{
		{name: "no slash", path: "obj.txt", get: "obj.txt"},
		{name: "one slash", path: "/obj.txt", get: "obj.txt"},
		{name: "folder one slash", path: "/dir/obj.txt", get: "dir/obj.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMSDSDKClient(t, mediastoredata.NewHandler(mediastoredata.NewInMemoryBackend("us-east-1")))

			_, err := client.PutObject(t.Context(), &msdsdk.PutObjectInput{
				Path: aws.String(tt.path), Body: bytes.NewReader([]byte("x")),
			})
			require.NoError(t, err)

			_, err = client.GetObject(t.Context(), &msdsdk.GetObjectInput{Path: aws.String(tt.get)})
			require.NoError(t, err)
		})
	}
}
