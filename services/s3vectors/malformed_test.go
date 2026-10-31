package s3vectors_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestMalformedBodiesAreClientErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t, "mal-bucket")
	f.index(t, "mal-bucket", "mal", "cosine", 2)

	target := `"vectorBucketName":"mal-bucket","indexName":"mal"`

	tests := []struct {
		name, path, body string
	}{
		{"not json", "/CreateVectorBucket", `{`},
		{"name wrong type", "/CreateVectorBucket", `{"vectorBucketName":5}`},
		{"dimension wrong type", "/CreateIndex", `{"vectorBucketName":"mal-bucket","indexName":"x1x","dimension":"4"}`},
		{"vectors wrong type", "/PutVectors", `{` + target + `,"vectors":5}`},
		{"data wrong type", "/PutVectors", `{` + target + `,"vectors":[{"key":"a","data":{"float32":"x"}}]}`},
		{
			"metadata wrong type",
			"/PutVectors",
			`{` + target + `,"vectors":[{"key":"a","data":{"float32":[1,1]},"metadata":7}]}`,
		},
		{"query vector missing", "/QueryVectors", `{` + target + `,"topK":1}`},
		{"filter wrong type", "/QueryVectors", `{` + target + `,"topK":1,"queryVector":{"float32":[1,1]},"filter":5}`},
		{
			"filter nested wrong type",
			"/QueryVectors",
			`{` + target + `,"topK":1,"queryVector":{"float32":[1,1]},"filter":{"a":{"$in":5}}}`,
		},
		{"keys wrong type", "/GetVectors", `{` + target + `,"keys":"a"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodPost,
				f.url+tt.path,
				strings.NewReader(tt.body),
			)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("User-Agent", "aws-sdk-go-v2/1.41.0 api/s3vectors#1.0.0")

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}
