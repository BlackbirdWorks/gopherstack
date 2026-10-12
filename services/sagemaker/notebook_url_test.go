package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestNotebookInstance_UrlHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		nb     string
		want   string
	}{
		{name: "us east", region: "us-east-1", nb: "nb1", want: "nb1.notebook.us-east-1.sagemaker.aws"},
		{name: "hyphenated", region: "us-east-1", nb: "my-nb", want: "my-nb.notebook.us-east-1.sagemaker.aws"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := sagemaker.NewHandler(sagemaker.NewInMemoryBackend("000000000000", tt.region))

			rr := doSageMakerRequest(t, h, "CreateNotebookInstance", map[string]any{
				"NotebookInstanceName": tt.nb, "InstanceType": "ml.t3.medium",
				"RoleArn": "arn:aws:iam::000000000000:role/sm",
			})
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

			rr = doSageMakerRequest(t, h, "DescribeNotebookInstance", map[string]any{"NotebookInstanceName": tt.nb})
			require.Equal(t, http.StatusOK, rr.Code)

			var out struct {
				URL string `json:"Url"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
			assert.Equal(t, tt.want, out.URL)
		})
	}
}
