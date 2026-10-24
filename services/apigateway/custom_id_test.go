package apigateway_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateRestAPICustomIDTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantID     string
		wantStatus int
	}{
		{name: "localstack tag", body: `{"name":"a","tags":{"_custom_id_":"myapi","env":"dev"}}`, wantID: "myapi", wantStatus: http.StatusCreated},
		{name: "native tag", body: `{"name":"a","tags":{"gopherstack:id":"native1"}}`, wantID: "native1", wantStatus: http.StatusCreated},
		{name: "hyphen id", body: `{"name":"a","tags":{"_custom_id_":"my-api-1"}}`, wantID: "my-api-1", wantStatus: http.StatusCreated},
		{name: "uppercase rejected", body: `{"name":"a","tags":{"_custom_id_":"UPPER"}}`, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newProxyRoutingServer(t)

			var out struct {
				ID   string            `json:"id"`
				Tags map[string]string `json:"tags"`
			}

			status := jsonAction(t, srv.Client(), srv.URL, "CreateRestApi", tt.body, &out)
			require.Equal(t, tt.wantStatus, status)

			if tt.wantID == "" {
				return
			}

			assert.Equal(t, tt.wantID, out.ID)
			assert.NotContains(t, out.Tags, "_custom_id_")
			assert.NotContains(t, out.Tags, "gopherstack:id")

			dup := jsonAction(t, srv.Client(), srv.URL, "CreateRestApi", tt.body, nil)
			assert.Equal(t, http.StatusConflict, dup)
		})
	}
}

func TestTagResourceRejectsCustomIDTag(t *testing.T) {
	t.Parallel()

	srv := newProxyRoutingServer(t)

	var out struct {
		ID string `json:"id"`
	}

	require.Equal(t, http.StatusCreated,
		jsonAction(t, srv.Client(), srv.URL, "CreateRestApi", `{"name":"a"}`, &out))

	status := jsonAction(t, srv.Client(), srv.URL, "TagResource",
		`{"resourceArn":"arn:aws:apigateway:us-east-1::/restapis/`+out.ID+`","tags":{"_custom_id_":"x"}}`, nil)
	assert.Equal(t, http.StatusBadRequest, status)
}
