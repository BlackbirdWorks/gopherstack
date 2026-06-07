package firehose

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func TestFirehoseRegionIsolation(t *testing.T) {
	backend := NewInMemoryBackend(config.DefaultAccountID, "us-east-1")
	handler := NewHandler(backend)
	e := echo.New()

	createStream := func(region, name string) {
		input := createDeliveryStreamInput{
			DeliveryStreamName: name,
			DeliveryStreamType: "DirectPut",
		}
		body, _ := json.Marshal(input)
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		req.Header.Set("X-Amz-Target", firehoseTargetPrefix+"CreateDeliveryStream")
		req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=AKIA/20260607/%s/firehose/aws4_request", region))

		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	}

	listStreams := func(region string) []string {
		input := listDeliveryStreamsInput{}
		body, _ := json.Marshal(input)
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		req.Header.Set("X-Amz-Target", firehoseTargetPrefix+"ListDeliveryStreams")
		req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=AKIA/20260607/%s/firehose/aws4_request", region))

		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var output listDeliveryStreamsOutput
		err = json.Unmarshal(rec.Body.Bytes(), &output)
		require.NoError(t, err)
		return output.DeliveryStreamNames
	}

	// 1. Create stream in us-east-1
	createStream("us-east-1", "stream-east")

	// 2. Create stream in us-west-2
	createStream("us-west-2", "stream-west")

	// 3. Verify us-east-1 only sees its stream
	eastStreams := listStreams("us-east-1")
	assert.Contains(t, eastStreams, "stream-east")
	assert.NotContains(t, eastStreams, "stream-west")

	// 4. Verify us-west-2 only sees its stream
	westStreams := listStreams("us-west-2")
	assert.Contains(t, westStreams, "stream-west")
	assert.NotContains(t, westStreams, "stream-east")
}
