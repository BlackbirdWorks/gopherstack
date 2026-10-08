package iotdataplane_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	"github.com/aws/aws-sdk-go-v2/service/iotdataplane/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

type ackingMock struct {
	*mockMQTTPublisher

	ackErr  error
	timeout time.Duration
}

func (m *ackingMock) SendToClientAwaitAck(
	clientID, _ string, _ []byte, _ iotdataplane.MQTT5Properties, timeout time.Duration,
) (bool, error) {
	m.timeout = timeout

	return m.knownClients[clientID], m.ackErr
}

func TestSendDirectMessage_Confirmation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ackErr      error
		name        string
		wantErr     string
		wantTimeout time.Duration
		timeout     int32
		confirm     bool
	}{
		{name: "default_timeout", confirm: true, wantTimeout: 5 * time.Second},
		{name: "explicit_timeout", confirm: true, timeout: 9, wantTimeout: 9 * time.Second},
		{name: "no_puback", confirm: true, ackErr: iotdataplane.ErrDeliveryTimeout, wantErr: "GatewayTimeoutException"},
		{name: "timeout_out_of_range", confirm: true, timeout: 16, wantErr: "InvalidRequestException"},
		{name: "timeout_ignored_without_confirmation", confirm: false, timeout: 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := iotdataplane.NewInMemoryBackend()
			mock := &ackingMock{
				mockMQTTPublisher: &mockMQTTPublisher{knownClients: map[string]bool{"c1": true}},
				ackErr:            tt.ackErr,
			}
			backend.SetBroker(mock)
			require.NoError(t, backend.RegisterConnection("c1", "10.0.0.1"))

			client := newTestIoTDataPlaneClient(t, iotdataplane.NewHandler(backend))

			_, err := client.SendDirectMessage(t.Context(), &iotdataplanesdk.SendDirectMessageInput{
				ClientId:     aws.String("c1"),
				Topic:        aws.String("t/1"),
				Payload:      []byte("x"),
				Confirmation: tt.confirm,
				Timeout:      tt.timeout,
			})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				if tt.wantErr == "GatewayTimeoutException" {
					var gw *types.GatewayTimeoutException
					require.ErrorAs(t, err, &gw)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTimeout, mock.timeout)
		})
	}
}
