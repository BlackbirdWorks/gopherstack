package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestDescribeEndpoint_Shapes_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointType string
		wantPattern  string
		wantErr      bool
	}{
		{name: "default", wantPattern: `^[a-z0-9]{14}\.iot\.us-east-1\.amazonaws\.com$`},
		{name: "data", endpointType: "iot:Data", wantPattern: `^[a-z0-9]{14}\.iot\.us-east-1\.amazonaws\.com$`},
		{name: "ats", endpointType: "iot:Data-ATS", wantPattern: `^[a-z0-9]{14}-ats\.iot\.us-east-1\.amazonaws\.com$`},
		{
			name: "credentials", endpointType: "iot:CredentialProvider",
			wantPattern: `^[a-z0-9]{14}\.credentials\.iot\.us-east-1\.amazonaws\.com$`,
		},
		{name: "jobs", endpointType: "iot:Jobs", wantPattern: `^[a-z0-9]{14}\.jobs\.iot\.us-east-1\.amazonaws\.com$`},
		{name: "unknown", endpointType: "iot:Nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))
			in := &iotsdk.DescribeEndpointInput{}
			if tt.endpointType != "" {
				in.EndpointType = aws.String(tt.endpointType)
			}

			out, err := client.DescribeEndpoint(t.Context(), in)
			if tt.wantErr {
				var ire *types.InvalidRequestException
				require.ErrorAs(t, err, &ire)

				return
			}

			require.NoError(t, err)
			assert.Regexp(t, tt.wantPattern, aws.ToString(out.EndpointAddress))

			again, err := client.DescribeEndpoint(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(out.EndpointAddress), aws.ToString(again.EndpointAddress))
		})
	}
}
