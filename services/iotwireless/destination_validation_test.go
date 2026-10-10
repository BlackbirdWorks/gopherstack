package iotwireless_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotwirelesssdk "github.com/aws/aws-sdk-go-v2/service/iotwireless"
	types "github.com/aws/aws-sdk-go-v2/service/iotwireless/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDestination_ConflictAndValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		destName string
		exprType types.ExpressionType
		wantErr  string
		dup      bool
	}{
		{name: "ok", destName: "d1", exprType: types.ExpressionTypeRuleName},
		{
			name:     "duplicate",
			destName: "d1",
			exprType: types.ExpressionTypeRuleName,
			dup:      true,
			wantErr:  "ConflictException",
		},
		{
			name:     "bad_name",
			destName: "bad name",
			exprType: types.ExpressionTypeRuleName,
			wantErr:  "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIoTWirelessSDKClient(t, newTestIoTWirelessRegistryServer(t).URL)
			in := func() *iotwirelesssdk.CreateDestinationInput {
				return &iotwirelesssdk.CreateDestinationInput{
					Name: aws.String(tt.destName), Expression: aws.String("rule"), ExpressionType: tt.exprType,
					RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
				}
			}

			if tt.dup {
				_, err := client.CreateDestination(t.Context(), in())
				require.NoError(t, err)
			}

			_, err := client.CreateDestination(t.Context(), in())
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			var apiErr interface {
				ErrorCode() string
				ErrorMessage() string
			}

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantErr, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception")
		})
	}
}

func TestListPagination_InvalidParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token   *string
		name    string
		max     int32
		wantErr bool
	}{
		{name: "valid", max: 5},
		{name: "bad_token", token: aws.String("garbage"), wantErr: true},
		{name: "too_big", max: 251, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIoTWirelessSDKClient(t, newTestIoTWirelessRegistryServer(t).URL)

			_, err := client.ListDestinations(t.Context(), &iotwirelesssdk.ListDestinationsInput{
				NextToken: tt.token, MaxResults: tt.max,
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr interface{ ErrorCode() string }

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
		})
	}
}
