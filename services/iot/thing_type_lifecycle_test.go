package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestThingTypeLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(t *testing.T, c *iotsdk.Client) error
		name    string
		wantErr string
	}{
		{
			name: "create_thing_with_deprecated_type", wantErr: "InvalidRequestException",
			run: func(t *testing.T, c *iotsdk.Client) error {
				t.Helper()

				_, err := c.CreateThing(t.Context(), &iotsdk.CreateThingInput{
					ThingName: aws.String("t"), ThingTypeName: aws.String("tt"),
				})

				return err
			},
		},
		{
			name: "update_thing_to_deprecated_type", wantErr: "InvalidRequestException",
			run: func(t *testing.T, c *iotsdk.Client) error {
				t.Helper()

				_, err := c.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("t")})
				require.NoError(t, err)

				_, err = c.UpdateThing(t.Context(), &iotsdk.UpdateThingInput{
					ThingName: aws.String("t"), ThingTypeName: aws.String("tt"),
				})

				return err
			},
		},
		{
			name: "update_thing_to_unknown_type", wantErr: "ResourceNotFoundException",
			run: func(t *testing.T, c *iotsdk.Client) error {
				t.Helper()

				_, err := c.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("t")})
				require.NoError(t, err)

				_, err = c.UpdateThing(t.Context(), &iotsdk.UpdateThingInput{
					ThingName: aws.String("t"), ThingTypeName: aws.String("nope"),
				})

				return err
			},
		},
		{
			name: "delete_type_with_things", wantErr: "InvalidRequestException",
			run: func(t *testing.T, c *iotsdk.Client) error {
				t.Helper()

				_, err := c.CreateThingType(
					t.Context(),
					&iotsdk.CreateThingTypeInput{ThingTypeName: aws.String("live")},
				)
				require.NoError(t, err)

				_, err = c.CreateThing(t.Context(), &iotsdk.CreateThingInput{
					ThingName: aws.String("t"), ThingTypeName: aws.String("live"),
				})
				require.NoError(t, err)

				_, err = c.DeprecateThingType(
					t.Context(),
					&iotsdk.DeprecateThingTypeInput{ThingTypeName: aws.String("live")},
				)
				require.NoError(t, err)

				_, err = c.DeleteThingType(t.Context(), &iotsdk.DeleteThingTypeInput{ThingTypeName: aws.String("live")})

				return err
			},
		},
		{
			name: "delete_type_after_detaching_things",
			run: func(t *testing.T, c *iotsdk.Client) error {
				t.Helper()

				_, err := c.CreateThingType(
					t.Context(),
					&iotsdk.CreateThingTypeInput{ThingTypeName: aws.String("live")},
				)
				require.NoError(t, err)

				_, err = c.CreateThing(t.Context(), &iotsdk.CreateThingInput{
					ThingName: aws.String("t"), ThingTypeName: aws.String("live"),
				})
				require.NoError(t, err)

				_, err = c.DeprecateThingType(
					t.Context(),
					&iotsdk.DeprecateThingTypeInput{ThingTypeName: aws.String("live")},
				)
				require.NoError(t, err)

				_, err = c.UpdateThing(
					t.Context(),
					&iotsdk.UpdateThingInput{ThingName: aws.String("t"), RemoveThingType: true},
				)
				require.NoError(t, err)

				_, err = c.DeleteThingType(t.Context(), &iotsdk.DeleteThingTypeInput{ThingTypeName: aws.String("live")})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))

			_, err := c.CreateThingType(t.Context(), &iotsdk.CreateThingTypeInput{ThingTypeName: aws.String("tt")})
			require.NoError(t, err)

			_, err = c.DeprecateThingType(t.Context(), &iotsdk.DeprecateThingTypeInput{ThingTypeName: aws.String("tt")})
			require.NoError(t, err)

			err = tt.run(t, c)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)

			var apiErr interface{ ErrorCode() string }

			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, tt.wantErr, apiErr.ErrorCode())
		})
	}
}
