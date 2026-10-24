package outposts_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/outposts"
)

func testVpcInformation() []types.VpcInformation {
	return []types.VpcInformation{{
		VpcId:         aws.String("vpc-0123456789abcdef0"),
		SubnetIds:     []string{"subnet-0123456789abcdef0"},
		VpcEndpointId: aws.String("vpce-0123456789abcdef0"),
	}}
}

func TestPrivateConnectivityConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	o := createTestOutpost(t, client, createTestSite(t, client))

	createIn := &outpostssdk.CreatePrivateConnectivityConfigInput{
		OutpostId:          o.OutpostId,
		VpcInformationList: testVpcInformation(),
	}
	created, err := client.CreatePrivateConnectivityConfig(t.Context(), createIn)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(o.OutpostId), aws.ToString(created.OutpostId))
	require.NotNil(t, created.PrivateConnectivityConfig)
	assert.Equal(t, types.PrivateConnectivityStatusEnabled, created.PrivateConnectivityConfig.PrivateConnectivityStatus)
	assert.NotEmpty(t, aws.ToString(created.PrivateConnectivityConfig.RoleArn))
	assert.Equal(t, testVpcInformation(), created.PrivateConnectivityConfig.VpcInformationList)

	got, err := client.GetPrivateConnectivityConfig(t.Context(), &outpostssdk.GetPrivateConnectivityConfigInput{
		OutpostId: o.OutpostId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.PrivateConnectivityConfig)
	assert.Equal(t, created.PrivateConnectivityConfig.RoleArn, got.PrivateConnectivityConfig.RoleArn)
	assert.Equal(t, types.PrivateConnectivityStatusEnabled, got.PrivateConnectivityConfig.PrivateConnectivityStatus)
	assert.Equal(t, testVpcInformation(), got.PrivateConnectivityConfig.VpcInformationList)
}

func TestPrivateConnectivityConfig_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		run  func(ctx context.Context, client *outpostssdk.Client, outpostID *string) error
		name string
	}{
		{
			name: "create unknown outpost",
			want: new(*types.NotFoundException),
			run: func(ctx context.Context, client *outpostssdk.Client, _ *string) error {
				_, err := client.CreatePrivateConnectivityConfig(ctx, &outpostssdk.CreatePrivateConnectivityConfigInput{
					OutpostId:          aws.String("op-doesnotexist"),
					VpcInformationList: testVpcInformation(),
				})

				return err
			},
		},
		{
			name: "get unknown outpost",
			want: new(*types.NotFoundException),
			run: func(ctx context.Context, client *outpostssdk.Client, _ *string) error {
				_, err := client.GetPrivateConnectivityConfig(ctx, &outpostssdk.GetPrivateConnectivityConfigInput{
					OutpostId: aws.String("op-doesnotexist"),
				})

				return err
			},
		},
		{
			name: "get unconfigured",
			want: new(*types.NotFoundException),
			run: func(ctx context.Context, client *outpostssdk.Client, outpostID *string) error {
				_, err := client.GetPrivateConnectivityConfig(ctx, &outpostssdk.GetPrivateConnectivityConfigInput{
					OutpostId: outpostID,
				})

				return err
			},
		},
		{
			name: "create duplicate",
			want: new(*types.ConflictException),
			run: func(ctx context.Context, client *outpostssdk.Client, outpostID *string) error {
				in := &outpostssdk.CreatePrivateConnectivityConfigInput{
					OutpostId: outpostID, VpcInformationList: testVpcInformation(),
				}
				if _, err := client.CreatePrivateConnectivityConfig(ctx, in); err != nil {
					return err
				}
				_, err := client.CreatePrivateConnectivityConfig(ctx, in)

				return err
			},
		},
		{
			name: "create empty vpc list",
			want: new(*types.ValidationException),
			run: func(ctx context.Context, client *outpostssdk.Client, outpostID *string) error {
				_, err := client.CreatePrivateConnectivityConfig(ctx, &outpostssdk.CreatePrivateConnectivityConfigInput{
					OutpostId: outpostID, VpcInformationList: []types.VpcInformation{},
				})

				return err
			},
		},
		{
			name: "create two subnets",
			want: new(*types.ValidationException),
			run: func(ctx context.Context, client *outpostssdk.Client, outpostID *string) error {
				vpc := testVpcInformation()
				vpc[0].SubnetIds = []string{"subnet-a", "subnet-b"}
				_, err := client.CreatePrivateConnectivityConfig(ctx, &outpostssdk.CreatePrivateConnectivityConfigInput{
					OutpostId: outpostID, VpcInformationList: vpc,
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			o := createTestOutpost(t, client, createTestSite(t, client))

			err := tc.run(t.Context(), client, o.OutpostId)
			require.Error(t, err)

			switch want := tc.want.(type) {
			case **types.NotFoundException:
				require.ErrorAs(t, err, want)
			case **types.ConflictException:
				require.ErrorAs(t, err, want)
			case **types.ValidationException:
				require.ErrorAs(t, err, want)
			}
		})
	}
}

func TestPrivateConnectivityConfig_PersistAndDelete(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	o := createTestOutpost(t, client, createTestSite(t, client))

	_, err := client.CreatePrivateConnectivityConfig(t.Context(), &outpostssdk.CreatePrivateConnectivityConfigInput{
		OutpostId: o.OutpostId, VpcInformationList: testVpcInformation(),
	})
	require.NoError(t, err)

	restored := outposts.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(restored.Close)
	require.NoError(t, restored.Restore(t.Context(), h.Snapshot(t.Context())))
	restoredClient := newRoundTripClient(t, outposts.NewHandler(restored))

	got, err := restoredClient.GetPrivateConnectivityConfig(t.Context(), &outpostssdk.GetPrivateConnectivityConfigInput{
		OutpostId: o.OutpostId,
	})
	require.NoError(t, err)
	assert.Equal(t, testVpcInformation(), got.PrivateConnectivityConfig.VpcInformationList)

	_, err = client.DeleteOutpost(t.Context(), &outpostssdk.DeleteOutpostInput{OutpostId: o.OutpostId})
	require.NoError(t, err)

	_, err = client.GetPrivateConnectivityConfig(t.Context(), &outpostssdk.GetPrivateConnectivityConfigInput{
		OutpostId: o.OutpostId,
	})
	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}
