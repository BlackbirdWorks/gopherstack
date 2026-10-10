package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPackageMutationTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e iotDroppedEnv)
		name string
	}{
		{
			name: "delete package replay succeeds",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreatePackage(t.Context(), &iotsdk.CreatePackageInput{PackageName: aws.String("p")})
				require.NoError(t, err)

				del := &iotsdk.DeletePackageInput{PackageName: aws.String("p"), ClientToken: aws.String("t1")}
				_, err = e.client.DeletePackage(t.Context(), del)
				require.NoError(t, err)

				_, err = e.client.DeletePackage(t.Context(), del)
				require.NoError(t, err)

				_, err = e.client.DeletePackage(t.Context(), &iotsdk.DeletePackageInput{
					PackageName: aws.String("p"), ClientToken: aws.String("t2"),
				})
				require.Error(t, err)
			},
		},
		{
			name: "update package replay does not reapply",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreatePackage(t.Context(), &iotsdk.CreatePackageInput{PackageName: aws.String("p")})
				require.NoError(t, err)

				first := &iotsdk.UpdatePackageInput{
					PackageName: aws.String("p"), Description: aws.String("a"), ClientToken: aws.String("t1"),
				}
				_, err = e.client.UpdatePackage(t.Context(), first)
				require.NoError(t, err)

				_, err = e.client.UpdatePackage(t.Context(), &iotsdk.UpdatePackageInput{
					PackageName: aws.String("p"), Description: aws.String("b"), ClientToken: aws.String("t2"),
				})
				require.NoError(t, err)

				_, err = e.client.UpdatePackage(t.Context(), first)
				require.NoError(t, err)

				got, err := e.client.GetPackage(t.Context(), &iotsdk.GetPackageInput{PackageName: aws.String("p")})
				require.NoError(t, err)
				assert.Equal(t, "b", aws.ToString(got.Description))
			},
		},
		{
			name: "delete package version replay succeeds",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreatePackage(t.Context(), &iotsdk.CreatePackageInput{PackageName: aws.String("p")})
				require.NoError(t, err)
				_, err = e.client.CreatePackageVersion(t.Context(), &iotsdk.CreatePackageVersionInput{
					PackageName: aws.String("p"), VersionName: aws.String("1"),
				})
				require.NoError(t, err)

				del := &iotsdk.DeletePackageVersionInput{
					PackageName: aws.String("p"), VersionName: aws.String("1"), ClientToken: aws.String("t1"),
				}
				_, err = e.client.DeletePackageVersion(t.Context(), del)
				require.NoError(t, err)

				_, err = e.client.DeletePackageVersion(t.Context(), del)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newIoTDroppedEnv(t))
		})
	}
}
