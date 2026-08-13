package workspaces_test

import (
	"maps"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestImportWorkspaceImage_RequiredMembers locks in IngestionProcess being
// rejected when absent (workspaces@v1.73.1 api_op_ImportWorkspaceImage.go:67,
// "This member is required"). The real SDK client validates this
// client-side (validators.go's validateOpImportWorkspaceImageInput), so this
// drives the raw HTTP handler directly to exercise the server-side
// InvalidParameterValuesException path a non-SDK caller would still hit.
func TestImportWorkspaceImage_RequiredMembers(t *testing.T) {
	t.Parallel()

	t.Run("missing ingestionprocess rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doTargetRequest(t, h, "ImportWorkspaceImage", map[string]any{
			"Ec2ImageId":       "ami-12345678",
			"ImageName":        "imported",
			"ImageDescription": "ec2 import",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid ingestionprocess rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doTargetRequest(t, h, "ImportWorkspaceImage", map[string]any{
			"Ec2ImageId":       "ami-12345678",
			"ImageName":        "imported",
			"ImageDescription": "ec2 import",
			"IngestionProcess": "NOT_A_REAL_PROCESS",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestImportCustomWorkspaceImage_RequiredMembers locks in ComputeType,
// ImageSource, InfrastructureConfigurationArn, OsVersion, Platform and
// Protocol being rejected when absent (workspaces@v1.73.1
// api_op_ImportCustomWorkspaceImage.go:33-75, all "This member is
// required"). Driven via raw HTTP for the same client-side-validation
// reason as TestImportWorkspaceImage_RequiredMembers above.
func TestImportCustomWorkspaceImage_RequiredMembers(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"ImageName":                      "custom-img",
		"ImageDescription":               "custom",
		"ComputeType":                    "BASE",
		"ImageSource":                    map[string]any{"Ec2ImageId": "ami-custom"},
		"InfrastructureConfigurationArn": "arn:aws:imagebuilder:us-east-1:000000000000:infrastructure-configuration/test",
		"OsVersion":                      "Windows_11",
		"Platform":                       "WINDOWS",
		"Protocol":                       "DCV",
	}

	withoutKey := func(key string) map[string]any {
		body := make(map[string]any, len(full))
		for k, v := range full {
			if k != key {
				body[k] = v
			}
		}

		return body
	}

	tests := []struct {
		name   string
		absent string
	}{
		{name: "missing computetype rejected", absent: "ComputeType"},
		{name: "missing imagesource rejected", absent: "ImageSource"},
		{name: "missing infrastructureconfigurationarn rejected", absent: "InfrastructureConfigurationArn"},
		{name: "missing osversion rejected", absent: "OsVersion"},
		{name: "missing platform rejected", absent: "Platform"},
		{name: "missing protocol rejected", absent: "Protocol"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTargetRequest(t, h, "ImportCustomWorkspaceImage", withoutKey(tt.absent))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}

	t.Run("invalid enum values rejected", func(t *testing.T) {
		t.Parallel()

		for _, field := range []string{"ComputeType", "OsVersion", "Platform", "Protocol"} {
			body := make(map[string]any, len(full))
			maps.Copy(body, full)

			body[field] = "NOT_A_REAL_VALUE"

			h := newTestHandler(t)
			rec := doTargetRequest(t, h, "ImportCustomWorkspaceImage", body)
			assert.Equalf(t, http.StatusBadRequest, rec.Code, "field %s", field)
		}
	})
}

// TestSDKRoundTrip_ImportImages_EchoesRequiredMembers drives
// ImportWorkspaceImage and ImportCustomWorkspaceImage through the real
// aws-sdk-go-v2 client and proves their required members are accepted on
// create and observable afterward -- IngestionProcess isn't echoed anywhere
// on the real wire (ImportWorkspaceImageOutput only carries ImageId), so
// that half is proven by the create call itself succeeding through the real
// client's request-side validation; ImageSource and
// InfrastructureConfigurationArn are both present on the real
// DescribeCustomWorkspaceImageImportOutput (types.go via
// api_op_DescribeCustomWorkspaceImageImport.go:39-73) and are checked there.
func TestSDKRoundTrip_ImportImages_EchoesRequiredMembers(t *testing.T) {
	t.Parallel()

	t.Run("import workspace image", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		out, err := client.ImportWorkspaceImage(t.Context(), &wssdk.ImportWorkspaceImageInput{
			Ec2ImageId:       aws.String("ami-12345678"),
			ImageName:        aws.String("rt-imported"),
			ImageDescription: aws.String("rt ec2 import"),
			IngestionProcess: types.WorkspaceImageIngestionProcessByolRegular,
		})
		require.NoError(t, err)
		require.NotNil(t, out.ImageId)
		assert.NotEmpty(t, *out.ImageId)
	})

	t.Run("import custom workspace image", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		created, err := client.ImportCustomWorkspaceImage(t.Context(), &wssdk.ImportCustomWorkspaceImageInput{
			ImageName:        aws.String("rt-custom-img"),
			ImageDescription: aws.String("rt custom"),
			ComputeType:      types.ImageComputeTypeBase,
			ImageSource: &types.ImageSourceIdentifierMemberEc2ImageId{
				Value: "ami-rt-custom",
			},
			InfrastructureConfigurationArn: aws.String(
				"arn:aws:imagebuilder:us-east-1:000000000000:infrastructure-configuration/rt-test",
			),
			OsVersion: types.OSVersionWindows11,
			Platform:  types.PlatformWindows,
			Protocol:  types.CustomImageProtocolDcv,
		})
		require.NoError(t, err)
		require.NotNil(t, created.ImageId)

		described, err := client.DescribeCustomWorkspaceImageImport(
			t.Context(),
			&wssdk.DescribeCustomWorkspaceImageImportInput{ImageId: created.ImageId},
		)
		require.NoError(t, err)

		require.NotNil(t, described.InfrastructureConfigurationArn)
		assert.Equal(t,
			"arn:aws:imagebuilder:us-east-1:000000000000:infrastructure-configuration/rt-test",
			*described.InfrastructureConfigurationArn,
		)

		require.NotNil(t, described.ImageSource)
		src, ok := described.ImageSource.(*types.ImageSourceIdentifierMemberEc2ImageId)
		require.True(t, ok, "ImageSource must round-trip as Ec2ImageId")
		assert.Equal(t, "ami-rt-custom", src.Value)
	})
}
