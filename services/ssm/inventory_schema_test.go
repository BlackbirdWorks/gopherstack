package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func putCustom(t *testing.T, client *ssmsdk.Client, version string) error {
	t.Helper()

	_, err := client.PutInventory(t.Context(), &ssmsdk.PutInventoryInput{
		InstanceId: aws.String("i-custom"),
		Items: []ssmtypes.InventoryItem{{
			TypeName:      aws.String("Custom:Rack"),
			SchemaVersion: aws.String(version),
			CaptureTime:   aws.String("2024-01-01T00:00:00Z"),
			Content:       []map[string]string{{"Slot": "3", "Name": "a"}},
		}},
	})

	return err
}

func customSchema(t *testing.T, client *ssmsdk.Client) *ssmtypes.InventoryItemSchema {
	t.Helper()

	out, err := client.GetInventorySchema(t.Context(), &ssmsdk.GetInventorySchemaInput{
		TypeName: aws.String("Custom:"),
	})
	require.NoError(t, err)

	if len(out.Schemas) == 0 {
		return nil
	}

	return &out.Schemas[0]
}

func TestDeleteInventory_SchemaDeleteOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		typeName   string
		option     ssmtypes.InventorySchemaDeleteOption
		wantErr    string
		wantListed bool
	}{
		{name: "delete_schema", typeName: "Custom:Rack", option: ssmtypes.InventorySchemaDeleteOptionDeleteSchema},
		{
			name: "disable_schema", typeName: "Custom:Rack",
			option: ssmtypes.InventorySchemaDeleteOptionDisableSchema, wantListed: true,
		},
		{
			name: "unregistered_type", typeName: "Custom:Missing",
			option: ssmtypes.InventorySchemaDeleteOptionDeleteSchema, wantErr: "InvalidTypeNameException",
		},
		{
			name: "builtin_type", typeName: "AWS:Network",
			option: ssmtypes.InventorySchemaDeleteOptionDeleteSchema, wantErr: "InvalidTypeNameException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			require.NoError(t, putCustom(t, client, "1.0"))

			sch := customSchema(t, client)
			require.NotNil(t, sch)
			assert.Equal(t, "Custom:Rack", aws.ToString(sch.TypeName))
			require.Len(t, sch.Attributes, 2)
			assert.Equal(t, "Name", aws.ToString(sch.Attributes[0].Name))
			assert.Equal(t, ssmtypes.InventoryAttributeDataTypeString, sch.Attributes[0].DataType)
			assert.Equal(t, ssmtypes.InventoryAttributeDataTypeNumber, sch.Attributes[1].DataType)

			out, err := client.DeleteInventory(t.Context(), &ssmsdk.DeleteInventoryInput{
				TypeName: aws.String(tt.typeName), SchemaDeleteOption: tt.option,
			})
			if tt.wantErr != "" {
				require.Error(t, err)

				var invalid *ssmtypes.InvalidTypeNameException

				require.ErrorAs(t, err, &invalid)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.DeletionSummary)
			assert.Equal(t, int32(1), out.DeletionSummary.TotalCount)
			require.Len(t, out.DeletionSummary.SummaryItems, 1)
			assert.Equal(t, "1.0", aws.ToString(out.DeletionSummary.SummaryItems[0].Version))
			assert.Equal(t, tt.wantListed, customSchema(t, client) != nil)
		})
	}
}

func TestPutInventory_DisabledSchemaVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		wantErr bool
	}{
		{name: "same_version", version: "1.0", wantErr: true},
		{name: "lower_version", version: "0.9", wantErr: true},
		{name: "greater_version", version: "1.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			require.NoError(t, putCustom(t, client, "1.0"))

			_, err := client.DeleteInventory(t.Context(), &ssmsdk.DeleteInventoryInput{
				TypeName:           aws.String("Custom:Rack"),
				SchemaDeleteOption: ssmtypes.InventorySchemaDeleteOptionDisableSchema,
			})
			require.NoError(t, err)

			err = putCustom(t, client, tt.version)
			if tt.wantErr {
				var unsupported *ssmtypes.UnsupportedInventorySchemaVersionException

				require.Error(t, err)
				require.ErrorAs(t, err, &unsupported)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.version, aws.ToString(customSchema(t, client).Version))
		})
	}
}

func TestGetInventorySchema_BuiltinAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		typeName   string
		name       string
		wantFirst  string
		wantNumber string
		wantCount  int
	}{
		{name: "network", typeName: "AWS:Network", wantFirst: "Name", wantCount: 8},
		{
			name:       "patch_summary",
			typeName:   "AWS:PatchSummary",
			wantFirst:  "PatchGroup",
			wantNumber: "FailedCount",
			wantCount:  23,
		},
		{name: "tag", typeName: "AWS:Tag", wantFirst: "Key", wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			out, err := client.GetInventorySchema(t.Context(), &ssmsdk.GetInventorySchemaInput{
				TypeName: aws.String(tt.typeName),
			})
			require.NoError(t, err)
			require.Len(t, out.Schemas, 1)

			attrs := out.Schemas[0].Attributes
			require.Len(t, attrs, tt.wantCount)
			assert.Equal(t, tt.wantFirst, aws.ToString(attrs[0].Name))
			assert.Equal(t, ssmtypes.InventoryAttributeDataTypeString, attrs[0].DataType)

			if tt.wantNumber != "" {
				for _, a := range attrs {
					if aws.ToString(a.Name) == tt.wantNumber {
						assert.Equal(t, ssmtypes.InventoryAttributeDataTypeNumber, a.DataType)
					}
				}
			}
		})
	}
}

func TestDescribePatchProperties_FromCatalogue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		os       ssmtypes.OperatingSystem
		property ssmtypes.PatchProperty
		want     []map[string]string
	}{
		{
			name:     "windows_product",
			os:       ssmtypes.OperatingSystemWindows,
			property: ssmtypes.PatchPropertyProduct,
			want: []map[string]string{
				{"Name": "WindowsServer2019", "ProductFamily": "Windows"},
				{"Name": "WindowsServer2022", "ProductFamily": "Windows"},
			},
		},
		{
			name:     "windows_family",
			os:       ssmtypes.OperatingSystemWindows,
			property: ssmtypes.PatchPropertyPatchProductFamily,
			want:     []map[string]string{{"Name": "Windows"}},
		},
		{
			name:     "windows_msrc",
			os:       ssmtypes.OperatingSystemWindows,
			property: ssmtypes.PatchPropertyPatchMsrcSeverity,
			want:     []map[string]string{{"Name": "Critical"}, {"Name": "Important"}},
		},
		{
			name:     "ubuntu_priority",
			os:       ssmtypes.OperatingSystemUbuntu,
			property: ssmtypes.PatchPropertyPatchPriority,
			want:     []map[string]string{{"Name": "High"}},
		},
		{
			name:     "invalid_property_for_os",
			os:       ssmtypes.OperatingSystemUbuntu,
			property: ssmtypes.PatchPropertyPatchMsrcSeverity,
			want:     []map[string]string{},
		},
		{
			name:     "no_patches_for_os",
			os:       ssmtypes.OperatingSystemMacOS,
			property: ssmtypes.PatchPropertyProduct,
			want:     []map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			out, err := client.DescribePatchProperties(t.Context(), &ssmsdk.DescribePatchPropertiesInput{
				OperatingSystem: tt.os,
				Property:        tt.property,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.Properties)
		})
	}
}
