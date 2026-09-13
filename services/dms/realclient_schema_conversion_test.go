package dms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_SchemaConversion drives every gopherstack-n3zi uncovered
// dms op (the entire schema-conversion metadata-model family) through the
// real aws-sdk-go-v2 client.
func TestRealClient_SchemaConversion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "assessment_lifecycle", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			ctx := t.Context()

			preOut, err := client.DescribeMetadataModelAssessments(
				ctx,
				&databasemigrationservice.DescribeMetadataModelAssessmentsInput{
					MigrationProjectIdentifier: aws.String("proj-assess"),
				},
			)
			require.NoError(t, err)
			assert.Empty(t, preOut.Requests)

			startOut, err := client.StartMetadataModelAssessment(
				ctx,
				&databasemigrationservice.StartMetadataModelAssessmentInput{
					MigrationProjectIdentifier: aws.String("proj-assess"),
					SelectionRules:             aws.String(`{"rules":[]}`),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, startOut.RequestIdentifier)

			postOut, err := client.DescribeMetadataModelAssessments(
				ctx,
				&databasemigrationservice.DescribeMetadataModelAssessmentsInput{
					MigrationProjectIdentifier: aws.String("proj-assess"),
				},
			)
			require.NoError(t, err)
			require.Len(t, postOut.Requests, 1)
			assert.Equal(
				t,
				aws.ToString(startOut.RequestIdentifier),
				aws.ToString(postOut.Requests[0].RequestIdentifier),
			)
		}},
		{name: "conversion_lifecycle_with_cancel", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			ctx := t.Context()

			startOut, err := client.StartMetadataModelConversion(
				ctx,
				&databasemigrationservice.StartMetadataModelConversionInput{
					MigrationProjectIdentifier: aws.String("proj-conv"),
					SelectionRules:             aws.String(`{"rules":[]}`),
				},
			)
			require.NoError(t, err)

			descOut, err := client.DescribeMetadataModelConversions(
				ctx,
				&databasemigrationservice.DescribeMetadataModelConversionsInput{
					MigrationProjectIdentifier: aws.String("proj-conv"),
				},
			)
			require.NoError(t, err)
			require.Len(t, descOut.Requests, 1)

			cancelOut, err := client.CancelMetadataModelConversion(
				ctx,
				&databasemigrationservice.CancelMetadataModelConversionInput{
					MigrationProjectIdentifier: aws.String("proj-conv"),
					RequestIdentifier:          startOut.RequestIdentifier,
				},
			)
			require.NoError(t, err)
			require.NotNil(t, cancelOut.Request)
			assert.Equal(t, aws.ToString(startOut.RequestIdentifier), aws.ToString(cancelOut.Request.RequestIdentifier))
		}},
		{name: "creation_lifecycle_with_cancel", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			ctx := t.Context()

			startOut, err := client.StartMetadataModelCreation(
				ctx,
				&databasemigrationservice.StartMetadataModelCreationInput{
					MigrationProjectIdentifier: aws.String("proj-create"),
					MetadataModelName:          aws.String("my-model"),
					SelectionRules:             aws.String(`{"rules":[]}`),
					Properties: &types.MetadataModelPropertiesMemberStatementProperties{
						Value: types.StatementProperties{Definition: aws.String("SELECT 1")},
					},
				},
			)
			require.NoError(t, err)

			descOut, err := client.DescribeMetadataModelCreations(
				ctx,
				&databasemigrationservice.DescribeMetadataModelCreationsInput{
					MigrationProjectIdentifier: aws.String("proj-create"),
				},
			)
			require.NoError(t, err)
			require.Len(t, descOut.Requests, 1)

			cancelOut, err := client.CancelMetadataModelCreation(
				ctx,
				&databasemigrationservice.CancelMetadataModelCreationInput{
					MigrationProjectIdentifier: aws.String("proj-create"),
					RequestIdentifier:          startOut.RequestIdentifier,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(startOut.RequestIdentifier), aws.ToString(cancelOut.Request.RequestIdentifier))
		}},
		{name: "export_as_script_lifecycle", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			ctx := t.Context()

			startOut, err := client.StartMetadataModelExportAsScript(
				ctx,
				&databasemigrationservice.StartMetadataModelExportAsScriptInput{
					MigrationProjectIdentifier: aws.String("proj-export-script"),
					Origin:                     types.OriginTypeValueSource,
					SelectionRules:             aws.String(`{"rules":[]}`),
				},
			)
			require.NoError(t, err)

			descOut, err := client.DescribeMetadataModelExportsAsScript(
				ctx,
				&databasemigrationservice.DescribeMetadataModelExportsAsScriptInput{
					MigrationProjectIdentifier: aws.String("proj-export-script"),
				},
			)
			require.NoError(t, err)
			require.Len(t, descOut.Requests, 1)
			assert.Equal(
				t,
				aws.ToString(startOut.RequestIdentifier),
				aws.ToString(descOut.Requests[0].RequestIdentifier),
			)
		}},
		{name: "export_to_target_lifecycle", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			ctx := t.Context()

			startOut, err := client.StartMetadataModelExportToTarget(
				ctx,
				&databasemigrationservice.StartMetadataModelExportToTargetInput{
					MigrationProjectIdentifier: aws.String("proj-export-target"),
					SelectionRules:             aws.String(`{"rules":[]}`),
				},
			)
			require.NoError(t, err)

			descOut, err := client.DescribeMetadataModelExportsToTarget(
				ctx,
				&databasemigrationservice.DescribeMetadataModelExportsToTargetInput{
					MigrationProjectIdentifier: aws.String("proj-export-target"),
				},
			)
			require.NoError(t, err)
			require.Len(t, descOut.Requests, 1)
			assert.Equal(
				t,
				aws.ToString(startOut.RequestIdentifier),
				aws.ToString(descOut.Requests[0].RequestIdentifier),
			)
		}},
		{name: "import_lifecycle", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			ctx := t.Context()

			startOut, err := client.StartMetadataModelImport(
				ctx,
				&databasemigrationservice.StartMetadataModelImportInput{
					MigrationProjectIdentifier: aws.String("proj-import"),
					Origin:                     types.OriginTypeValueSource,
					SelectionRules:             aws.String(`{"rules":[]}`),
					Refresh:                    true,
				},
			)
			require.NoError(t, err)

			descOut, err := client.DescribeMetadataModelImports(
				ctx,
				&databasemigrationservice.DescribeMetadataModelImportsInput{
					MigrationProjectIdentifier: aws.String("proj-import"),
				},
			)
			require.NoError(t, err)
			require.Len(t, descOut.Requests, 1)
			assert.Equal(
				t,
				aws.ToString(startOut.RequestIdentifier),
				aws.ToString(descOut.Requests[0].RequestIdentifier),
			)
		}},
		{name: "extension_pack_association", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			ctx := t.Context()

			startOut, err := client.StartExtensionPackAssociation(
				ctx,
				&databasemigrationservice.StartExtensionPackAssociationInput{
					MigrationProjectIdentifier: aws.String("proj-ext"),
				},
			)
			require.NoError(t, err)

			descOut, err := client.DescribeExtensionPackAssociations(
				ctx,
				&databasemigrationservice.DescribeExtensionPackAssociationsInput{
					MigrationProjectIdentifier: aws.String("proj-ext"),
				},
			)
			require.NoError(t, err)
			require.Len(t, descOut.Requests, 1)
			assert.Equal(
				t,
				aws.ToString(startOut.RequestIdentifier),
				aws.ToString(descOut.Requests[0].RequestIdentifier),
			)
		}},
		{name: "conversion_configuration", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			ctx := t.Context()

			modOut, err := client.ModifyConversionConfiguration(
				ctx,
				&databasemigrationservice.ModifyConversionConfigurationInput{
					MigrationProjectIdentifier: aws.String("proj-cfg"),
					ConversionConfiguration:    aws.String(`{"key":"value"}`),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "proj-cfg", aws.ToString(modOut.MigrationProjectIdentifier))

			descOut, err := client.DescribeConversionConfiguration(
				ctx,
				&databasemigrationservice.DescribeConversionConfigurationInput{
					MigrationProjectIdentifier: aws.String("proj-cfg"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "proj-cfg", aws.ToString(descOut.MigrationProjectIdentifier))
		}},
		{name: "metadata_model_reads", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)
			ctx := t.Context()

			descOut, err := client.DescribeMetadataModel(ctx, &databasemigrationservice.DescribeMetadataModelInput{
				MigrationProjectIdentifier: aws.String("proj-model"),
				Origin:                     types.OriginTypeValueSource,
				SelectionRules:             aws.String(`{"rules":[]}`),
			})
			require.NoError(t, err)
			assert.Empty(t, descOut.TargetMetadataModels)

			childOut, err := client.DescribeMetadataModelChildren(
				ctx,
				&databasemigrationservice.DescribeMetadataModelChildrenInput{
					MigrationProjectIdentifier: aws.String("proj-model"),
					Origin:                     types.OriginTypeValueSource,
					SelectionRules:             aws.String(`{"rules":[]}`),
				},
			)
			require.NoError(t, err)
			assert.Empty(t, childOut.MetadataModelChildren)

			rulesOut, err := client.GetTargetSelectionRules(ctx, &databasemigrationservice.GetTargetSelectionRulesInput{
				MigrationProjectIdentifier: aws.String("proj-model"),
				SelectionRules:             aws.String(`{"rules":["x"]}`),
			})
			require.NoError(t, err)
			assert.JSONEq(t, `{"rules":["x"]}`, aws.ToString(rulesOut.TargetSelectionRules))

			exportOut, err := client.ExportMetadataModelAssessment(
				ctx,
				&databasemigrationservice.ExportMetadataModelAssessmentInput{
					MigrationProjectIdentifier: aws.String("proj-model"),
					SelectionRules:             aws.String(`{"rules":[]}`),
				},
			)
			require.NoError(t, err)
			assert.Nil(t, exportOut.CsvReport)
			assert.Nil(t, exportOut.PdfReport)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
