package cloudformation_test

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- Backend: Exports (ListExports, ListImports) ----------------------------

const exportTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {}}
	},
	"Outputs": {
		"BucketName": {
			"Value": {"Ref": "MyBucket"},
			"Export": {"Name": "shared-bucket"}
		}
	}
}`

const importTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"MyTopic": {"Type": "AWS::SNS::Topic", "Properties": {}}
	},
	"Outputs": {
		"ImportedBucket": {
			"Value": {"Fn::ImportValue": "shared-bucket"}
		}
	}
}`

func TestBackend_ListExports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stacks    []string
		wantNames []string
	}{
		{
			name:      "no_exports",
			stacks:    nil,
			wantNames: nil,
		},
		{
			name:      "single_export",
			stacks:    []string{exportTemplate},
			wantNames: []string{"shared-bucket"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			for i, tmpl := range tt.stacks {
				stackName := fmt.Sprintf("%s-stack-%d", tt.name, i)
				_, err := b.CreateStack(t.Context(), stackName, tmpl, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			}

			p, err := b.ListExports("")
			require.NoError(t, err)

			names := make([]string, 0, len(p.Data))
			for _, exp := range p.Data {
				names = append(names, exp.Name)
			}

			if len(tt.wantNames) == 0 {
				assert.Empty(t, names)
			} else {
				assert.Equal(t, tt.wantNames, names)
			}
		})
	}
}

func TestBackend_ExportsRemovedOnDelete(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "export-stack", exportTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	p, err := b.ListExports("")
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "shared-bucket", p.Data[0].Name)

	err = b.DeleteStack(t.Context(), "export-stack")
	require.NoError(t, err)

	p2, err := b.ListExports("")
	require.NoError(t, err)
	assert.Empty(t, p2.Data)
}

// noImportTemplate is importTemplate with the Fn::ImportValue reference
// removed, used to simulate the importing stack being updated away from the
// export before the exporting stack is deleted, and as an update target for
// the exporting stack itself (one that no longer produces any Outputs).
const noImportTemplate = `{
	"AWSTemplateFormatVersion": "2010-09-09",
	"Resources": {
		"MyTopic": {"Type": "AWS::SNS::Topic", "Properties": {}}
	}
}`

// TestBackend_DeleteStack_BlockedByImportedExport verifies AWS's real
// behaviour: a stack cannot be deleted while another active stack still
// imports one of its exports ("Export X cannot be deleted as it is in use by
// Y"). The block is lifted once the importing stack no longer references the
// export (deleted or updated away).
func TestBackend_DeleteStack_BlockedByImportedExport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		arrange func(t *testing.T, b *cloudformation.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "blocked_while_importer_active",
			arrange: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "importer", importTemplate, nil,
					cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			wantErr: true,
		},
		{
			name: "allowed_after_importer_deleted",
			arrange: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "importer", importTemplate, nil,
					cloudformation.StackOptions{})
				require.NoError(t, err)
				require.NoError(t, b.DeleteStack(t.Context(), "importer"))
			},
			wantErr: false,
		},
		{
			name:    "allowed_with_no_importer",
			arrange: func(t *testing.T, _ *cloudformation.InMemoryBackend) { t.Helper() },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStack(t.Context(), "exporter", exportTemplate, nil, cloudformation.StackOptions{})
			require.NoError(t, err)

			tt.arrange(t, b)

			err = b.DeleteStack(t.Context(), "exporter")
			if tt.wantErr {
				require.ErrorIs(t, err, cloudformation.ErrExportInUse)
				assert.Contains(t, err.Error(), "shared-bucket")
				assert.Contains(t, err.Error(), "importer")

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestBackend_UpdateStack_BlockedByImportedExportRemoval verifies that
// updating a stack's template in a way that would drop an export still
// imported by another active stack is rejected before any resource is
// touched, matching AWS's export-in-use protection for updates as well as
// deletes.
func TestBackend_UpdateStack_BlockedByImportedExportRemoval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		arrange     func(t *testing.T, b *cloudformation.InMemoryBackend)
		name        string
		newTemplate string
		wantErr     bool
	}{
		{
			name: "blocked_when_importer_active",
			arrange: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "importer", importTemplate, nil,
					cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			newTemplate: noImportTemplate,
			wantErr:     true,
		},
		{
			name: "allowed_after_importer_no_longer_references_export",
			arrange: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "importer", importTemplate, nil,
					cloudformation.StackOptions{})
				require.NoError(t, err)
				require.NoError(t, b.DeleteStack(t.Context(), "importer"))
			},
			newTemplate: noImportTemplate,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStack(t.Context(), "exporter", exportTemplate, nil, cloudformation.StackOptions{})
			require.NoError(t, err)

			tt.arrange(t, b)

			_, err = b.UpdateStack(t.Context(), "exporter", tt.newTemplate, nil, cloudformation.StackOptions{})
			require.NoError(t, err) // UpdateStack itself never errors; failures surface via StackStatus.

			stack, descErr := b.DescribeStack("exporter")
			require.NoError(t, descErr)

			if tt.wantErr {
				assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", stack.StackStatus)
				assert.Contains(t, stack.StackStatusReason, "shared-bucket")

				p, expErr := b.ListExports("")
				require.NoError(t, expErr)
				assert.Len(t, p.Data, 1, "export must survive a blocked update")

				return
			}

			assert.Equal(t, "UPDATE_COMPLETE", stack.StackStatus)
			p, expErr := b.ListExports("")
			require.NoError(t, expErr)
			assert.Empty(t, p.Data, "export should be gone once no longer produced by the template")
		})
	}
}

func TestBackend_ListImports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		exportName string
		setup      func(*cloudformation.InMemoryBackend)
		wantErr    error
		wantStacks []string
	}{
		{
			name:       "export_not_found",
			exportName: "does-not-exist",
			setup:      func(_ *cloudformation.InMemoryBackend) {},
			wantErr:    cloudformation.ErrExportNotFound,
		},
		{
			name:       "no_importers",
			exportName: "shared-bucket",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, err := b.CreateStack(t.Context(), "exporter", exportTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			wantStacks: nil,
		},
		{
			name:       "one_importer",
			exportName: "shared-bucket",
			setup: func(b *cloudformation.InMemoryBackend) {
				_, err := b.CreateStack(t.Context(), "exporter", exportTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
				_, err = b.CreateStack(t.Context(), "importer", importTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			wantStacks: []string{"importer"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			p, err := b.ListImports(tt.exportName, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantStacks == nil {
				assert.Empty(t, p.Data)
			} else {
				assert.Equal(t, tt.wantStacks, p.Data)
			}
		})
	}
}

func TestBackend_ExportOutput_IncludesExportName(t *testing.T) {
	t.Parallel()

	b := newBackend()
	stack, err := b.CreateStack(t.Context(), "export-stack", exportTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	require.NotNil(t, stack)

	require.Len(t, stack.Outputs, 1)
	assert.Equal(t, "BucketName", stack.Outputs[0].OutputKey)
	assert.Equal(t, "shared-bucket", stack.Outputs[0].ExportName)
}

func TestBackend_DuplicateExportFails(t *testing.T) {
	t.Parallel()

	b := newBackend()

	// First stack creates the export.
	stack1, err := b.CreateStack(t.Context(), "first-stack", exportTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack1.StackStatus)

	// Second stack tries to create the same export name — should fail.
	stack2, err := b.CreateStack(t.Context(), "second-stack", exportTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE_FAILED", stack2.StackStatus)
	assert.Contains(t, stack2.StackStatusReason, "shared-bucket")
}

func TestBackend_UnresolvedImportFails(t *testing.T) {
	t.Parallel()

	b := newBackend()

	// Try to create a stack that imports a non-existent export.
	stack, err := b.CreateStack(t.Context(), "importer-stack", importTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)
	// AWS rolls back to ROLLBACK_COMPLETE when pre-flight import validation fails.
	assert.Equal(t, "ROLLBACK_COMPLETE", stack.StackStatus)
	assert.Contains(t, stack.StackStatusReason, "shared-bucket")
}

// ---- Handler: ListExports ---------------------------------------------------

func TestHandler_ListExports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudformation.Handler)
		name       string
		wantElem   string
		wantExport string
		wantCode   int
	}{
		{
			name:     "no_exports",
			setup:    func(_ *cloudformation.Handler) {},
			wantCode: 200,
			wantElem: "ListExportsResult",
		},
		{
			name: "with_export",
			setup: func(h *cloudformation.Handler) {
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"export-stack"},
					"TemplateBody": {exportTemplate},
				}.Encode())
			},
			wantCode:   200,
			wantElem:   "ListExportsResult",
			wantExport: "shared-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			tt.setup(h)

			rec := postForm(t, h, url.Values{
				"Action": {"ListExports"},
			}.Encode())

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)

			if tt.wantExport != "" {
				assert.Contains(t, rec.Body.String(), tt.wantExport)
			}
		})
	}
}

// ---- Handler: ListImports ---------------------------------------------------

func TestHandler_ListImports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*cloudformation.Handler)
		name       string
		exportName string
		wantElem   string
		wantCode   int
	}{
		{
			name:       "missing_export_name",
			setup:      func(_ *cloudformation.Handler) {},
			exportName: "",
			wantCode:   400,
			wantElem:   "ErrorResponse",
		},
		{
			name: "export_not_found",
			setup: func(_ *cloudformation.Handler) {
				// no stacks created
			},
			exportName: "nonexistent-export",
			wantCode:   400,
			wantElem:   "ErrorResponse",
		},
		{
			name: "found_importers",
			setup: func(h *cloudformation.Handler) {
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"exporter"},
					"TemplateBody": {exportTemplate},
				}.Encode())
				postForm(t, h, url.Values{
					"Action":       {"CreateStack"},
					"StackName":    {"importer"},
					"TemplateBody": {importTemplate},
				}.Encode())
			},
			exportName: "shared-bucket",
			wantCode:   200,
			wantElem:   "ListImportsResult",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			tt.setup(h)

			var formBody string
			if tt.exportName == "" {
				formBody = url.Values{
					"Action": {"ListImports"},
				}.Encode()
			} else {
				formBody = url.Values{
					"Action":     {"ListImports"},
					"ExportName": {tt.exportName},
				}.Encode()
			}

			rec := postForm(t, h, formBody)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantElem)
		})
	}
}

// ---- Handler: DescribeStacks shows ExportName in outputs -------------------

func TestHandler_DescribeStacks_ExportName(t *testing.T) {
	t.Parallel()

	h := newHandler()

	postForm(t, h, url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"export-ds-stack"},
		"TemplateBody": {exportTemplate},
	}.Encode())

	rec := postForm(t, h, url.Values{
		"Action":    {"DescribeStacks"},
		"StackName": {"export-ds-stack"},
	}.Encode())

	require.Equal(t, 200, rec.Code)

	type outputXML struct {
		OutputKey   string `xml:"OutputKey"`
		OutputValue string `xml:"OutputValue"`
		ExportName  string `xml:"ExportName"`
	}
	type stackXML struct {
		Outputs []outputXML `xml:"Outputs>member"`
	}
	type result struct {
		Stacks []stackXML `xml:"Stacks>member"`
	}
	type resp struct {
		XMLName xml.Name `xml:"DescribeStacksResponse"`
		Result  result   `xml:"DescribeStacksResult"`
	}

	var decoded resp
	require.NoError(t, xml.NewDecoder(strings.NewReader(rec.Body.String())).Decode(&decoded))
	require.Len(t, decoded.Result.Stacks, 1)
	require.Len(t, decoded.Result.Stacks[0].Outputs, 1)
	assert.Equal(t, "shared-bucket", decoded.Result.Stacks[0].Outputs[0].ExportName)
}
