package cloudformation_test

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestParity_CreateChangeSet_NoChanges verifies an empty change set is marked
// FAILED / UNAVAILABLE so it cannot be executed (AWS behavior), while a change
// set that introduces resources is AVAILABLE.
func TestParity_CreateChangeSet_NoChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		template            string
		wantStatus          string
		wantExecutionStatus string
	}{
		{
			name:                "empty_template_no_changes",
			template:            "",
			wantStatus:          "FAILED",
			wantExecutionStatus: "UNAVAILABLE",
		},
		{
			name:                "template_with_resource_available",
			template:            simpleTemplate,
			wantStatus:          "CREATE_COMPLETE",
			wantExecutionStatus: "AVAILABLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			cs, err := b.CreateChangeSet(
				context.Background(),
				"stack-"+tt.name, "cs-"+tt.name, tt.template, "",
				nil,
			)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, cs.Status)
			assert.Equal(t, tt.wantExecutionStatus, cs.ExecutionStatus)
		})
	}
}

// TestParity_CreateStack_ErrorMapping verifies CreateStack distinguishes
// AlreadyExistsException from InsufficientCapabilitiesException rather than
// collapsing all errors to AlreadyExistsException.
func TestParity_CreateStack_ErrorMapping(t *testing.T) {
	t.Parallel()

	const iamTemplate = `{"AWSTemplateFormatVersion":"2010-09-09",` +
		`"Resources":{"R":{"Type":"AWS::IAM::Role","Properties":{}}}}`

	tests := []struct {
		name     string
		stack    string
		template string
		wantCode string
		seedDup  bool
	}{
		{
			name:     "duplicate_stack_already_exists",
			seedDup:  true,
			stack:    "dup-stack",
			template: simpleTemplate,
			wantCode: "AlreadyExistsException",
		},
		{
			name:     "missing_iam_capability",
			seedDup:  false,
			stack:    "iam-stack",
			template: iamTemplate,
			wantCode: "InsufficientCapabilitiesException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.seedDup {
				postFormValues(t, h, url.Values{
					"Action": {"CreateStack"}, "StackName": {tt.stack},
					"TemplateBody": {tt.template},
				})
			}

			resp := postFormValues(t, h, url.Values{
				"Action": {"CreateStack"}, "StackName": {tt.stack},
				"TemplateBody": {tt.template},
			})
			assert.Contains(t, resp.Body, tt.wantCode)
		})
	}
}

// TestParity_DescribeStacks_DisableRollbackAlwaysPresent verifies DisableRollback
// is always serialized (AWS returns it even when false), not dropped by omitempty.
func TestParity_DescribeStacks_DisableRollbackAlwaysPresent(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postFormValues(t, h, url.Values{
		"Action": {"CreateStack"}, "StackName": {"dr-stack"},
		"TemplateBody": {simpleTemplate},
	})

	resp := postFormValues(t, h, url.Values{
		"Action": {"DescribeStacks"}, "StackName": {"dr-stack"},
	})
	assert.Contains(t, resp.Body, "<DisableRollback>")
}

// TestParity_DynamicRef_ExactLimitNotError verifies a value with exactly the
// maximum number of dynamic references resolves successfully (off-by-one guard).
func TestParity_DynamicRef_ExactLimitNotError(t *testing.T) {
	t.Parallel()

	const maxRefs = 100

	tests := []struct {
		name    string
		count   int
		wantErr bool
	}{
		{name: "exactly_at_limit_ok", count: maxRefs, wantErr: false},
		{name: "over_limit_errors", count: maxRefs + 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			params := make(map[string]string, tt.count)

			var value strings.Builder
			for i := range tt.count {
				name := "p" + strconv.Itoa(i)
				params[name] = "v"
				value.WriteString("{{resolve:ssm:" + name + "}}")
			}

			tmplBody := `{"AWSTemplateFormatVersion":"2010-09-09",` +
				`"Resources":{"R":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":` +
				strconv.Quote(value.String()) + `}}}}`

			tmpl := mustParseTemplate(t, tmplBody)
			resolver := &stubResolver{params: params}

			err := cloudformation.ResolveDynamicRefsInTemplate(tmpl, resolver)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
