package cloudformation_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CreateStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		stackName   string
		template    string
		wantStatus  string
		wantDesc    string
		params      []cloudformation.Parameter
		tags        []cloudformation.Tag
		wantOutputs int
		wantTags    int
		setupDup    bool
		checkID     bool
	}{
		{
			name:       "success",
			stackName:  "my-stack",
			template:   simpleTemplate,
			wantStatus: "CREATE_COMPLETE",
			checkID:    true,
		},
		{
			name:      "already_exists",
			stackName: "dup-stack",
			template:  simpleTemplate,
			setupDup:  true,
			wantErr:   cloudformation.ErrStackAlreadyExists,
		},
		{
			name:       "invalid_template",
			stackName:  "bad-stack",
			template:   "{bad}",
			wantStatus: "CREATE_FAILED",
		},
		{
			name:      "with_params",
			stackName: "param-stack",
			template:  templateWithParams,
			params: []cloudformation.Parameter{
				{ParameterKey: "BucketName", ParameterValue: "test-bucket"},
			},
			wantStatus:  "CREATE_COMPLETE",
			wantOutputs: 1,
		},
		{
			name:       "with_tags",
			stackName:  "tagged-stack",
			template:   simpleTemplate,
			tags:       []cloudformation.Tag{{Key: "env", Value: "test"}},
			wantStatus: "CREATE_COMPLETE",
			wantTags:   1,
		},
		{
			name:       "yaml_template",
			stackName:  "yaml-stack",
			template:   yamlTemplate,
			wantStatus: "CREATE_COMPLETE",
			wantDesc:   "YAML template",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setupDup {
				_, err := b.CreateStack(t.Context(), tt.stackName, tt.template, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			}

			stack, err := b.CreateStack(
				t.Context(),
				tt.stackName,
				tt.template,
				tt.params,
				cloudformation.StackOptions{Tags: tt.tags},
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantStatus != "" {
				assert.Equal(t, tt.wantStatus, stack.StackStatus)
			}

			if tt.checkID {
				assert.Equal(t, tt.stackName, stack.StackName)
				assert.Contains(t, stack.StackID, tt.stackName)
			}

			if tt.wantDesc != "" {
				assert.Equal(t, tt.wantDesc, stack.Description)
			}

			if tt.wantOutputs > 0 {
				require.Len(t, stack.Outputs, tt.wantOutputs)
				assert.Equal(t, "test-bucket", stack.Outputs[0].OutputValue)
			}

			if tt.wantTags > 0 {
				require.Len(t, stack.Tags, tt.wantTags)
				assert.Equal(t, "env", stack.Tags[0].Key)
			}
		})
	}
}

func TestBackend_DescribeStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, b *cloudformation.InMemoryBackend)
		stackName string
		wantErr   error
		wantName  string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "desc-stack", simpleTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			stackName: "desc-stack",
			wantName:  "desc-stack",
		},
		{
			name:      "not_found",
			stackName: "nonexistent",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			stack, err := b.DescribeStack(tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantName, stack.StackName)
		})
	}
}

func TestBackend_UpdateStack(t *testing.T) {
	t.Parallel()

	// Templates used by the rollback test case.
	rollbackOriginal := `{"AWSTemplateFormatVersion":"2010-09-09",` +
		`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`
	rollbackUpdated := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{` +
		`"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}},` +
		`"NewQueue":{"Type":"AWS::SQS::Queue","Properties":{}}}}`

	// Templates used by the stale-resource deletion test case.
	withBucketAndQueue := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{` +
		`"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}},` +
		`"OldQueue":{"Type":"AWS::SQS::Queue","Properties":{}}}}`
	bucketOnly := `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{` +
		`"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`

	tests := []struct {
		name            string
		setup           func(t *testing.T, b *cloudformation.InMemoryBackend)
		updateTemplate  string
		stackName       string
		wantErr         error
		wantStatus      string
		wantResources   []string
		wantNoResources []string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "upd-stack", simpleTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			stackName:      "upd-stack",
			updateTemplate: simpleTemplate,
			wantStatus:     "UPDATE_COMPLETE",
		},
		{
			name:           "not_found",
			stackName:      "no-stack",
			updateTemplate: simpleTemplate,
			wantErr:        cloudformation.ErrStackNotFound,
		},
		{
			name: "rollback_on_creation_failure",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(
					t.Context(),
					"rollback-stack",
					rollbackOriginal,
					nil,
					cloudformation.StackOptions{},
				)
				require.NoError(t, err)
				// Inject a hook that fails when creating the new SQS queue.
				b.GetCreator().InjectCreateHook(func(resourceType string) error {
					if resourceType == "AWS::SQS::Queue" {
						return errSimulatedCreate
					}

					return nil
				})
			},
			stackName:       "rollback-stack",
			updateTemplate:  rollbackUpdated,
			wantStatus:      "UPDATE_ROLLBACK_COMPLETE",
			wantResources:   []string{"MyBucket"},
			wantNoResources: []string{"NewQueue"},
		},
		{
			name: "stale_resources_deleted",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(
					t.Context(),
					"stale-stack",
					withBucketAndQueue,
					nil,
					cloudformation.StackOptions{},
				)
				require.NoError(t, err)
			},
			stackName:       "stale-stack",
			updateTemplate:  bucketOnly,
			wantStatus:      "UPDATE_COMPLETE",
			wantResources:   []string{"MyBucket"},
			wantNoResources: []string{"OldQueue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			updated, err := b.UpdateStack(
				t.Context(),
				tt.stackName,
				tt.updateTemplate,
				nil,
				cloudformation.StackOptions{},
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, updated.StackStatus)

			if len(tt.wantResources) > 0 || len(tt.wantNoResources) > 0 {
				resources, listErr := b.ListStackResources(tt.stackName, "")
				require.NoError(t, listErr)

				logicalIDs := make([]string, 0, len(resources.Data))
				for _, r := range resources.Data {
					logicalIDs = append(logicalIDs, r.LogicalResourceID)
				}

				for _, want := range tt.wantResources {
					assert.Contains(t, logicalIDs, want)
				}

				for _, noWant := range tt.wantNoResources {
					assert.NotContains(t, logicalIDs, noWant)
				}
			}
		})
	}
}

func TestBackend_DeleteStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, b *cloudformation.InMemoryBackend)
		stackName  string
		wantErr    error
		wantStatus string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "del-stack", simpleTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			stackName:  "del-stack",
			wantStatus: "DELETE_COMPLETE",
		},
		{
			// DeleteStack is idempotent in real AWS: it has no modeled "stack
			// not found" error, so deleting a stack that never existed is a
			// silent no-op success, not an error.
			name:      "not_found_is_noop",
			stackName: "missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteStack(t.Context(), tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantStatus == "" {
				return
			}

			stack, err := b.DescribeStack(tt.stackName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, stack.StackStatus)
		})
	}
}

func TestBackend_DeleteStack_CleansInternalMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *cloudformation.InMemoryBackend) (stackID string)
		name  string
	}{
		{
			name: "cleans_resources",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) string {
				t.Helper()
				stack, err := b.CreateStack(
					t.Context(),
					"clean-stack",
					simpleTemplate,
					nil,
					cloudformation.StackOptions{},
				)
				require.NoError(t, err)

				return stack.StackID
			},
		},
		{
			name: "cleans_changeset_map",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) string {
				t.Helper()
				stack, err := b.CreateStack(
					t.Context(),
					"clean-cs-stack",
					simpleTemplate,
					nil,
					cloudformation.StackOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateChangeSet(t.Context(), "clean-cs-stack", "cs1", simpleTemplate, "", nil, nil)
				require.NoError(t, err)

				return stack.StackID
			},
		},
		{
			name: "cleans_drift_detections",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) string {
				t.Helper()
				stack, err := b.CreateStack(
					t.Context(),
					"clean-drift-stack",
					simpleTemplate,
					nil,
					cloudformation.StackOptions{},
				)
				require.NoError(t, err)
				_, err = b.DetectStackDrift("clean-drift-stack")
				require.NoError(t, err)
				_, err = b.DetectStackDrift("clean-drift-stack")
				require.NoError(t, err)

				return stack.StackID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			stackID := tt.setup(t, b)

			s, err := b.DescribeStack(stackID)
			require.NoError(t, err)
			stackName := s.StackName

			err = b.DeleteStack(t.Context(), stackID)
			require.NoError(t, err)

			assert.False(t, b.ResourcesEntryExists(stackID), "resources map entry should be removed")
			assert.False(t, b.ChangeSetsEntryExists(stackName), "changeSets map entry should be removed")
			assert.Equal(t, 0, b.DriftDetectionCount(stackID), "driftDetections should be pruned")
		})
	}
}

const twoResourceTemplate = `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{` +
	`"FirstBucket":{"Type":"AWS::S3::Bucket","Properties":{}},` +
	`"SecondQueue":{"Type":"AWS::SQS::Queue","Properties":{}}}}`

func TestBackend_CreateStack_RollbackOnProvisioningFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		template          string
		failResourceType  string
		wantStatus        string
		wantResourceCount int
	}{
		{
			name:              "only_resource_fails_no_partial_resources",
			template:          simpleTemplate,
			failResourceType:  "AWS::S3::Bucket",
			wantStatus:        "ROLLBACK_COMPLETE",
			wantResourceCount: 0,
		},
		{
			name:              "second_resource_fails_first_is_rolled_back",
			template:          twoResourceTemplate,
			failResourceType:  "AWS::SQS::Queue",
			wantStatus:        "ROLLBACK_COMPLETE",
			wantResourceCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			b.GetCreator().InjectCreateHook(func(resourceType string) error {
				if resourceType == tt.failResourceType {
					return errSimulatedCreate
				}

				return nil
			})

			stack, err := b.CreateStack(t.Context(), "fail-stack", tt.template, nil, cloudformation.StackOptions{})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, stack.StackStatus)
			assert.Equal(t, tt.wantResourceCount, b.ResourceCountForStack(stack.StackID))
		})
	}
}

func TestBackend_ListStacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  []string
		wantLen int
	}{
		{
			name:    "no_filter",
			filter:  nil,
			wantLen: 2,
		},
		{
			name:    "filter_create_complete",
			filter:  []string{"CREATE_COMPLETE"},
			wantLen: 2,
		},
		{
			name:    "filter_no_match",
			filter:  []string{"ROLLBACK_COMPLETE"},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStack(t.Context(), "list-s1", simpleTemplate, nil, cloudformation.StackOptions{})
			require.NoError(t, err)
			_, err = b.CreateStack(t.Context(), "list-s2", simpleTemplate, nil, cloudformation.StackOptions{})
			require.NoError(t, err)

			result, err := b.ListStacks(tt.filter, "")
			require.NoError(t, err)
			assert.Len(t, result.Data, tt.wantLen)
		})
	}
}

func TestBackend_DescribeStackEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(t *testing.T, b *cloudformation.InMemoryBackend)
		name      string
		stackName string
		wantEmpty bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "evt-stack", simpleTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			stackName: "evt-stack",
		},
		{
			name:      "not_found",
			stackName: "missing",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			evtPage, err := b.DescribeStackEvents(tt.stackName, "")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, evtPage.Data)
		})
	}
}

// TestBackend_DescribeStackEvents_Pagination verifies DescribeStackEvents
// honors NextToken instead of always returning the full event history —
// previously it ignored pagination entirely and returned every event in one
// response, unlike real AWS.
func TestBackend_DescribeStackEvents_Pagination(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "evt-page-stack", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	// Each successful UpdateStack on an unchanged plain resource emits 3
	// events (stack UPDATE_IN_PROGRESS, resource UPDATE_COMPLETE, stack
	// UPDATE_COMPLETE), so 40 updates comfortably exceeds one default page.
	for range 40 {
		_, uerr := b.UpdateStack(t.Context(), "evt-page-stack", simpleTemplate, nil, cloudformation.StackOptions{})
		require.NoError(t, uerr)
	}

	firstPage, err := b.DescribeStackEvents("evt-page-stack", "")
	require.NoError(t, err)
	require.NotEmpty(t, firstPage.Next, "expected more than one page of events")
	assert.Len(t, firstPage.Data, 100, "default page size")

	seen := make(map[string]bool, len(firstPage.Data))
	for _, e := range firstPage.Data {
		seen[e.EventID] = true
	}

	token := firstPage.Next
	var totalAfterFirst int

	for token != "" {
		next, nerr := b.DescribeStackEvents("evt-page-stack", token)
		require.NoError(t, nerr)

		for _, e := range next.Data {
			assert.False(t, seen[e.EventID], "event %s returned on more than one page", e.EventID)
			seen[e.EventID] = true
		}

		totalAfterFirst += len(next.Data)
		token = next.Next
	}

	assert.Positive(t, totalAfterFirst, "expected additional events beyond the first page")
}

func TestBackend_GetTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, b *cloudformation.InMemoryBackend)
		stackName string
		wantErr   error
		wantBody  string
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "tmpl-stack", simpleTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			stackName: "tmpl-stack",
			wantBody:  simpleTemplate,
		},
		{
			name:      "not_found",
			stackName: "no-stack",
			wantErr:   cloudformation.ErrStackNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			body, err := b.GetTemplate(tt.stackName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.JSONEq(t, tt.wantBody, body)
		})
	}
}

func TestBackend_ListAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   int
		wantLen int
	}{
		{
			name:    "two_stacks",
			count:   2,
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			for i := range tt.count {
				name := "stack-" + string(rune('a'+i))
				_, err := b.CreateStack(t.Context(), name, simpleTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			}

			all := b.ListAll()
			assert.Len(t, all, tt.wantLen)
		})
	}
}

// TestCreateStack_ErrorMapping verifies CreateStack distinguishes
// AlreadyExistsException from InsufficientCapabilitiesException rather than
// collapsing all errors to AlreadyExistsException.
func TestCreateStack_ErrorMapping(t *testing.T) {
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

// TestDescribeStacks_DisableRollbackAlwaysPresent verifies DisableRollback
// is always serialized (AWS returns it even when false), not dropped by omitempty.
func TestDescribeStacks_DisableRollbackAlwaysPresent(t *testing.T) {
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
