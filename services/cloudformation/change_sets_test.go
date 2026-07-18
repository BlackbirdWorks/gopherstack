package cloudformation_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_ChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(t *testing.T, b *cloudformation.InMemoryBackend)
		name      string
		stackName string
		csName    string
		template  string
		wantCS    bool
	}{
		{
			name: "create_describe_delete_workflow",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(t.Context(), "cs-stack", simpleTemplate, nil, cloudformation.StackOptions{})
				require.NoError(t, err)
			},
			stackName: "cs-stack",
			csName:    "my-cs",
			template:  modifiedTemplate,
			wantCS:    true,
		},
		{
			name: "already_exists",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateChangeSet(t.Context(), "cs-stack", "dup-cs", simpleTemplate, "", nil)
				require.NoError(t, err)
			},
			stackName: "cs-stack",
			csName:    "dup-cs",
			template:  simpleTemplate,
			wantErr:   cloudformation.ErrChangeSetExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			cs, err := b.CreateChangeSet(t.Context(), tt.stackName, tt.csName, tt.template, "desc", nil)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.csName, cs.ChangeSetName)
			assert.Equal(t, "CREATE_COMPLETE", cs.Status)

			got, err := b.DescribeChangeSet(tt.stackName, tt.csName)
			require.NoError(t, err)
			assert.Equal(t, cs.ChangeSetID, got.ChangeSetID)

			err = b.DeleteChangeSet(tt.stackName, tt.csName)
			require.NoError(t, err)

			_, err = b.DescribeChangeSet(tt.stackName, tt.csName)
			require.ErrorIs(t, err, cloudformation.ErrChangeSetNotFound)
		})
	}
}

func TestBackend_ExecuteChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, b *cloudformation.InMemoryBackend)
		stackName  string
		csName     string
		wantErr    error
		wantStatus string
	}{
		{
			name: "new_stack",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateChangeSet(t.Context(), "new-cs-stack", "exec-cs", simpleTemplate, "", nil)
				require.NoError(t, err)
			},
			stackName:  "new-cs-stack",
			csName:     "exec-cs",
			wantStatus: "CREATE_COMPLETE",
		},
		{
			// modifiedTemplate adds a resource relative to simpleTemplate, so
			// this change set carries a real Add change and is AVAILABLE.
			name: "existing_stack",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(
					t.Context(),
					"existing-stack",
					simpleTemplate,
					nil,
					cloudformation.StackOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateChangeSet(t.Context(), "existing-stack", "upd-cs", modifiedTemplate, "", nil)
				require.NoError(t, err)
			},
			stackName: "existing-stack",
			csName:    "upd-cs",
		},
		{
			// Re-submitting the identical template yields zero changes, so AWS
			// marks the change set FAILED/UNAVAILABLE and ExecuteChangeSet must
			// reject it with InvalidChangeSetStatus rather than silently
			// re-applying the (unchanged) template.
			name: "existing_stack_no_changes",
			setup: func(t *testing.T, b *cloudformation.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateStack(
					t.Context(),
					"nochange-stack",
					simpleTemplate,
					nil,
					cloudformation.StackOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateChangeSet(t.Context(), "nochange-stack", "noop-cs", simpleTemplate, "", nil)
				require.NoError(t, err)
			},
			stackName: "nochange-stack",
			csName:    "noop-cs",
			wantErr:   cloudformation.ErrChangeSetNotExecutable,
		},
		{
			name:      "not_found",
			stackName: "s",
			csName:    "missing-cs",
			wantErr:   cloudformation.ErrChangeSetNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.ExecuteChangeSet(t.Context(), tt.stackName, tt.csName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantStatus != "" {
				stack, descErr := b.DescribeStack(tt.stackName)
				require.NoError(t, descErr)
				assert.Equal(t, tt.wantStatus, stack.StackStatus)
			}
		})
	}
}

// templateWithTopic is simpleTemplate plus an SNS topic instead of the SQS
// queue modifiedTemplate adds, giving a second, independent real change.
const templateWithTopic = `{"AWSTemplateFormatVersion":"2010-09-09",` +
	`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}},` +
	`"MyTopic":{"Type":"AWS::SNS::Topic","Properties":{}}}}`

// TestBackend_ExecuteChangeSet_DeletesOtherChangeSets verifies AWS's documented
// behaviour: "When you execute a change set, CloudFormation deletes all other
// change sets associated with the stack because they aren't valid for the
// updated stack" — not just the one that was executed.
func TestBackend_ExecuteChangeSet_DeletesOtherChangeSets(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "multi-cs-stack", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	_, err = b.CreateChangeSet(t.Context(), "multi-cs-stack", "cs-a", modifiedTemplate, "", nil)
	require.NoError(t, err)
	_, err = b.CreateChangeSet(t.Context(), "multi-cs-stack", "cs-b", templateWithTopic, "", nil)
	require.NoError(t, err)

	list, err := b.ListChangeSets("multi-cs-stack", "")
	require.NoError(t, err)
	require.Len(t, list.Data, 2)

	require.NoError(t, b.ExecuteChangeSet(t.Context(), "multi-cs-stack", "cs-a"))

	_, err = b.DescribeChangeSet("multi-cs-stack", "cs-a")
	require.ErrorIs(t, err, cloudformation.ErrChangeSetNotFound, "executed change set must be gone")

	_, err = b.DescribeChangeSet("multi-cs-stack", "cs-b")
	require.ErrorIs(t, err, cloudformation.ErrChangeSetNotFound,
		"sibling change sets must also be discarded — they no longer apply to the updated stack")

	list, err = b.ListChangeSets("multi-cs-stack", "")
	require.NoError(t, err)
	assert.Empty(t, list.Data)
}

func TestBackend_ListChangeSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stackName string
		csNames   []string
		wantLen   int
	}{
		{
			name:      "two_changesets",
			stackName: "list-cs-stack",
			csNames:   []string{"cs1", "cs2"},
			wantLen:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			for _, cs := range tt.csNames {
				_, err := b.CreateChangeSet(t.Context(), tt.stackName, cs, simpleTemplate, "", nil)
				require.NoError(t, err)
			}

			summaries, err := b.ListChangeSets(tt.stackName, "")
			require.NoError(t, err)
			assert.Len(t, summaries.Data, tt.wantLen)
		})
	}
}

func TestBackend_DeleteChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		stackName string
		csName    string
	}{
		{
			name:      "not_found",
			stackName: "no-stack",
			csName:    "no-cs",
			wantErr:   cloudformation.ErrChangeSetNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			err := b.DeleteChangeSet(tt.stackName, tt.csName)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestCreateChangeSet_NoChanges verifies an empty change set is marked
// FAILED / UNAVAILABLE so it cannot be executed (AWS behavior), while a change
// set that introduces resources is AVAILABLE.
func TestCreateChangeSet_NoChanges(t *testing.T) {
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

// changeByLogicalID indexes a change set's resource changes by logical ID.
func changeByLogicalID(cs *cloudformation.ChangeSet) map[string]cloudformation.ResourceChange {
	out := make(map[string]cloudformation.ResourceChange, len(cs.Changes))
	for _, ch := range cs.Changes {
		out[ch.ResourceChange.LogicalID] = ch.ResourceChange
	}

	return out
}

func TestComputeChanges_Actions(t *testing.T) {
	t.Parallel()

	const base = `{"Resources":{
		"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig"}},
		"Table":{"Type":"AWS::DynamoDB::Table","Properties":{"TableName":"t1"}}
	}}`

	tests := []struct {
		// want maps logicalID → expected action ("" = must be absent).
		want            map[string]string
		wantReplacement map[string]string
		wantScope       map[string][]string
		name            string
		newTemplate     string
	}{
		{
			name: "add new resource only",
			newTemplate: `{"Resources":{
				"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig"}},
				"Table":{"Type":"AWS::DynamoDB::Table","Properties":{"TableName":"t1"}},
				"Queue":{"Type":"AWS::SQS::Queue","Properties":{}}
			}}`,
			want: map[string]string{"Queue": "Add", "Bucket": "", "Table": ""},
		},
		{
			name: "remove dropped resource",
			newTemplate: `{"Resources":{
				"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig"}}
			}}`,
			want: map[string]string{"Table": "Remove", "Bucket": ""},
		},
		{
			name:        "identical template yields no changes",
			newTemplate: base,
			want:        map[string]string{"Bucket": "", "Table": ""},
		},
		{
			name: "replacing property change (BucketName)",
			newTemplate: `{"Resources":{
				"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"renamed"}},
				"Table":{"Type":"AWS::DynamoDB::Table","Properties":{"TableName":"t1"}}
			}}`,
			want:            map[string]string{"Bucket": "Modify", "Table": ""},
			wantReplacement: map[string]string{"Bucket": "True"},
			wantScope:       map[string][]string{"Bucket": {"Properties"}},
		},
		{
			name: "non-replacing tags change",
			newTemplate: `{"Resources":{
				"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig","Tags":[{"Key":"a","Value":"b"}]}},
				"Table":{"Type":"AWS::DynamoDB::Table","Properties":{"TableName":"t1"}}
			}}`,
			want:            map[string]string{"Bucket": "Modify", "Table": ""},
			wantReplacement: map[string]string{"Bucket": "False"},
			wantScope:       map[string][]string{"Bucket": {"Tags"}},
		},
		{
			name: "type change forces replacement",
			newTemplate: `{"Resources":{
				"Bucket":{"Type":"AWS::SQS::Queue","Properties":{}},
				"Table":{"Type":"AWS::DynamoDB::Table","Properties":{"TableName":"t1"}}
			}}`,
			want:            map[string]string{"Bucket": "Modify"},
			wantReplacement: map[string]string{"Bucket": "True"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStack(t.Context(), "cs-actions", base, nil, cloudformation.StackOptions{})
			require.NoError(t, err)

			cs, err := b.CreateChangeSet(t.Context(), "cs-actions", "cs1", tc.newTemplate, "d", nil)
			require.NoError(t, err)

			idx := changeByLogicalID(cs)
			for id, wantAction := range tc.want {
				rc, present := idx[id]
				if wantAction == "" {
					assert.False(t, present, "resource %s must not appear (unchanged)", id)

					continue
				}
				require.True(t, present, "resource %s must appear as %s", id, wantAction)
				assert.Equal(t, wantAction, rc.Action, "action for %s", id)
			}
			for id, wantRepl := range tc.wantReplacement {
				assert.Equal(t, wantRepl, idx[id].Replacement, "replacement for %s", id)
			}
			for id, wantScope := range tc.wantScope {
				assert.ElementsMatch(t, wantScope, idx[id].Scope, "scope for %s", id)
			}
		})
	}
}

func TestComputeChanges_RemoveCarriesPhysicalID(t *testing.T) {
	t.Parallel()

	const base = `{"Resources":{
		"Bucket":{"Type":"AWS::S3::Bucket","Properties":{}},
		"Queue":{"Type":"AWS::SQS::Queue","Properties":{}}
	}}`
	const dropped = `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "cs-remove", base, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	cs, err := b.CreateChangeSet(t.Context(), "cs-remove", "cs1", dropped, "d", nil)
	require.NoError(t, err)

	idx := changeByLogicalID(cs)
	rc, ok := idx["Queue"]
	require.True(t, ok)
	assert.Equal(t, "Remove", rc.Action)
	assert.NotEmpty(t, rc.PhysicalID, "Remove change must report the physical ID")
}

func TestComputeChanges_ModifyDetails(t *testing.T) {
	t.Parallel()

	const base = `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"orig"}}}}`
	const renamed = `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"new"}}}}`

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "cs-details", base, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	cs, err := b.CreateChangeSet(t.Context(), "cs-details", "cs1", renamed, "d", nil)
	require.NoError(t, err)

	idx := changeByLogicalID(cs)
	rc := idx["Bucket"]
	require.Len(t, rc.Details, 1)
	require.NotNil(t, rc.Details[0].Target)
	assert.Equal(t, "BucketName", rc.Details[0].Target.Name)
	assert.Equal(t, "Always", rc.Details[0].Target.RequiresRecreation)
	assert.Equal(t, "Properties", rc.Details[0].Target.Attribute)
	assert.Equal(t, "Static", rc.Details[0].Evaluation)
}

func TestCreateChangeSet_NoChangesUnavailable(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "cs-noop", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	cs, err := b.CreateChangeSet(t.Context(), "cs-noop", "cs1", simpleTemplate, "d", nil)
	require.NoError(t, err)
	assert.Empty(t, cs.Changes)
	assert.Equal(t, "FAILED", cs.Status)
	assert.Equal(t, "UNAVAILABLE", cs.ExecutionStatus)
}
