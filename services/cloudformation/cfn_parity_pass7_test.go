package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// changeByLogicalID indexes a change set's resource changes by logical ID.
func changeByLogicalID(cs *cloudformation.ChangeSet) map[string]cloudformation.ResourceChange {
	out := make(map[string]cloudformation.ResourceChange, len(cs.Changes))
	for _, ch := range cs.Changes {
		out[ch.ResourceChange.LogicalID] = ch.ResourceChange
	}

	return out
}

// ---- Change sets: Add / Remove / Modify / no-change / Replacement ------------

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

// ---- Drift: property-aware differences ---------------------------------------

func TestDrift_PropertyDifferences(t *testing.T) {
	t.Parallel()

	const template = `{"Resources":{
		"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"b","AccessControl":"Private"}}
	}}`

	tests := []struct {
		live       map[string]any
		name       string
		wantStatus string
		wantPath   string
		wantType   string
	}{
		{
			name:       "in sync",
			live:       map[string]any{"BucketName": "b", "AccessControl": "Private"},
			wantStatus: "IN_SYNC",
		},
		{
			name: "added property",
			live: map[string]any{
				"BucketName":              "b",
				"AccessControl":           "Private",
				"VersioningConfiguration": map[string]any{"Status": "Enabled"},
			},
			wantStatus: "MODIFIED",
			wantPath:   "/VersioningConfiguration",
			wantType:   "ADD",
		},
		{
			name:       "removed property",
			live:       map[string]any{"BucketName": "b"},
			wantStatus: "MODIFIED",
			wantPath:   "/AccessControl",
			wantType:   "REMOVE",
		},
		{
			name:       "changed value",
			live:       map[string]any{"BucketName": "b", "AccessControl": "PublicRead"},
			wantStatus: "MODIFIED",
			wantPath:   "/AccessControl",
			wantType:   "NOT_EQUAL",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStack(t.Context(), "drift-prop", template, nil, cloudformation.StackOptions{})
			require.NoError(t, err)

			require.NoError(t, b.RecordResourceMutation("drift-prop", "Bucket", tc.live))

			_, err = b.DetectStackDrift("drift-prop")
			require.NoError(t, err)

			drifts, err := b.DescribeStackResourceDrifts("drift-prop")
			require.NoError(t, err)
			require.Len(t, drifts, 1)

			d := drifts[0]
			assert.Equal(t, tc.wantStatus, d.StackResourceDriftStatus)

			if tc.wantStatus == "IN_SYNC" {
				assert.Empty(t, d.PropertyDifferences)

				return
			}

			require.NotEmpty(t, d.PropertyDifferences)
			var found bool
			for _, pd := range d.PropertyDifferences {
				if pd.PropertyPath == tc.wantPath {
					found = true
					assert.Equal(t, tc.wantType, pd.DifferenceType)
				}
			}
			assert.True(t, found, "expected a difference at %s", tc.wantPath)
			assert.NotEmpty(t, d.ExpectedProperties)
			assert.NotEmpty(t, d.ActualProperties)
		})
	}
}

func TestRecordResourceMutation_Errors(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStack(t.Context(), "rrm", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	tests := []struct {
		wantErr   error
		name      string
		stack     string
		logicalID string
	}{
		{name: "unknown stack", stack: "nope", logicalID: "MyBucket", wantErr: cloudformation.ErrStackNotFound},
		{name: "unknown resource", stack: "rrm", logicalID: "Ghost", wantErr: cloudformation.ErrResourceNotFound},
		{name: "ok", stack: "rrm", logicalID: "MyBucket", wantErr: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			merr := b.RecordResourceMutation(tc.stack, tc.logicalID, map[string]any{"X": "y"})
			if tc.wantErr != nil {
				require.ErrorIs(t, merr, tc.wantErr)

				return
			}
			require.NoError(t, merr)
		})
	}
}

// ---- Fn::ForEach expansion ---------------------------------------------------

func TestParseTemplate_FnForEach(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		template     string
		wantResource []string
		wantAbsent   []string
	}{
		{
			name: "list literal expansion",
			template: `{"Resources":{
				"Fn::ForEach::Topics":[
					"Name",
					["Alpha","Beta","Gamma"],
					{"Topic${Name}":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"${Name}"}}}
				]
			}}`,
			wantResource: []string{"TopicAlpha", "TopicBeta", "TopicGamma"},
		},
		{
			name: "loop coexists with static resource",
			template: `{"Resources":{
				"MainBucket":{"Type":"AWS::S3::Bucket","Properties":{}},
				"Fn::ForEach::Queues":[
					"Q",
					["one","two"],
					{"Queue${Q}":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"${Q}"}}}
				]
			}}`,
			wantResource: []string{"MainBucket", "Queueone", "Queuetwo"},
		},
		{
			name: "comma-delimited string collection",
			template: `{"Resources":{
				"Fn::ForEach::Items":[
					"I",
					"x,y",
					{"Res${I}":{"Type":"AWS::SNS::Topic","Properties":{}}}
				]
			}}`,
			wantResource: []string{"Resx", "Resy"},
			wantAbsent:   []string{"Fn::ForEach::Items"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpl, err := cloudformation.ParseTemplate(tc.template)
			require.NoError(t, err)

			for _, id := range tc.wantResource {
				_, ok := tmpl.Resources[id]
				assert.True(t, ok, "expected expanded resource %q", id)
			}
			for _, id := range tc.wantAbsent {
				_, ok := tmpl.Resources[id]
				assert.False(t, ok, "loop key %q must not survive expansion", id)
			}
		})
	}
}

func TestParseTemplate_FnForEach_Substitutes(t *testing.T) {
	t.Parallel()

	const tpl = `{"Resources":{
		"Fn::ForEach::Topics":[
			"Name",
			["Alpha"],
			{"Topic${Name}":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"prefix-${Name}"}}}
		]
	}}`

	tmpl, err := cloudformation.ParseTemplate(tpl)
	require.NoError(t, err)

	res, ok := tmpl.Resources["TopicAlpha"]
	require.True(t, ok)
	assert.Equal(t, "AWS::SNS::Topic", res.Type)
	assert.Equal(t, "prefix-Alpha", res.Properties["TopicName"])
}

// ---- StackSets: real child-stack provisioning --------------------------------

func TestCreateStackInstances_ProvisionsChildStacks(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("prov-ss", "desc", simpleTemplate)
	require.NoError(t, err)

	before := len(b.ListAll())

	_, err = b.CreateStackInstances(
		t.Context(),
		"prov-ss",
		[]string{"111111111111", "222222222222"},
		[]string{"us-east-1"},
	)
	require.NoError(t, err)

	instances, err := b.ListStackInstances("prov-ss", "")
	require.NoError(t, err)
	require.Len(t, instances.Data, 2)

	// Each instance must reference a real, describable child stack.
	for _, inst := range instances.Data {
		assert.Equal(t, "CURRENT", inst.Status)
		require.NotEmpty(t, inst.StackID)
		child, derr := b.DescribeStack(inst.StackID)
		require.NoError(t, derr, "instance stack %s must be a real stack", inst.StackID)
		assert.Equal(t, "CREATE_COMPLETE", child.StackStatus)
	}

	after := len(b.ListAll())
	assert.Equal(t, before+2, after, "two child stacks must have been provisioned")
}

func TestDeleteStackInstances_TearsDownChildStacks(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("teardown-ss", "desc", simpleTemplate)
	require.NoError(t, err)

	_, err = b.CreateStackInstances(t.Context(), "teardown-ss", []string{"111111111111"}, []string{"us-east-1"})
	require.NoError(t, err)

	instances, err := b.ListStackInstances("teardown-ss", "")
	require.NoError(t, err)
	require.Len(t, instances.Data, 1)
	childID := instances.Data[0].StackID

	_, err = b.DeleteStackInstances(t.Context(), "teardown-ss", []string{"111111111111"}, []string{"us-east-1"})
	require.NoError(t, err)

	remaining, err := b.ListStackInstances("teardown-ss", "")
	require.NoError(t, err)
	assert.Empty(t, remaining.Data)

	// The provisioned child stack must have been torn down.
	child, derr := b.DescribeStack(childID)
	if derr == nil {
		assert.Equal(t, "DELETE_COMPLETE", child.StackStatus)
	}
}
