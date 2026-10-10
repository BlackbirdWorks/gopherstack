package resourcegroups_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

type fakeSource struct {
	stacks map[string]*resourcegroups.StackState
	tagged []resourcegroups.TaggedResource
}

func (f fakeSource) TaggedResources(context.Context) []resourcegroups.TaggedResource { return f.tagged }

func (f fakeSource) Stack(id string) (*resourcegroups.StackState, bool) {
	s, ok := f.stacks[id]

	return s, ok
}

func newSourceBackend() *resourcegroups.InMemoryBackend {
	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetResourceSource(fakeSource{
		tagged: []resourcegroups.TaggedResource{
			{ARN: "arn:aws:sqs:us-east-1:000000000000:q1", Tags: map[string]string{"env": "prod", "team": "a"}},
			{ARN: "arn:aws:sqs:us-east-1:000000000000:q2", Tags: map[string]string{"env": "dev"}},
			{ARN: "arn:aws:sns:us-east-1:000000000000:t1", Tags: map[string]string{"env": "prod"}},
		},
		stacks: map[string]*resourcegroups.StackState{
			"good": {Status: "CREATE_COMPLETE", Resources: []resourcegroups.StackResource{
				{Type: "AWS::SQS::Queue", PhysicalID: "q2"},
				{Type: "AWS::SNS::Topic", PhysicalID: "arn:aws:sns:us-east-1:000000000000:t1"},
				{Type: "AWS::SQS::Queue", PhysicalID: "missing"},
			}},
			"mixed": {Status: "CREATE_COMPLETE", Resources: []resourcegroups.StackResource{
				{Type: "AWS::SQS::Queue", PhysicalID: "q2"},
				{Type: "AWS::Fake::Unsupported", PhysicalID: "a"},
				{Type: "AWS::Fake::Unsupported", PhysicalID: "b"},
			}},
			"failed":  {Status: "ROLLBACK_COMPLETE"},
			"deleted": {Status: "DELETE_COMPLETE"},
		},
	})

	return b
}

func TestResourceQueryEvaluation(t *testing.T) {
	t.Parallel()

	const (
		q1 = "arn:aws:sqs:us-east-1:000000000000:q1"
		q2 = "arn:aws:sqs:us-east-1:000000000000:q2"
		t1 = "arn:aws:sns:us-east-1:000000000000:t1"
	)

	tests := []struct {
		name     string
		qType    string
		query    string
		wantErr  string
		wantARNs []string
	}{
		{
			name: "tag_key_value", qType: "TAG_FILTERS_1_0",
			query:    `{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":"env","Values":["prod"]}]}`,
			wantARNs: []string{t1, q1},
		},
		{
			name: "tag_key_only_and_type", qType: "TAG_FILTERS_1_0",
			query:    `{"ResourceTypeFilters":["AWS::SQS::Queue"],"TagFilters":[{"Key":"env"}]}`,
			wantARNs: []string{q1, q2},
		},
		{
			name: "tag_and_semantics", qType: "TAG_FILTERS_1_0",
			query: `{"ResourceTypeFilters":["AWS::AllSupported"],` +
				`"TagFilters":[{"Key":"env","Values":["prod"]},{"Key":"team"}]}`,
			wantARNs: []string{q1},
		},
		{
			name: "stack_members", qType: "CLOUDFORMATION_STACK_1_0",
			query:    `{"ResourceTypeFilters":["AWS::AllSupported"],"StackIdentifier":"good"}`,
			wantARNs: []string{t1, q2},
		},
		{
			name: "stack_type_filter", qType: "CLOUDFORMATION_STACK_1_0",
			query:    `{"ResourceTypeFilters":["AWS::SNS::Topic"],"StackIdentifier":"good"}`,
			wantARNs: []string{t1},
		},
		{
			name: "stack_missing", qType: "CLOUDFORMATION_STACK_1_0",
			query:   `{"ResourceTypeFilters":["AWS::AllSupported"],"StackIdentifier":"nope"}`,
			wantErr: "CLOUDFORMATION_STACK_NOT_EXISTING",
		},
		{
			name: "stack_deleted", qType: "CLOUDFORMATION_STACK_1_0",
			query:   `{"ResourceTypeFilters":["AWS::AllSupported"],"StackIdentifier":"deleted"}`,
			wantErr: "CLOUDFORMATION_STACK_NOT_EXISTING",
		},
		{
			name: "stack_inactive", qType: "CLOUDFORMATION_STACK_1_0",
			query:   `{"ResourceTypeFilters":["AWS::AllSupported"],"StackIdentifier":"failed"}`,
			wantErr: "CLOUDFORMATION_STACK_INACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newSourceBackend()
			rq := &resourcegroups.ResourceQuery{Type: tt.qType, Query: tt.query}

			_, err := b.CreateGroup(ctx, "g", "", rq, nil, nil)
			require.NoError(t, err)

			search, err := b.SearchResourcesPage(ctx, rq, "", 0)
			require.NoError(t, err)

			list, err := b.ListGroupResourcesPage(ctx, "g", nil, "", 0)
			require.NoError(t, err)

			for _, p := range []resourcegroups.QueryPage{search, list} {
				got := make([]string, 0, len(p.Identifiers))
				for _, id := range p.Identifiers {
					got = append(got, id.ResourceArn)
				}

				if tt.wantErr != "" {
					require.Len(t, p.Errors, 1)
					assert.Equal(t, tt.wantErr, p.Errors[0].ErrorCode)
					assert.Empty(t, got)

					continue
				}

				assert.Empty(t, p.Errors)
				assert.Equal(t, tt.wantARNs, got)
			}
		})
	}
}

func TestStackQueryUnsupportedResourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantErrors int
	}{
		{
			name:       "all_types",
			query:      `{"ResourceTypeFilters":["AWS::AllSupported"],"StackIdentifier":"mixed"}`,
			wantErrors: 1,
		},
		{name: "filtered_out", query: `{"ResourceTypeFilters":["AWS::SQS::Queue"],"StackIdentifier":"mixed"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newSourceBackend()
			rq := &resourcegroups.ResourceQuery{Type: "CLOUDFORMATION_STACK_1_0", Query: tt.query}

			page, err := b.SearchResourcesPage(context.Background(), rq, "", 0)
			require.NoError(t, err)
			require.Len(t, page.Errors, tt.wantErrors)

			if tt.wantErrors > 0 {
				assert.Equal(t, "RESOURCE_TYPE_NOT_SUPPORTED", page.Errors[0].ErrorCode)
			}

			require.Len(t, page.Identifiers, 1)
			assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:q2", page.Identifiers[0].ResourceArn)
		})
	}
}
