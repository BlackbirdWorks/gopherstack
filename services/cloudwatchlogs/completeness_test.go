package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func newTestBackend(t *testing.T) *cloudwatchlogs.InMemoryBackend {
	t.Helper()
	b := cloudwatchlogs.NewInMemoryBackend()
	t.Cleanup(func() { b.Close() })

	return b
}

// ---- ResourcePolicy ----

func TestResourcePolicy_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_and_describe",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				p, err := b.PutResourcePolicy("my-policy", `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
				assert.Equal(t, "my-policy", p.PolicyName)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies()
				require.Len(t, policies, 1)
				assert.Equal(t, "my-policy", policies[0].PolicyName)
				assert.Equal(t, `{"Version":"2012-10-17"}`, policies[0].PolicyDocument)
			},
		},
		{
			name: "put_multiple_sorted",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("z-policy", `{}`)
				require.NoError(t, err)
				_, err = b.PutResourcePolicy("a-policy", `{}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies()
				require.Len(t, policies, 2)
				assert.Equal(t, "a-policy", policies[0].PolicyName)
				assert.Equal(t, "z-policy", policies[1].PolicyName)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("my-policy", `{"old":"doc"}`)
				require.NoError(t, err)
				_, err = b.PutResourcePolicy("my-policy", `{"new":"doc"}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeResourcePolicies()
				require.Len(t, policies, 1)
				assert.Equal(t, `{"new":"doc"}`, policies[0].PolicyDocument)
			},
		},
		{
			name: "delete_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("del-policy", `{}`)
				require.NoError(t, err)
				err = b.DeleteResourcePolicy("del-policy")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.Empty(t, b.DescribeResourcePolicies())
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteResourcePolicy("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrResourcePolicyNotFound)
			},
		},
		{
			name: "empty_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutResourcePolicy("", `{}`)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

// ---- DeliveryDestination ----

func TestDeliveryDestination_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_get_describe_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dest, err := b.PutDeliveryDestination("my-dest", "arn:aws:s3:::my-bucket", "JSON", nil)
				require.NoError(t, err)
				assert.Equal(t, "my-dest", dest.Name)
				assert.NotEmpty(t, dest.Arn)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				got, err := b.GetDeliveryDestination("my-dest")
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:s3:::my-bucket", got.TargetArn)

				dests := b.DescribeDeliveryDestinations()
				require.Len(t, dests, 1)

				err = b.DeleteDeliveryDestination("my-dest")
				require.NoError(t, err)

				_, err = b.GetDeliveryDestination("my-dest")
				require.Error(t, err)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDeliveryDestination("dest1", "arn:aws:s3:::old", "JSON", nil)
				require.NoError(t, err)
				_, err = b.PutDeliveryDestination("dest1", "arn:aws:s3:::new", "TEXT", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				got, err := b.GetDeliveryDestination("dest1")
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:s3:::new", got.TargetArn)
				assert.Equal(t, "TEXT", got.OutputFormat)
			},
		},
		{
			name: "get_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.GetDeliveryDestination("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrDeliveryDestinationNotFound)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteDeliveryDestination("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrDeliveryDestinationNotFound)
			},
		},
		{
			name: "policy_put_get_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDeliveryDestination("policy-dest", "arn:aws:s3:::bucket", "JSON", nil)
				require.NoError(t, err)
				err = b.PutDeliveryDestinationPolicy("policy-dest", `{"Statement":[]}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policy, err := b.GetDeliveryDestinationPolicy("policy-dest")
				require.NoError(t, err)
				assert.Contains(t, policy, "Statement")

				err = b.DeleteDeliveryDestinationPolicy("policy-dest")
				require.NoError(t, err)

				policy, err = b.GetDeliveryDestinationPolicy("policy-dest")
				require.NoError(t, err)
				assert.Empty(t, policy)
			},
		},
		{
			name: "policy_on_nonexistent_dest_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutDeliveryDestinationPolicy("ghost", `{}`)
				require.ErrorIs(t, err, cloudwatchlogs.ErrDeliveryDestinationNotFound)
			},
		},
		{
			name: "put_empty_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDeliveryDestination("", "arn:aws:s3:::b", "JSON", nil)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
		{
			name: "describe_sorted_by_name",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDeliveryDestination("z-dest", "arn:aws:s3:::z", "JSON", nil)
				require.NoError(t, err)
				_, err = b.PutDeliveryDestination("a-dest", "arn:aws:s3:::a", "JSON", nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dests := b.DescribeDeliveryDestinations()
				require.Len(t, dests, 2)
				assert.Equal(t, "a-dest", dests[0].Name)
				assert.Equal(t, "z-dest", dests[1].Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

// ---- DeliverySource ----

func TestDeliverySource_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_get_describe_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				src, err := b.PutDeliverySource("my-src", "APPLICATION_LOGS", []string{"arn:aws:ec2:::instance/i-123"}, nil)
				require.NoError(t, err)
				assert.Equal(t, "my-src", src.Name)
				assert.NotEmpty(t, src.Arn)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				got, err := b.GetDeliverySource("my-src")
				require.NoError(t, err)
				assert.Equal(t, "APPLICATION_LOGS", got.LogType)
				assert.Len(t, got.ResourceArns, 1)

				srcs := b.DescribeDeliverySources()
				require.Len(t, srcs, 1)

				err = b.DeleteDeliverySource("my-src")
				require.NoError(t, err)

				_, err = b.GetDeliverySource("my-src")
				require.Error(t, err)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDeliverySource("src1", "FLOW_LOGS", []string{"arn:old"}, nil)
				require.NoError(t, err)
				_, err = b.PutDeliverySource("src1", "VPC_FLOW_LOGS", []string{"arn:new"}, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				got, err := b.GetDeliverySource("src1")
				require.NoError(t, err)
				assert.Equal(t, "VPC_FLOW_LOGS", got.LogType)
				require.Len(t, got.ResourceArns, 1)
				assert.Equal(t, "arn:new", got.ResourceArns[0])
			},
		},
		{
			name: "get_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.GetDeliverySource("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrDeliverySourceNotFound)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteDeliverySource("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrDeliverySourceNotFound)
			},
		},
		{
			name: "put_empty_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDeliverySource("", "FLOW_LOGS", nil, nil)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
		{
			name: "describe_sorted_by_name",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDeliverySource("z-src", "FLOW_LOGS", nil, nil)
				require.NoError(t, err)
				_, err = b.PutDeliverySource("a-src", "FLOW_LOGS", nil, nil)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				srcs := b.DescribeDeliverySources()
				require.Len(t, srcs, 2)
				assert.Equal(t, "a-src", srcs[0].Name)
				assert.Equal(t, "z-src", srcs[1].Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

// ---- Destination (log routing) ----

func TestDestination_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_describe_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dest, err := b.PutDestination("my-dest", "arn:aws:kinesis:::stream/s", "arn:aws:iam:::role/r")
				require.NoError(t, err)
				assert.Equal(t, "my-dest", dest.DestinationName)
				assert.NotEmpty(t, dest.Arn)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dests := b.DescribeDestinations("")
				require.Len(t, dests, 1)

				err := b.DeleteDestination("my-dest")
				require.NoError(t, err)

				dests = b.DescribeDestinations("")
				assert.Empty(t, dests)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDestination("dest1", "arn:old", "arn:role-old")
				require.NoError(t, err)
				_, err = b.PutDestination("dest1", "arn:new", "arn:role-new")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dests := b.DescribeDestinations("")
				require.Len(t, dests, 1)
				assert.Equal(t, "arn:new", dests[0].TargetArn)
				assert.Equal(t, "arn:role-new", dests[0].RoleArn)
			},
		},
		{
			name: "put_destination_policy",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDestination("my-dest", "arn:aws:kinesis:::stream/s", "arn:aws:iam:::role/r")
				require.NoError(t, err)
				err = b.PutDestinationPolicy("my-dest", `{"Statement":[]}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				dests := b.DescribeDestinations("")
				require.Len(t, dests, 1)
				assert.JSONEq(t, `{"Statement":[]}`, dests[0].AccessPolicy)
			},
		},
		{
			name: "describe_prefix_filter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDestination("prod-dest", "arn:a", "arn:r")
				require.NoError(t, err)
				_, err = b.PutDestination("dev-dest", "arn:b", "arn:r")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				all := b.DescribeDestinations("")
				assert.Len(t, all, 2)

				prod := b.DescribeDestinations("prod")
				require.Len(t, prod, 1)
				assert.Equal(t, "prod-dest", prod[0].DestinationName)

				none := b.DescribeDestinations("nonexistent")
				assert.Empty(t, none)
			},
		},
		{
			name: "put_policy_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutDestinationPolicy("ghost", `{}`)
				require.ErrorIs(t, err, cloudwatchlogs.ErrDestinationNotFound)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteDestination("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrDestinationNotFound)
			},
		},
		{
			name: "put_empty_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutDestination("", "arn:a", "arn:r")
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

// ---- IndexPolicy ----

func TestIndexPolicy_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_describe_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				p, err := b.PutIndexPolicy("/aws/lambda/fn", `{"fields":["@message"]}`)
				require.NoError(t, err)
				assert.Equal(t, "/aws/lambda/fn", p.LogGroupIdentifier)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeIndexPolicies()
				require.Len(t, policies, 1)
				assert.Equal(t, "/aws/lambda/fn", policies[0].LogGroupIdentifier)

				err := b.DeleteIndexPolicy("/aws/lambda/fn")
				require.NoError(t, err)

				assert.Empty(t, b.DescribeIndexPolicies())
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIndexPolicy("/grp", `{"old":"policy"}`)
				require.NoError(t, err)
				_, err = b.PutIndexPolicy("/grp", `{"new":"policy"}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeIndexPolicies()
				require.Len(t, policies, 1)
				assert.Equal(t, `{"new":"policy"}`, policies[0].PolicyDocument)
			},
		},
		{
			name: "describe_sorted_by_identifier",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIndexPolicy("/z-grp", `{}`)
				require.NoError(t, err)
				_, err = b.PutIndexPolicy("/a-grp", `{}`)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				policies := b.DescribeIndexPolicies()
				require.Len(t, policies, 2)
				assert.Equal(t, "/a-grp", policies[0].LogGroupIdentifier)
				assert.Equal(t, "/z-grp", policies[1].LogGroupIdentifier)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteIndexPolicy("nonexistent")
				require.ErrorIs(t, err, cloudwatchlogs.ErrIndexPolicyNotFound)
			},
		},
		{
			name: "put_empty_identifier_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIndexPolicy("", `{}`)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

// ---- Transformer ----

func TestTransformer_CRUD(t *testing.T) {
	t.Parallel()

	baseProcessors := []map[string]any{
		{"parseJSON": map[string]any{}},
	}

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_get_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutTransformer("/aws/lambda/fn", baseProcessors)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				tr, err := b.GetTransformer("/aws/lambda/fn")
				require.NoError(t, err)
				assert.Equal(t, "/aws/lambda/fn", tr.LogGroupIdentifier)
				require.Len(t, tr.Processors, 1)

				err = b.DeleteTransformer("/aws/lambda/fn")
				require.NoError(t, err)

				_, err = b.GetTransformer("/aws/lambda/fn")
				require.ErrorIs(t, err, cloudwatchlogs.ErrTransformerNotFound)
			},
		},
		{
			name: "put_updates_existing",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutTransformer("/grp", baseProcessors)
				require.NoError(t, err)
				two := []map[string]any{{"parseJSON": map[string]any{}}, {"addField": map[string]any{"key": "v"}}}
				err = b.PutTransformer("/grp", two)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				tr, err := b.GetTransformer("/grp")
				require.NoError(t, err)
				assert.Len(t, tr.Processors, 2)
			},
		},
		{
			name: "get_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.GetTransformer("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrTransformerNotFound)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteTransformer("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrTransformerNotFound)
			},
		},
		{
			name: "put_empty_identifier_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.PutTransformer("", baseProcessors)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

// ---- Integration ----

func TestIntegration_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_get_list_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				ig, err := b.PutIntegration("my-opensearch", "OPENSEARCH")
				require.NoError(t, err)
				assert.Equal(t, "my-opensearch", ig.Name)
				assert.Equal(t, "ACTIVE", ig.Status)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				got, err := b.GetIntegration("my-opensearch")
				require.NoError(t, err)
				assert.Equal(t, "OPENSEARCH", got.Type)

				igs := b.ListIntegrations()
				require.Len(t, igs, 1)

				err = b.DeleteIntegration("my-opensearch")
				require.NoError(t, err)

				_, err = b.GetIntegration("my-opensearch")
				require.ErrorIs(t, err, cloudwatchlogs.ErrIntegrationNotFound)
			},
		},
		{
			name: "list_multiple_sorted",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIntegration("z-integration", "OPENSEARCH")
				require.NoError(t, err)
				_, err = b.PutIntegration("a-integration", "OPENSEARCH")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				igs := b.ListIntegrations()
				require.Len(t, igs, 2)
				assert.Equal(t, "a-integration", igs[0].Name)
				assert.Equal(t, "z-integration", igs[1].Name)
			},
		},
		{
			name: "get_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.GetIntegration("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrIntegrationNotFound)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteIntegration("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrIntegrationNotFound)
			},
		},
		{
			name: "put_empty_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIntegration("", "OPENSEARCH")
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

// ---- LogGroupDeletionProtection ----

func TestLogGroupDeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "default_is_false",
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.False(t, b.IsLogGroupDeletionProtected("/aws/lambda/fn"))
			},
		},
		{
			name: "enable_protection",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.SetLogGroupDeletionProtection("/aws/lambda/fn", true)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.True(t, b.IsLogGroupDeletionProtected("/aws/lambda/fn"))
			},
		},
		{
			name: "disable_protection",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.SetLogGroupDeletionProtection("/aws/lambda/fn", true)
				require.NoError(t, err)
				err = b.SetLogGroupDeletionProtection("/aws/lambda/fn", false)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.False(t, b.IsLogGroupDeletionProtected("/aws/lambda/fn"))
			},
		},
		{
			name: "groups_are_independent",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.SetLogGroupDeletionProtection("/grp-a", true)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				assert.True(t, b.IsLogGroupDeletionProtected("/grp-a"))
				assert.False(t, b.IsLogGroupDeletionProtected("/grp-b"))
			},
		},
		{
			name: "empty_identifier_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.SetLogGroupDeletionProtection("", true)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}

// ---- UpdateDeliveryConfiguration ----

func TestUpdateDeliveryConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, deliveryID string)
		name   string
	}{
		{
			name: "update_field_delimiter",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateDelivery("src", "arn:aws:logs:::delivery-destination:dst", nil)
				require.NoError(t, err)
				deliveries, _, err := b.DescribeDeliveries(100, "")
				require.NoError(t, err)
				require.Len(t, deliveries, 1)
				id := deliveries[0].ID
				err = b.UpdateDeliveryConfiguration(id, ",", nil)
				require.NoError(t, err)

				return id
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, _ string) {
				t.Helper()
				deliveries, _, err := b.DescribeDeliveries(100, "")
				require.NoError(t, err)
				require.Len(t, deliveries, 1)
				assert.Equal(t, ",", deliveries[0].FieldDelimiter)
			},
		},
		{
			name: "update_record_fields",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateDelivery("src", "arn:aws:logs:::delivery-destination:dst", nil)
				require.NoError(t, err)
				deliveries, _, err := b.DescribeDeliveries(100, "")
				require.NoError(t, err)
				require.Len(t, deliveries, 1)
				id := deliveries[0].ID
				err = b.UpdateDeliveryConfiguration(id, "", []string{"@timestamp", "@message"})
				require.NoError(t, err)

				return id
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, _ string) {
				t.Helper()
				deliveries, _, err := b.DescribeDeliveries(100, "")
				require.NoError(t, err)
				assert.Equal(t, []string{"@timestamp", "@message"}, deliveries[0].RecordFields)
			},
		},
		{
			name: "update_both_delimiter_and_fields",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateDelivery("src", "arn:aws:logs:::delivery-destination:dst", nil)
				require.NoError(t, err)
				deliveries, _, err := b.DescribeDeliveries(100, "")
				require.NoError(t, err)
				require.Len(t, deliveries, 1)
				id := deliveries[0].ID
				err = b.UpdateDeliveryConfiguration(id, "\t", []string{"@timestamp", "@message", "@logStream"})
				require.NoError(t, err)

				return id
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, _ string) {
				t.Helper()
				deliveries, _, err := b.DescribeDeliveries(100, "")
				require.NoError(t, err)
				require.Len(t, deliveries, 1)
				assert.Equal(t, "\t", deliveries[0].FieldDelimiter)
				assert.Equal(t, []string{"@timestamp", "@message", "@logStream"}, deliveries[0].RecordFields)
			},
		},
		{
			name: "update_nonexistent_delivery_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) string {
				t.Helper()
				err := b.UpdateDeliveryConfiguration("nonexistent-id", ",", nil)
				require.Error(t, err)

				return ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			var deliveryID string
			if tt.setup != nil {
				deliveryID = tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b, deliveryID)
			}
		})
	}
}
