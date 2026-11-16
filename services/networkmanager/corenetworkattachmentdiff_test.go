package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const attachmentPolicyDoc = `{
  "version": "2021.12",
  "core-network-configuration": {
    "asn-ranges": ["64512-64555"],
    "edge-locations": [{"location": "us-east-1", "asn": 64512}]
  },
  "segments": [{"name": "prod"}, {"name": "dev"}],
  "segment-actions": [
    {"action": "create-route", "segment": "prod",
     "destination-cidr-blocks": ["10.9.0.0/16"], "destinations": ["blackhole"]}
  ],
  "attachment-policies": [
    {"rule-number": 100,
     "conditions": [{"type": "tag-value", "operator": "equals", "key": "env", "value": "prod"}],
     "action": {"association-method": "constant", "segment": "prod"}},
    {"rule-number": 200, "condition-logic": "and",
     "conditions": [
       {"type": "tag-exists", "key": "segment"},
       {"type": "account-id", "operator": "equals", "value": "000000000000"}],
     "action": {"association-method": "tag", "tag-value-of-key": "segment"}}
  ]
}`

func TestCoreNetworkChangeSet_AttachmentAndEdgeChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want     map[string]string
		name     string
		wantType types.ChangeType
		tags     []types.Tag
	}{
		{
			name: "constant_rule", tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
			wantType: types.ChangeTypeAttachmentMapping, want: map[string]string{"SegmentName": "prod"},
		},
		{
			name: "tag_rule", tags: []types.Tag{{Key: aws.String("segment"), Value: aws.String("dev")}},
			wantType: types.ChangeTypeAttachmentMapping, want: map[string]string{"SegmentName": "dev"},
		},
		{
			name: "propagation", tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
			wantType: types.ChangeTypeAttachmentRoutePropagation, want: map[string]string{"SegmentName": "prod"},
		},
		{name: "edge", wantType: types.ChangeTypeCoreNetworkEdge, want: map[string]string{"Edge": "us-east-1"}},
		{
			name:     "static_route",
			wantType: types.ChangeTypeAttachmentRouteStatic,
			want:     map[string]string{"Cidr": "10.9.0.0/16"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			cn := createTestCoreNetwork(t, client)
			id := cn.CoreNetwork.CoreNetworkId

			att, err := client.CreateVpcAttachment(ctx, &networkmanagersdk.CreateVpcAttachmentInput{
				CoreNetworkId: id, Tags: tt.tags,
				VpcArn:     aws.String("arn:aws:ec2:us-east-1:000000000000:vpc/vpc-0123456789abcdef0"),
				SubnetArns: []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-0123456789abcdef0"},
			})
			require.NoError(t, err)

			put, err := client.PutCoreNetworkPolicy(ctx, &networkmanagersdk.PutCoreNetworkPolicyInput{
				CoreNetworkId: id, PolicyDocument: aws.String(attachmentPolicyDoc),
			})
			require.NoError(t, err)

			out, err := client.GetCoreNetworkChangeSet(ctx, &networkmanagersdk.GetCoreNetworkChangeSetInput{
				CoreNetworkId: id, PolicyVersionId: put.CoreNetworkPolicy.PolicyVersionId,
			})
			require.NoError(t, err)

			var found *types.CoreNetworkChange

			for i := range out.CoreNetworkChanges {
				if out.CoreNetworkChanges[i].Type == tt.wantType {
					found = &out.CoreNetworkChanges[i]
				}
			}

			require.NotNil(t, found, "no %s change in %d changes", tt.wantType, len(out.CoreNetworkChanges))
			assert.Equal(t, types.ChangeActionAdd, found.Action)
			require.NotNil(t, found.NewValues)

			if seg, ok := tt.want["SegmentName"]; ok {
				assert.Equal(t, seg, aws.ToString(found.NewValues.SegmentName))
			}

			if edge, ok := tt.want["Edge"]; ok {
				assert.Equal(t, []string{edge}, found.NewValues.EdgeLocations)
				assert.EqualValues(t, 64512, aws.ToInt64(found.NewValues.Asn))
			}

			if cidr, ok := tt.want["Cidr"]; ok {
				assert.Equal(t, cidr, aws.ToString(found.NewValues.Cidr))
				assert.Equal(t, "blackhole", aws.ToString(found.NewValues.DestinationIdentifier))
			}

			if tt.wantType == types.ChangeTypeAttachmentMapping ||
				tt.wantType == types.ChangeTypeAttachmentRoutePropagation {
				assert.Equal(
					t,
					aws.ToString(att.VpcAttachment.Attachment.AttachmentId),
					aws.ToString(found.NewValues.AttachmentId),
				)
			}
		})
	}
}

const routingPolicyDoc = `{
  "version": "2025.11",
  "core-network-configuration": {
    "asn-ranges": ["64512-64555"],
    "edge-locations": [{"location": "us-east-1"}, {"location": "us-west-2"}]
  },
  "segments": [{"name": "prod"}, {"name": "dev"}],
  "routing-policies": [
    {"routing-policy-name": "RP1", "routing-policy-direction": "inbound", "routing-policy-number": 10,
     "routing-policy-rules": [{"rule-number": 1, "rule-definition": {
       "match-conditions": [{"type": "prefix-in-cidr", "value": "10.0.0.0/8"}],
       "condition-logic": "and", "action": {"type": "drop"}}}]}
  ],
  "segment-actions": [
    {"action": "share", "mode": "attachment-route", "segment": "prod", "share-with": ["dev"],
     "routing-policy-names": ["RP1"]},
    {"action": "associate-routing-policy", "segment": "prod",
     "edge-location-association": {
       "edge-location": "us-east-1", "peer-edge-location": "us-west-2", "routing-policy-names": ["RP1"]}}
  ],
  "attachment-routing-policy-rules": [
    {"rule-number": 10, "conditions": [{"type": "routing-policy-label", "value": "gold"}],
     "action": {"associate-routing-policies": ["RP1"]}}
  ]
}`

func TestCoreNetworkChangeSet_RoutingPolicyChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, v *types.CoreNetworkChangeValues)
		name  string
		typ   types.ChangeType
	}{
		{
			name: "policy", typ: types.ChangeTypeRoutingPolicy,
			check: func(t *testing.T, v *types.CoreNetworkChangeValues) {
				t.Helper()

				assert.Contains(t, aws.ToString(v.RoutingPolicy), `"RP1"`)
				assert.Equal(t, types.RoutingPolicyDirectionInbound, v.RoutingPolicyDirection)
			},
		},
		{
			name: "segment_association", typ: types.ChangeTypeRoutingPolicySegmentAssociation,
			check: func(t *testing.T, v *types.CoreNetworkChangeValues) {
				t.Helper()

				require.Len(t, v.RoutingPolicyAssociationDetails, 1)
				assert.Equal(t, []string{"RP1"}, v.RoutingPolicyAssociationDetails[0].RoutingPolicyNames)
				assert.Equal(t, []string{"dev"}, v.RoutingPolicyAssociationDetails[0].SharedSegments)
			},
		},
		{
			name: "edge_association", typ: types.ChangeTypeRoutingPolicyEdgeAssociation,
			check: func(t *testing.T, v *types.CoreNetworkChangeValues) {
				t.Helper()

				assert.Equal(t, []string{"us-east-1"}, v.EdgeLocations)
				assert.Equal(t, []string{"us-west-2"}, v.PeerEdgeLocations)
			},
		},
		{
			name: "attachment_association", typ: types.ChangeTypeRoutingPolicyAttachmentAssociation,
			check: func(t *testing.T, v *types.CoreNetworkChangeValues) {
				t.Helper()

				require.Len(t, v.RoutingPolicyAssociationDetails, 1)
				assert.Equal(t, []string{"RP1"}, v.RoutingPolicyAssociationDetails[0].RoutingPolicyNames)
				assert.NotEmpty(t, aws.ToString(v.AttachmentId))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			cn := createTestCoreNetwork(t, client)
			id := cn.CoreNetwork.CoreNetworkId

			att, err := client.CreateVpcAttachment(ctx, &networkmanagersdk.CreateVpcAttachmentInput{
				CoreNetworkId: id,
				VpcArn:        aws.String("arn:aws:ec2:us-east-1:000000000000:vpc/vpc-0123456789abcdef0"),
				SubnetArns:    []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-0123456789abcdef0"},
			})
			require.NoError(t, err)

			_, err = client.PutAttachmentRoutingPolicyLabel(
				ctx,
				&networkmanagersdk.PutAttachmentRoutingPolicyLabelInput{
					CoreNetworkId:      id,
					AttachmentId:       att.VpcAttachment.Attachment.AttachmentId,
					RoutingPolicyLabel: aws.String("gold"),
				},
			)
			require.NoError(t, err)

			put, err := client.PutCoreNetworkPolicy(ctx, &networkmanagersdk.PutCoreNetworkPolicyInput{
				CoreNetworkId: id, PolicyDocument: aws.String(routingPolicyDoc),
			})
			require.NoError(t, err)

			out, err := client.GetCoreNetworkChangeSet(ctx, &networkmanagersdk.GetCoreNetworkChangeSetInput{
				CoreNetworkId: id, PolicyVersionId: put.CoreNetworkPolicy.PolicyVersionId,
			})
			require.NoError(t, err)

			for i := range out.CoreNetworkChanges {
				if c := &out.CoreNetworkChanges[i]; c.Type == tt.typ {
					assert.Equal(t, types.ChangeActionAdd, c.Action)
					require.NotNil(t, c.NewValues)
					tt.check(t, c.NewValues)

					return
				}
			}

			t.Fatalf("no %s change in %d changes", tt.typ, len(out.CoreNetworkChanges))
		})
	}
}
