package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wantFinding struct {
	path      string
	elem      string
	variant   variantKind
	confident bool
}

func TestScanDir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want []wantFinding
	}{
		{
			// Pre-fix services/ec2/handler_instances.go (commit 3337c961d^):
			// DescribeInstanceTopology's NetworkNodeSet. Confirmed by the fix
			// commit to hard-fail a real client's decode.
			name: "double wrap plain item is confident",
			src: `package ec2

type instanceTopologyItem struct {
	AvailabilityZone string ` + "`xml:\"availabilityZone\"`" + `
	NetworkNodeSet   struct {
		Items []struct {
			Value string ` + "`xml:\"item\"`" + `
		} ` + "`xml:\"item\"`" + `
	} ` + "`xml:\"networkNodeSet\"`" + `
}
`,
			want: []wantFinding{
				{
					path:      "instanceTopologyItem.NetworkNodeSet.Items",
					elem:      "item",
					variant:   variantDoubleWrap,
					confident: true,
				},
			},
		},
		{
			// Pre-fix services/ec2/handler_network_interfaces.go (commit 3337c961d^):
			// AssignIpv6Addresses. Wrapper name has no "Set"/"List" suffix at all --
			// proves double-wrap needs no naming signal to be confident.
			name: "double wrap with no set suffix is still confident",
			src: `package ec2

type assignIpv6Response struct {
	RequestID             string ` + "`xml:\"requestId\"`" + `
	AssignedIpv6Addresses struct {
		Items []struct {
			Ipv6Address string ` + "`xml:\"item\"`" + `
		} ` + "`xml:\"item\"`" + `
	} ` + "`xml:\"assignedIpv6Addresses\"`" + `
}
`,
			want: []wantFinding{
				{
					path:      "assignIpv6Response.AssignedIpv6Addresses.Items",
					elem:      "item",
					variant:   variantDoubleWrap,
					confident: true,
				},
			},
		},
		{
			// The classic AWS Query protocol (rds, sns, autoscaling, ... --
			// awsAwsquery_ prefix per services/_PROTOCOLS.md) wraps repeated
			// list elements in <member>, not <item> -- confirmed against
			// sns@v1.42.4/deserializers.go and rds@v1.124.1/deserializers.go,
			// both switching on strings.EqualFold("member", t.Name.Local). The
			// same double-wrap mistake in that convention must be caught too.
			name: "double wrap member sentinel is confident",
			src: `package sns

type topicItem struct {
	Endpoints struct {
		Items []struct {
			ARN string ` + "`xml:\"member\"`" + `
		} ` + "`xml:\"member\"`" + `
	} ` + "`xml:\"Endpoints\"`" + `
}
`,
			want: []wantFinding{
				{path: "topicItem.Endpoints.Items", elem: "member", variant: variantDoubleWrap, confident: true},
			},
		},
		{
			name: "named child member sentinel is needs review",
			src: `package sns

type subscriptionItem struct {
	Attributes struct {
		Items []struct {
			Name string ` + "`xml:\"key\"`" + `
		} ` + "`xml:\"member\"`" + `
	} ` + "`xml:\"Attributes\"`" + `
}
`,
			want: []wantFinding{
				{path: "subscriptionItem.Attributes.Items", elem: "key", variant: variantNamedChild, confident: false},
			},
		},
		{
			// Pre-fix services/ec2/handler_scheduled_instances.go (commit 3337c961d^):
			// RunScheduledInstances InstanceIDSet -- a real confirmed decode-crash
			// bug. Still reported needs-review, not confident: this exact shape
			// (single member, "Set"-suffixed wrapper) is structurally identical to
			// GetInstanceTypesFromInstanceRequirements' InstanceTypeSet, a real
			// correct AWS shape (types.InstanceTypeInfoFromInstanceRequirements has
			// exactly one member, InstanceType) -- there is no syntactic way to
			// tell them apart, only a real SDK deserializer read can.
			name: "named child anonymous wrapper is needs review not confident",
			src: `package ec2

type runScheduledInstancesResponse struct {
	RequestID     string ` + "`xml:\"requestId\"`" + `
	InstanceIDSet struct {
		Items []struct {
			InstanceID string ` + "`xml:\"instanceId,omitempty\"`" + `
		} ` + "`xml:\"item\"`" + `
	} ` + "`xml:\"instanceIdSet\"`" + `
}
`,
			want: []wantFinding{
				{
					path: "runScheduledInstancesResponse.InstanceIDSet.Items", elem: "instanceId",
					variant: variantNamedChild, confident: false,
				},
			},
		},
		{
			// Pre-fix services/ec2/handler_deepdive_ops.go (commit b430921d9^):
			// vpcEndpointSubnetIDSet, a NAMED wrapper type rather than an inline
			// anonymous struct -- another real confirmed bug, still needs-review.
			name: "named child named wrapper type is needs review",
			src: `package ec2

type vpcEndpointSubnetIDSet struct {
	Items []struct {
		SubnetID string ` + "`xml:\"subnetId\"`" + `
	} ` + "`xml:\"item\"`" + `
}

type vpcEndpointItem struct {
	SubnetIDs vpcEndpointSubnetIDSet ` + "`xml:\"subnetIdSet\"`" + `
}
`,
			want: []wantFinding{
				{path: "vpcEndpointSubnetIDSet.Items", elem: "subnetId", variant: variantNamedChild, confident: false},
			},
		},
		{
			// Pre-fix services/ec2/handler_account_attrs.go (commit b430921d9^):
			// DescribePrefixLists cidrSet, the nested-path tag form ("cidrSet>item")
			// with no wrapper struct at all -- a real confirmed bug, needs-review.
			name: "named child path tag is needs review",
			src: `package ec2

type cidrItem struct {
	CIDR string ` + "`xml:\"cidrIp\"`" + `
}

type describePrefixListsItem struct {
	CidrsSet []cidrItem ` + "`xml:\"cidrSet>item\"`" + `
}
`,
			want: []wantFinding{
				{
					path:      "describePrefixListsItem.CidrsSet",
					elem:      "cidrIp",
					variant:   variantNamedChild,
					confident: false,
				},
			},
		},
		{
			name: "already fixed plain string list not flagged",
			src: `package ec2

type assignIpv6ResponseFixed struct {
	AssignedIpv6Addresses struct {
		Items []string ` + "`xml:\"item\"`" + `
	} ` + "`xml:\"assignedIpv6Addresses\"`" + `
}
`,
			want: nil,
		},
		{
			name: "already fixed named wrapper type plain string not flagged",
			src: `package ec2

type vpcEndpointSubnetIDSet struct {
	Items []string ` + "`xml:\"item\"`" + `
}
`,
			want: nil,
		},
		{
			// services/autoscaling/handler.go:567's xmlStringValueList: a
			// chardata-capturing wrapper struct is the correct, decode-safe way
			// to represent a plain <member>value</member> scalar list -- must
			// NOT be flagged, even though it structurally has exactly one
			// meaningful, non-sentinel-tagged member (empty name before the
			// comma in xml:",chardata").
			name: "chardata wrapped scalar not flagged",
			src: `package autoscaling

type xmlStringValue struct {
	Value string ` + "`xml:\",chardata\"`" + `
}

type xmlStringValueList struct {
	Members []xmlStringValue ` + "`xml:\"member\"`" + `
}
`,
			want: nil,
		},
		{
			// getIpamPoolCidrsResponse.IpamPoolCidrSet: a genuine two-member
			// object list wrapped in a Set-suffixed name. Must NOT be flagged --
			// this is exactly the "some list-of-object shapes are genuinely
			// correct" case the task warns about.
			name: "genuine multi field object list not flagged",
			src: `package ec2

type ipamPoolCidrItem struct {
	Cidr  string ` + "`xml:\"cidr\"`" + `
	State string ` + "`xml:\"state\"`" + `
}

type getIpamPoolCidrsResponse struct {
	IpamPoolCidrSet struct {
		Items []ipamPoolCidrItem ` + "`xml:\"item\"`" + `
	} ` + "`xml:\"ipamPoolCidrSet\"`" + `
}
`,
			want: nil,
		},
		{
			// DescribeVpcEndpointServicePermissions.AllowedPrincipals: a real,
			// currently-shipping shape that happens to structurally match variant
			// b (single member, not tagged "item") but is a genuinely correct
			// partial rendering of the real two-member types.AllowedPrincipal.
			// Still reported (candidates always are), but never confident.
			name: "single field member shape still reported as needs review",
			src: `package ec2

type describeVpcEndpointServicePermissionsResponse struct {
	RequestID         string ` + "`xml:\"requestId\"`" + `
	AllowedPrincipals struct {
		Items []struct {
			Principal string ` + "`xml:\"principal\"`" + `
		} ` + "`xml:\"item\"`" + `
	} ` + "`xml:\"allowedPrincipals\"`" + `
}
`,
			want: []wantFinding{
				{
					path: "describeVpcEndpointServicePermissionsResponse.AllowedPrincipals.Items", elem: "principal",
					variant: variantNamedChild, confident: false,
				},
			},
		},
		{
			name: "attr and xmlname members ignored when counting single member",
			src: `package ec2

type xmlnsItem struct {
	XMLName xml.Name ` + "`xml:\"item\"`" + `
	Xmlns   string   ` + "`xml:\"xmlns,attr\"`" + `
	Value   string   ` + "`xml:\"principal\"`" + `
}

type withXMLNSWrapper struct {
	NameSet struct {
		Items []xmlnsItem ` + "`xml:\"item\"`" + `
	} ` + "`xml:\"nameSet\"`" + `
}
`,
			want: []wantFinding{
				{
					path:      "withXMLNSWrapper.NameSet.Items",
					elem:      "principal",
					variant:   variantNamedChild,
					confident: false,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(tt.src), 0o600))

			got, err := scanDir(dir, dir)
			require.NoError(t, err)

			assert.Equal(t, tt.want, stripPositions(got))
		})
	}
}

func stripPositions(findings []finding) []wantFinding {
	if len(findings) == 0 {
		return nil
	}

	out := make([]wantFinding, len(findings))
	for i, f := range findings {
		out[i] = wantFinding{path: f.Path, elem: f.Elem, variant: f.Variant, confident: f.Confident}
	}

	return out
}

func TestIsSentinelTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		xmlVal string
		want   bool
	}{
		{name: "plain item", xmlVal: "item", want: true},
		{name: "item with option", xmlVal: "item,omitempty", want: true},
		{name: "nested item path", xmlVal: "cidrSet>item", want: true},
		{name: "deeper nested item path", xmlVal: "a>cidrSet>item", want: true},
		{name: "plain member", xmlVal: "member", want: true},
		{name: "nested member path", xmlVal: "TagList>member", want: true},
		{name: "not a sentinel tag", xmlVal: "principal", want: false},
		{name: "named element not sentinel", xmlVal: "instanceId,omitempty", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isSentinelTag(tt.xmlVal))
		})
	}
}

func TestXMLBaseName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		xmlVal string
		want   string
	}{
		{name: "plain name", xmlVal: "cidrIp", want: "cidrIp"},
		{name: "with option", xmlVal: "instanceId,omitempty", want: "instanceId"},
		{name: "nested path", xmlVal: "cidrSet>item", want: "item"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, xmlBaseName(tt.xmlVal))
		})
	}
}
