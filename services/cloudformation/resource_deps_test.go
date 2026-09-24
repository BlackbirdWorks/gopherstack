package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestTopoSortResources_InferredDependencies proves topoSortResources orders
// resources by every dependency real CloudFormation infers -- not just
// explicit DependsOn -- so a template whose logical IDs happen to sort
// against their real reference order still provisions correctly.
func TestTopoSortResources_InferredDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resources  map[string]cloudformation.TemplateResource
		name       string
		wantBefore [][2]string
	}{
		{
			name: "ref_orders_against_alphabet",
			resources: map[string]cloudformation.TemplateResource{
				"AEndpoint": {Type: "AWS::EC2::VPCEndpoint", Properties: map[string]any{
					"VpcId": map[string]any{"Ref": "ZVpc"},
				}},
				"ZVpc": {Type: "AWS::EC2::VPC"},
			},
			wantBefore: [][2]string{{"ZVpc", "AEndpoint"}},
		},
		{
			name: "getatt_array_form",
			resources: map[string]cloudformation.TemplateResource{
				"AResource": {Type: "AWS::SNS::Topic", Properties: map[string]any{
					"TopicName": map[string]any{"Fn::GetAtt": []any{"ZBucket", "Arn"}},
				}},
				"ZBucket": {Type: "AWS::S3::Bucket"},
			},
			wantBefore: [][2]string{{"ZBucket", "AResource"}},
		},
		{
			name: "getatt_dotted_string_form",
			resources: map[string]cloudformation.TemplateResource{
				"AResource": {Type: "AWS::SNS::Topic", Properties: map[string]any{
					"TopicName": map[string]any{"Fn::GetAtt": "ZBucket.Arn"},
				}},
				"ZBucket": {Type: "AWS::S3::Bucket"},
			},
			wantBefore: [][2]string{{"ZBucket", "AResource"}},
		},
		{
			name: "sub_plain_ref_and_dotted_attr",
			resources: map[string]cloudformation.TemplateResource{
				"AResource": {Type: "AWS::SNS::Topic", Properties: map[string]any{
					"TopicName": map[string]any{"Fn::Sub": "prefix-${ZBucket}-${ZBucket.Arn}"},
				}},
				"ZBucket": {Type: "AWS::S3::Bucket"},
			},
			wantBefore: [][2]string{{"ZBucket", "AResource"}},
		},
		{
			name: "sub_escaped_literal_is_not_a_ref",
			resources: map[string]cloudformation.TemplateResource{
				"AResource": {Type: "AWS::SNS::Topic", Properties: map[string]any{
					"TopicName": map[string]any{"Fn::Sub": "literal-${!ZBucket}"},
				}},
				"ZBucket": {Type: "AWS::S3::Bucket"},
			},
			// No dependency edge: falls back to the alphabetical tie-break,
			// which already puts AResource before ZBucket.
			wantBefore: [][2]string{{"AResource", "ZBucket"}},
		},
		{
			name: "sub_map_variable_value_is_a_ref",
			resources: map[string]cloudformation.TemplateResource{
				"AResource": {Type: "AWS::SNS::Topic", Properties: map[string]any{
					"TopicName": map[string]any{"Fn::Sub": []any{
						"prefix-${Local}",
						map[string]any{"Local": map[string]any{"Ref": "ZOther"}},
					}},
				}},
				"ZOther": {Type: "AWS::S3::Bucket"},
			},
			wantBefore: [][2]string{{"ZOther", "AResource"}},
		},
		{
			name: "sub_map_local_var_shadows_same_named_resource",
			resources: map[string]cloudformation.TemplateResource{
				// "ZVpc" is declared as Fn::Sub's own local variable name, so
				// ${ZVpc} resolves to the local var's value (Ref ZOther), not
				// to a Ref of the resource logical ID "ZVpc" -- no edge to
				// ZVpc should be inferred.
				"AResource": {Type: "AWS::SNS::Topic", Properties: map[string]any{
					"TopicName": map[string]any{"Fn::Sub": []any{
						"prefix-${ZVpc}",
						map[string]any{"ZVpc": map[string]any{"Ref": "ZOther"}},
					}},
				}},
				"ZVpc":   {Type: "AWS::EC2::VPC"},
				"ZOther": {Type: "AWS::S3::Bucket"},
			},
			wantBefore: [][2]string{
				{"ZOther", "AResource"},
				// No forced edge to ZVpc: alphabetical tie-break still applies.
				{"AResource", "ZVpc"},
			},
		},
		{
			name: "nested_if_join_select",
			resources: map[string]cloudformation.TemplateResource{
				"AResource": {Type: "AWS::SNS::Topic", Properties: map[string]any{
					"TopicName": map[string]any{
						"Fn::Join": []any{",", []any{
							map[string]any{"Fn::If": []any{
								"SomeCondition",
								map[string]any{"Ref": "ZBucket"},
								map[string]any{"Fn::Select": []any{
									float64(0),
									map[string]any{"Fn::GetAtt": []any{"ZQueue", "Arn"}},
								}},
							}},
						}},
					},
				}},
				"ZBucket": {Type: "AWS::S3::Bucket"},
				"ZQueue":  {Type: "AWS::SQS::Queue"},
			},
			wantBefore: [][2]string{
				{"ZBucket", "AResource"},
				{"ZQueue", "AResource"},
			},
		},
		{
			name: "pseudo_params_and_template_params_are_not_dependencies",
			resources: map[string]cloudformation.TemplateResource{
				"AResource": {Type: "AWS::SNS::Topic", Properties: map[string]any{
					"Region": map[string]any{"Ref": "AWS::Region"},
					"Param":  map[string]any{"Ref": "SomeParameter"},
				}},
				"ZBucket": {Type: "AWS::S3::Bucket"},
			},
			// Neither Ref resolves to a declared resource, so no edge is
			// inferred and the plain alphabetical tie-break applies.
			wantBefore: [][2]string{{"AResource", "ZBucket"}},
		},
		{
			name: "explicit_dependson_still_honoured",
			resources: map[string]cloudformation.TemplateResource{
				"AResource": {Type: "AWS::SNS::Topic", DependsOn: []string{"ZBucket"}},
				"ZBucket":   {Type: "AWS::S3::Bucket"},
			},
			wantBefore: [][2]string{{"ZBucket", "AResource"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := cloudformation.TopoSortResources(tt.resources)
			require.NoError(t, err)
			assert.Len(t, result, len(tt.resources))

			pos := make(map[string]int, len(result))
			for i, id := range result {
				pos[id] = i
			}

			for _, pair := range tt.wantBefore {
				before, after := pair[0], pair[1]
				assert.Less(t, pos[before], pos[after],
					"expected %s (pos %d) before %s (pos %d)", before, pos[before], after, pos[after])
			}
		})
	}
}

// TestTopoSortResources_CircularDependency proves a dependency cycle -- via
// either explicit DependsOn or an inferred Ref -- fails with
// ErrCircularDependency and CloudFormation's real message wording, instead
// of silently falling back to alphabetical order.
func TestTopoSortResources_CircularDependency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resources map[string]cloudformation.TemplateResource
		name      string
	}{
		{
			name: "cycle_via_ref",
			resources: map[string]cloudformation.TemplateResource{
				"A": {Type: "AWS::S3::Bucket", Properties: map[string]any{
					"X": map[string]any{"Ref": "B"},
				}},
				"B": {Type: "AWS::S3::Bucket", Properties: map[string]any{
					"X": map[string]any{"Ref": "A"},
				}},
			},
		},
		{
			name: "cycle_via_dependson",
			resources: map[string]cloudformation.TemplateResource{
				"A": {Type: "AWS::S3::Bucket", DependsOn: []string{"B"}},
				"B": {Type: "AWS::S3::Bucket", DependsOn: []string{"A"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := cloudformation.TopoSortResources(tt.resources)
			require.Error(t, err)
			assert.Nil(t, result)
			require.ErrorIs(t, err, cloudformation.ErrCircularDependency)
			assert.Contains(t, err.Error(), "Circular dependency between resources: [A, B]")
		})
	}
}

// TestReverseDependencyOrder proves the deletion-order helper reverses
// topoSortResources' own creation-order graph and correctly filters it down
// to a subset (used for DeleteStack's live resources and UpdateStack's
// stale-resource cleanup).
func TestReverseDependencyOrder(t *testing.T) {
	t.Parallel()

	const tmpl = `{"Resources":{
		"ASubnet": {"Type": "AWS::EC2::Subnet", "Properties": {"VpcId": {"Ref": "ZVpc"}}},
		"ZVpc": {"Type": "AWS::EC2::VPC"},
		"Unrelated": {"Type": "AWS::S3::Bucket"}
	}}`

	tests := []struct {
		name    string
		badTmpl string
		ids     []string
		want    []string
	}{
		{
			name: "dependent_deleted_before_dependency",
			ids:  []string{"ASubnet", "ZVpc"},
			want: []string{"ASubnet", "ZVpc"},
		},
		{
			name: "filters_to_subset",
			ids:  []string{"ASubnet", "ZVpc", "Unrelated"},
			want: []string{"ASubnet", "ZVpc", "Unrelated"},
		},
		{
			name:    "falls_back_to_alphabetical_on_unparsable_template",
			ids:     []string{"ZVpc", "ASubnet"},
			want:    []string{"ASubnet", "ZVpc"},
			badTmpl: "not valid json or yaml: [[[",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := tmpl
			if tt.badTmpl != "" {
				body = tt.badTmpl
			}

			got := cloudformation.ReverseDependencyOrderForTest(tt.ids, body)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestCreateStack_ResourceDependencyInference_EndToEnd proves CreateStack
// succeeds through the real SDK client for a template whose logical IDs are
// deliberately named to invert their real dependency order (subnet and
// security group both sort alphabetically before the VPC they reference via
// Ref) and that declares no DependsOn at all -- exactly the class of
// template real AWS provisions correctly today and this backend previously
// rejected with a spurious VPC-not-found error.
func TestCreateStack_ResourceDependencyInference_EndToEnd(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	const tmpl = `{
"Resources": {
  "ASubnet": {
    "Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "ZVpc"}, "CidrBlock": "10.0.1.0/24"}
  },
  "ASecurityGroup": {
    "Type": "AWS::EC2::SecurityGroup",
    "Properties": {"GroupDescription": "test sg", "VpcId": {"Ref": "ZVpc"}}
  },
  "ZVpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}}
},
"Outputs": {
  "VpcId": {"Value": {"Ref": "ZVpc"}},
  "SubnetId": {"Value": {"Ref": "ASubnet"}},
  "SgId": {"Value": {"Ref": "ASecurityGroup"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "dep-inference-stack", tmpl)

	vpcID := outputs["VpcId"]
	require.NotEmpty(t, vpcID)

	subnets := backends.EC2.Backend.DescribeSubnets([]string{outputs["SubnetId"]})
	require.Len(t, subnets, 1)
	assert.Equal(t, vpcID, subnets[0].VPCID)

	sgs := backends.EC2.Backend.DescribeSecurityGroups([]string{outputs["SgId"]})
	require.Len(t, sgs, 1)
	assert.Equal(t, vpcID, sgs[0].VPCID)
}

// TestDeleteStack_ResourceDependencyInference_EndToEnd extends the same
// inverted-alphabetical VPC/Subnet/SecurityGroup template to prove
// DeleteStack tears resources down in the reverse of their creation order:
// the VPC's dependents are always deleted before the VPC itself, matching
// real AWS (a stack whose deletion order came out wrong would leave the VPC
// DELETE_FAILED with an EC2 DependencyViolation, since EC2 refuses to
// delete a VPC that still has subnets or non-default security groups).
func TestDeleteStack_ResourceDependencyInference_EndToEnd(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	const tmpl = `{
"Resources": {
  "ASubnet": {
    "Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "ZVpc"}, "CidrBlock": "10.0.1.0/24"}
  },
  "ASecurityGroup": {
    "Type": "AWS::EC2::SecurityGroup",
    "Properties": {"GroupDescription": "test sg", "VpcId": {"Ref": "ZVpc"}}
  },
  "ZVpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}}
},
"Outputs": {"VpcId": {"Value": {"Ref": "ZVpc"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "dep-inference-delete-stack", tmpl)
	vpcID := outputs["VpcId"]
	require.NotEmpty(t, vpcID)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{
		StackName: aws.String("dep-inference-delete-stack"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{
		StackName: aws.String("dep-inference-delete-stack"),
	})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	assert.Equal(t, "DELETE_COMPLETE", string(desc.Stacks[0].StackStatus),
		"a wrong deletion order leaves the stack DELETE_FAILED (VPC has dependencies)")

	assert.Empty(t, backends.EC2.Backend.DescribeVpcs([]string{vpcID}))
}

// TestUpdateStack_ResourceDependencyInference_EndToEnd proves UpdateStack's
// resource-creation phase orders newly-added resources the same way
// CreateStack does: a subnet added in the same update as the VPC it
// references, with a logical ID that sorts before it, still provisions
// successfully with no explicit DependsOn.
func TestUpdateStack_ResourceDependencyInference_EndToEnd(t *testing.T) {
	t.Parallel()

	_, client := newManagedTypesTestClient(t)

	const stackName = "dep-inference-update-stack"

	const initial = `{"Resources": {
		"ZVpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}}
	}}`
	createStackAndGetOutputs(t, client, stackName, initial)

	const updated = `{
"Resources": {
  "ASubnet": {
    "Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "ZVpc"}, "CidrBlock": "10.0.1.0/24"}
  },
  "ZVpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}}
},
"Outputs": {"SubnetId": {"Value": {"Ref": "ASubnet"}}}
}`

	_, err := client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(updated),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	assert.Equal(t, "UPDATE_COMPLETE", string(desc.Stacks[0].StackStatus))
}

// TestUpdateStack_StaleResourceDependencyOrder_EndToEnd proves
// UpdateStack's stale-resource cleanup phase (deleteStaleResources) also
// deletes in reverse dependency order: removing both a VPC and its subnet
// from the template in the same update succeeds regardless of their logical
// IDs' alphabetical order, because the subnet is always dropped before the
// VPC it depended on.
func TestUpdateStack_StaleResourceDependencyOrder_EndToEnd(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	const stackName = "dep-inference-stale-stack"

	const initial = `{
"Resources": {
  "ASubnet": {
    "Type": "AWS::EC2::Subnet",
    "Properties": {"VpcId": {"Ref": "ZVpc"}, "CidrBlock": "10.0.1.0/24"}
  },
  "ZVpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}}
},
"Outputs": {"VpcId": {"Value": {"Ref": "ZVpc"}}}
}`
	outputs := createStackAndGetOutputs(t, client, stackName, initial)
	vpcID := outputs["VpcId"]
	require.NotEmpty(t, vpcID)

	const emptied = `{"Resources": {}}`

	_, err := client.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(emptied),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	assert.Equal(t, "UPDATE_COMPLETE", string(desc.Stacks[0].StackStatus),
		"a wrong stale-deletion order leaves the update failed (VPC has dependencies)")

	assert.Empty(t, backends.EC2.Backend.DescribeVpcs([]string{vpcID}))
}
