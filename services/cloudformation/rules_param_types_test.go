package cloudformation_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/require"
)

func TestRules_AWSSpecificParameterTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		assert  string
		wantErr string
		param   string
		ptype   string
		deptTag string
	}{
		{
			name:   "each_member_equals_value_of_all_pass",
			assert: `{"Fn::EachMemberEquals":[{"Fn::ValueOfAll":["AWS::EC2::VPC::Id","Tags.Department"]},"IT"]}`,
			ptype:  "String", deptTag: "IT",
		},
		{
			name:   "each_member_equals_value_of_all_fail",
			assert: `{"Fn::EachMemberEquals":[{"Fn::ValueOfAll":["AWS::EC2::VPC::Id","Tags.Department"]},"IT"]}`,
			ptype:  "String", deptTag: "Sales",
			wantErr: "Rule 'R' assertion failed: bad",
		},
		{
			name:   "value_of_tag_equals",
			assert: `{"Fn::Equals":[{"Fn::ValueOf":["P","Tags.Department"]},"IT"]}`,
			ptype:  "AWS::EC2::VPC::Id", param: "{vpc}", deptTag: "IT",
		},
		{
			name:   "value_of_tag_mismatch",
			assert: `{"Fn::Equals":[{"Fn::ValueOf":["P","Tags.Department"]},"IT"]}`,
			ptype:  "AWS::EC2::VPC::Id", param: "{vpc}", deptTag: "HR",
			wantErr: "Rule 'R' assertion failed: bad",
		},
		{
			name:   "subnet_vpc_in_ref_all",
			assert: `{"Fn::EachMemberIn":[{"Fn::ValueOf":["P","VpcId"]},{"Fn::RefAll":"AWS::EC2::VPC::Id"}]}`,
			ptype:  "List<AWS::EC2::Subnet::Id>", param: "{subnet}",
		},
		{
			name:   "subnet_az_contains",
			assert: `{"Fn::Contains":[{"Fn::ValueOf":["P","AvailabilityZone"]},"us-east-1z"]}`,
			ptype:  "AWS::EC2::Subnet::Id", param: "{subnet}",
			wantErr: "Rule 'R' assertion failed: bad",
		},
		{
			name:   "default_security_group",
			assert: `{"Fn::Equals":[{"Fn::ValueOf":["P","DefaultSecurityGroup"]},"{sg}"]}`,
			ptype:  "AWS::EC2::VPC::Id", param: "{vpc}",
		},
		{
			name:   "ref_all_security_groups_contains_default",
			assert: `{"Fn::Contains":[{"Fn::RefAll":"AWS::EC2::SecurityGroup::Id"},"{sg}"]}`,
			ptype:  "String",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backends, client := newManagedTypesTestClient(t)
			vpc, err := backends.EC2.Backend.CreateVpc("10.9.0.0/16", "default")
			require.NoError(t, err)

			if tc.deptTag != "" {
				require.NoError(t, backends.EC2.Backend.CreateTags([]string{vpc.ID},
					map[string]string{"Department": tc.deptTag}))
			}

			sn, err := backends.EC2.Backend.CreateSubnet(vpc.ID, "10.9.1.0/24", "us-east-1a")
			require.NoError(t, err)

			var defaultSG string

			for _, sg := range backends.EC2.Backend.DescribeSecurityGroups(nil) {
				if sg.VPCID == vpc.ID && sg.Name == "default" {
					defaultSG = sg.ID
				}
			}

			require.NotEmpty(t, defaultSG)

			val := map[string]string{"{vpc}": vpc.ID, "{subnet}": sn.ID}[tc.param]
			assertion := strings.ReplaceAll(tc.assert, "{sg}", defaultSG)
			tmpl := fmt.Sprintf(`{"Parameters":{"P":{"Type":%q}},`+
				`"Rules":{"R":{"Assertions":[{"Assert":%s,"AssertDescription":"bad"}]}},`+
				`"Resources":{"B":{"Type":"AWS::S3::Bucket"}}}`, tc.ptype, assertion)

			_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName:    aws.String("rules-" + strings.ReplaceAll(tc.name, "_", "-")),
				TemplateBody: aws.String(tmpl),
				Parameters:   []cfntypes.Parameter{{ParameterKey: aws.String("P"), ParameterValue: aws.String(val)}},
			})

			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
