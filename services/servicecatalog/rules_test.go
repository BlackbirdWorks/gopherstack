package servicecatalog_test

import (
	"context"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	networkURL      = "https://s3.amazonaws.com/templates/network.json"
	networkTemplate = `{"Parameters":{"Subnet":{"Type":"AWS::EC2::Subnet::Id","Default":"subnet-a"},` +
		`"Subnets":{"Type":"List<AWS::EC2::Subnet::Id>","Default":"subnet-a"},` +
		`"VpcId":{"Type":"AWS::EC2::VPC::Id","Default":"vpc-1"}},"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`
)

type fakeResources struct{ subnetVPC map[string]string }

func (f fakeResources) ResourceIDs(_ context.Context, paramType string) ([]string, bool) {
	switch paramType {
	case "AWS::EC2::VPC::Id":
		return []string{"vpc-1", "vpc-2"}, true
	case "AWS::EC2::Subnet::Id":
		ids := make([]string, 0, len(f.subnetVPC))
		for id := range f.subnetVPC {
			ids = append(ids, id)
		}

		slices.Sort(ids)

		return ids, true
	}

	return nil, false
}

func (f fakeResources) ResourceAttribute(_ context.Context, paramType, id, attr string) (string, bool) {
	if paramType == "AWS::EC2::Subnet::Id" && attr == "VpcId" {
		v, ok := f.subnetVPC[id]

		return v, ok
	}

	return "", false
}

func ruleDoc(assert, desc string) string {
	return `{"Rules":{"R":{"Assertions":[{"Assert":` + assert + `,"AssertDescription":"` + desc + `"}]}}}`
}

func TestExtendedTemplateRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params  map[string]string
		name    string
		assert  string
		wantErr string
		noLook  bool
	}{
		{name: "valueof_match", assert: `{"Fn::Equals":[{"Fn::ValueOf":["Subnet","VpcId"]},{"Ref":"VpcId"}]}`},
		{
			name:    "valueof_mismatch",
			params:  map[string]string{"Subnet": "subnet-b"},
			assert:  `{"Fn::Equals":[{"Fn::ValueOf":["Subnet","VpcId"]},{"Ref":"VpcId"}]}`,
			wantErr: `Rule "R" assertion failed: wrong vpc`,
		},
		{
			name:   "valueof_list_all_in_vpc",
			params: map[string]string{"Subnets": "subnet-a,subnet-c"},
			assert: `{"Fn::EachMemberEquals":[{"Fn::ValueOf":["Subnets","VpcId"]},{"Ref":"VpcId"}]}`,
		},
		{
			name:    "valueof_list_one_outside",
			params:  map[string]string{"Subnets": "subnet-a,subnet-b"},
			assert:  `{"Fn::EachMemberEquals":[{"Fn::ValueOf":["Subnets","VpcId"]},{"Ref":"VpcId"}]}`,
			wantErr: "wrong vpc",
		},
		{name: "refall_contains", assert: `{"Fn::Contains":[{"Fn::RefAll":"AWS::EC2::VPC::Id"},{"Ref":"VpcId"}]}`},
		{
			name:    "refall_missing",
			params:  map[string]string{"VpcId": "vpc-9"},
			assert:  `{"Fn::Contains":[{"Fn::RefAll":"AWS::EC2::VPC::Id"},{"Ref":"VpcId"}]}`,
			wantErr: "wrong vpc",
		},
		{
			name: "valueofall_in_refall",
			assert: `{"Fn::EachMemberIn":[{"Fn::ValueOfAll":["AWS::EC2::Subnet::Id","VpcId"]},` +
				`{"Fn::RefAll":"AWS::EC2::VPC::Id"}]}`,
		},
		{
			name:    "not_over_contains",
			assert:  `{"Fn::Not":[{"Fn::Contains":[["vpc-1","vpc-2"],{"Ref":"VpcId"}]}]}`,
			wantErr: "wrong vpc",
		},
		{
			name:   "unresolvable_without_lookup_passes",
			noLook: true, params: map[string]string{"Subnet": "subnet-b"},
			assert: `{"Fn::Equals":[{"Fn::ValueOf":["Subnet","VpcId"]},{"Ref":"VpcId"}]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.engine.templates[networkURL] = networkTemplate

			if !tc.noLook {
				f.backend.SetResourceLookup(fakeResources{
					subnetVPC: map[string]string{"subnet-a": "vpc-1", "subnet-b": "vpc-2", "subnet-c": "vpc-1"},
				})
			}

			c := f.client(t)
			cat := createCatalog(t, c, networkURL)
			addConstraint(t, c, cat, "TEMPLATE", ruleDoc(tc.assert, "wrong vpc"))

			in := &sdk.ProvisionProductInput{
				ProvisionToken: aws.String(token(t)), ProvisionedProductName: aws.String("net"),
				ProductId: aws.String(cat.productID), ProvisioningArtifactId: aws.String(cat.artifactID),
			}
			for k, v := range tc.params {
				in.ProvisioningParameters = append(
					in.ProvisioningParameters,
					sctypes.ProvisioningParameter{Key: aws.String(k), Value: aws.String(v)},
				)
			}

			_, err := c.ProvisionProduct(t.Context(), in)
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, "InvalidParametersException", apiCode(err))
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestTemplateRuleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params string
	}{
		{"unknown_function", ruleDoc(`{"Fn::Frobnicate":["a","b"]}`, "x")},
		{"no_assertions", `{"Rules":{"R":{"Assertions":[]}}}`},
		{"missing_assert", `{"Rules":{"R":{"Assertions":[{"AssertDescription":"x"}]}}}`},
		{"two_functions", ruleDoc(`{"Fn::Equals":["a","a"],"Fn::Not":[true]}`, "x")},
		{"bad_condition", `{"Rules":{"R":{"RuleCondition":{"Fn::Nope":[1]},"Assertions":[{"Assert":true}]}}}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newFixture(t).client(t)
			cat := createCatalog(t, c, templateURL)
			_, err := c.CreateConstraint(t.Context(), &sdk.CreateConstraintInput{
				PortfolioId: aws.String(cat.portfolioID), ProductId: aws.String(cat.productID),
				Type: aws.String("TEMPLATE"), Parameters: aws.String(tc.params), IdempotencyToken: aws.String(token(t)),
			})
			assert.Equal(t, "InvalidParametersException", apiCode(err))
		})
	}
}
