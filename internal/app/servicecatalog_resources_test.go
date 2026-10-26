package app

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scRuleTemplate(paramType string) string {
	return fmt.Sprintf(`{"Parameters":{"P":{"Type":%q}},"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`, paramType)
}

func scRule(assert string) string {
	return `{"Rules":{"R":{"Assertions":[{"Assert":` + assert + `,"AssertDescription":"not in account"}]}}}`
}

func TestServiceCatalogRulesResolveAWSSpecificParameterTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		paramType string
		assert    string
		good, bad string
	}{
		{
			name: "key_pair", paramType: "AWS::EC2::KeyPair::KeyName", good: "sc-key", bad: "other-key",
			assert: `{"Fn::Contains":[{"Fn::RefAll":"AWS::EC2::KeyPair::KeyName"},{"Ref":"P"}]}`,
		},
		{
			name: "availability_zone", paramType: "AWS::EC2::AvailabilityZone::Name",
			good: "us-east-1b", bad: "mars-1a",
			assert: `{"Fn::Contains":[{"Fn::RefAll":"AWS::EC2::AvailabilityZone::Name"},{"Ref":"P"}]}`,
		},
		{
			name: "image", paramType: "AWS::EC2::Image::Id", good: "ami-0c55b159cbfafe1f0", bad: "ami-missing",
			assert: `{"Fn::Contains":[{"Fn::RefAll":"AWS::EC2::Image::Id"},{"Ref":"P"}]}`,
		},
		{
			name: "security_group_name", paramType: "AWS::EC2::SecurityGroup::GroupName",
			good: "sc-group", bad: "no-group",
			assert: `{"Fn::Contains":[{"Fn::RefAll":"AWS::EC2::SecurityGroup::GroupName"},{"Ref":"P"}]}`,
		},
		{
			name: "volume_value_of", paramType: "AWS::EC2::Volume::Id", good: "@volume", bad: "@volume2",
			assert: `{"Fn::Equals":[{"Fn::ValueOf":["P","AvailabilityZone"]},"us-east-1a"]}`,
		},
		{
			name: "hosted_zone_value_of", paramType: "AWS::Route53::HostedZone::Id", good: "@zone", bad: "@zone2",
			assert: `{"Fn::Equals":[{"Fn::ValueOf":["P","Name"]},"sc.example.com."]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			ctx := t.Context()
			ec2c, r53 := ec2.NewFromConfig(fx.cfg), route53.NewFromConfig(fx.cfg)

			_, err := ec2c.CreateKeyPair(ctx, &ec2.CreateKeyPairInput{KeyName: aws.String("sc-key")})
			require.NoError(t, err)

			_, err = ec2c.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
				GroupName: aws.String("sc-group"), Description: aws.String("d"),
			})
			require.NoError(t, err)

			vol, err := ec2c.CreateVolume(ctx, &ec2.CreateVolumeInput{
				AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(1), VolumeType: ec2types.VolumeTypeGp3,
			})
			require.NoError(t, err)

			zone, err := r53.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
				Name: aws.String("sc.example.com."), CallerReference: aws.String("sc-zone"),
			})
			require.NoError(t, err)

			vol2, err := ec2c.CreateVolume(ctx, &ec2.CreateVolumeInput{
				AvailabilityZone: aws.String("us-east-1b"), Size: aws.Int32(1), VolumeType: ec2types.VolumeTypeGp3,
			})
			require.NoError(t, err)

			zone2, err := r53.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
				Name: aws.String("other.example.com."), CallerReference: aws.String("sc-zone2"),
			})
			require.NoError(t, err)

			ids := map[string]string{
				"@volume":  aws.ToString(vol.VolumeId),
				"@volume2": aws.ToString(vol2.VolumeId),
				"@zone": strings.TrimPrefix(
					aws.ToString(zone.HostedZone.Id),
					"/hostedzone/",
				),
				"@zone2": strings.TrimPrefix(aws.ToString(zone2.HostedZone.Id), "/hostedzone/"),
			}
			resolve := func(v string) string {
				if id, ok := ids[v]; ok {
					return id
				}

				return v
			}
			good, bad := resolve(tt.good), resolve(tt.bad)

			cat := newSCCatalog(t, fx, scRuleTemplate(tt.paramType))
			cat.constrain(t, "TEMPLATE", scRule(tt.assert))

			launch := func(name, value string) error {
				_, perr := cat.sc.ProvisionProduct(ctx, &servicecatalog.ProvisionProductInput{
					ProvisionToken: aws.String("tok-" + name), ProvisionedProductName: aws.String(name),
					ProductId: cat.product, ProvisioningArtifactId: cat.artifact,
					ProvisioningParameters: []sctypes.ProvisioningParameter{
						{Key: aws.String("P"), Value: aws.String(value)},
					},
				})

				return perr
			}

			require.NoError(t, launch("good", good))

			err = launch("bad", bad)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not in account")
		})
	}
}
