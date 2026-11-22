package app

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/acm"
	acmtypes "github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigwv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/mq"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchtypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/aws/aws-sdk-go-v2/service/personalize"
	personalizetypes "github.com/aws/aws-sdk-go-v2/service/personalize/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	redshifttypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/aws/aws-sdk-go-v2/service/transfer"
	transfertypes "github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/aws/aws-sdk-go-v2/service/wafv2"
	wafv2types "github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/aws/aws-sdk-go-v2/service/xray"
	xraytypes "github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/stretchr/testify/require"
)

func apigatewayInventoryCase() inventoryCase {
	return inventoryCase{
		name: "apigateway",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			out, err := apigateway.NewFromConfig(cfg).CreateRestApi(ctx, &apigateway.CreateRestApiInput{
				Name: aws.String(name), Tags: inventoryTagMap(tagged),
			})
			require.NoError(t, err)

			return inventoryARNNoAccount("apigateway", "/restapis/"+aws.ToString(out.Id))
		},
	}
}

func apigatewayv2InventoryCase() inventoryCase {
	return inventoryCase{
		name: "apigatewayv2",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			out, err := apigatewayv2.NewFromConfig(cfg).CreateApi(ctx, &apigatewayv2.CreateApiInput{
				Name: aws.String(name), ProtocolType: apigwv2types.ProtocolTypeHttp, Tags: inventoryTagMap(tagged),
			})
			require.NoError(t, err)

			return inventoryARNNoAccount("apigateway", "/apis/"+aws.ToString(out.ApiId))
		},
	}
}

func acmInventoryCase() inventoryCase {
	return inventoryCase{
		name: "acm",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &acm.RequestCertificateInput{DomainName: aws.String(name + ".example.com")}
			if tagged {
				in.Tags = []acmtypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := acm.NewFromConfig(cfg).RequestCertificate(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.CertificateArn)
		},
	}
}

func opensearchInventoryCase() inventoryCase {
	return inventoryCase{
		name: "opensearch",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &opensearch.CreateDomainInput{DomainName: aws.String(name)}
			if tagged {
				in.TagList = []opensearchtypes.Tag{
					{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)},
				}
			}

			out, err := opensearch.NewFromConfig(cfg).CreateDomain(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.DomainStatus.ARN)
		},
	}
}

func redshiftInventoryCase() inventoryCase {
	return inventoryCase{
		name: "redshift",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			c := redshift.NewFromConfig(cfg)
			_, err := c.CreateCluster(ctx, &redshift.CreateClusterInput{
				ClusterIdentifier: aws.String(name), NodeType: aws.String("dc2.large"),
				MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Passw0rd1234"),
			})
			require.NoError(t, err)

			clusterARN := inventoryARN("redshift", "cluster:"+name)
			if tagged {
				_, err = c.CreateTags(ctx, &redshift.CreateTagsInput{
					ResourceName: aws.String(clusterARN),
					Tags: []redshifttypes.Tag{
						{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)},
					},
				})
				require.NoError(t, err)
			}

			return clusterARN
		},
	}
}

func mqInventoryCase() inventoryCase {
	return inventoryCase{
		name: "mq",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			out, err := mq.NewFromConfig(cfg).CreateConfiguration(ctx, &mq.CreateConfigurationInput{
				Name: aws.String(name), EngineType: "ACTIVEMQ", EngineVersion: aws.String("5.17.6"),
				Tags: inventoryTagMap(tagged),
			})
			require.NoError(t, err)

			return aws.ToString(out.Arn)
		},
	}
}

func cognitoInventoryCase() inventoryCase {
	return inventoryCase{
		name: "cognitoidp",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			out, err := cognitoidentityprovider.NewFromConfig(cfg).CreateUserPool(ctx,
				&cognitoidentityprovider.CreateUserPoolInput{
					PoolName:     aws.String(name),
					UserPoolTags: inventoryTagMap(tagged),
				})
			require.NoError(t, err)

			return aws.ToString(out.UserPool.Arn)
		},
	}
}

func wafv2InventoryCase() inventoryCase {
	return inventoryCase{
		name: "wafv2",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &wafv2.CreateIPSetInput{
				Name: aws.String(name), Scope: wafv2types.ScopeRegional,
				IPAddressVersion: wafv2types.IPAddressVersionIpv4, Addresses: []string{"10.0.0.0/8"},
			}
			if tagged {
				in.Tags = []wafv2types.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := wafv2.NewFromConfig(cfg).CreateIPSet(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.Summary.ARN)
		},
	}
}

func appsyncInventoryCase() inventoryCase {
	return inventoryCase{
		name: "appsync",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			out, err := appsync.NewFromConfig(cfg).CreateGraphqlApi(ctx, &appsync.CreateGraphqlApiInput{
				Name: aws.String(name), AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
				Tags: inventoryTagMap(tagged),
			})
			require.NoError(t, err)

			return aws.ToString(out.GraphqlApi.Arn)
		},
	}
}

func transferInventoryCase() inventoryCase {
	return inventoryCase{
		name: "transfer",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, _ string, tagged bool) string {
			t.Helper()

			in := &transfer.CreateServerInput{}
			if tagged {
				in.Tags = []transfertypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := transfer.NewFromConfig(cfg).CreateServer(ctx, in)
			require.NoError(t, err)

			return inventoryARN("transfer", "server/"+aws.ToString(out.ServerId))
		},
	}
}

func xrayInventoryCase() inventoryCase {
	return inventoryCase{
		name: "xray",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &xray.CreateGroupInput{GroupName: aws.String(name)}
			if tagged {
				in.Tags = []xraytypes.Tag{{Key: aws.String(inventoryTagKey), Value: aws.String(inventoryTagValue)}}
			}

			out, err := xray.NewFromConfig(cfg).CreateGroup(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.Group.GroupARN)
		},
	}
}

func personalizeInventoryCase() inventoryCase {
	return inventoryCase{
		name: "personalize",
		create: func(ctx context.Context, t *testing.T, cfg aws.Config, name string, tagged bool) string {
			t.Helper()

			in := &personalize.CreateDatasetGroupInput{Name: aws.String(name)}
			if tagged {
				in.Tags = []personalizetypes.Tag{
					{TagKey: aws.String(inventoryTagKey), TagValue: aws.String(inventoryTagValue)},
				}
			}

			out, err := personalize.NewFromConfig(cfg).CreateDatasetGroup(ctx, in)
			require.NoError(t, err)

			return aws.ToString(out.DatasetGroupArn)
		},
	}
}
