package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestListItemElementNames_RealSDKClient drives each RDS list operation below
// through the real aws-sdk-go-v2 client end to end (create/seed a resource,
// then describe/list it) and asserts the SDK actually sees the item, not just
// that gopherstack returned 200 OK. The SDK's XML deserializer silently drops
// list items whose element name doesn't match its generated case, so a
// wrong-tag bug produces an empty slice here rather than a decode error.
func TestListItemElementNames_RealSDKClient(t *testing.T) {
	t.Parallel()

	t.Run("blue green deployments", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateBlueGreenDeployment(t.Context(), &rdssdk.CreateBlueGreenDeploymentInput{
			BlueGreenDeploymentName: aws.String("bgd1"),
			Source:                  aws.String("arn:aws:rds:us-east-1:123456789012:cluster:src"),
		})
		require.NoError(t, err)

		out, err := client.DescribeBlueGreenDeployments(t.Context(), &rdssdk.DescribeBlueGreenDeploymentsInput{})
		require.NoError(t, err)
		require.Len(t, out.BlueGreenDeployments, 1)
	})

	t.Run("global clusters", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateGlobalCluster(t.Context(), &rdssdk.CreateGlobalClusterInput{
			GlobalClusterIdentifier: aws.String("gc1"),
		})
		require.NoError(t, err)

		out, err := client.DescribeGlobalClusters(t.Context(), &rdssdk.DescribeGlobalClustersInput{})
		require.NoError(t, err)
		require.Len(t, out.GlobalClusters, 1)
	})

	t.Run("db recommendations", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		h.Backend.AddDBRecommendation(rds.DBRecommendation{
			RecommendationID: "rec-1",
			Status:           "active",
		})

		out, err := client.DescribeDBRecommendations(t.Context(), &rdssdk.DescribeDBRecommendationsInput{})
		require.NoError(t, err)
		require.Len(t, out.DBRecommendations, 1)
	})

	t.Run("event subscription source ids", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateEventSubscription(t.Context(), &rdssdk.CreateEventSubscriptionInput{
			SubscriptionName: aws.String("sub1"),
			SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:123456789012:topic1"),
			SourceIds:        []string{"db1"},
		})
		require.NoError(t, err)

		out, err := client.DescribeEventSubscriptions(t.Context(), &rdssdk.DescribeEventSubscriptionsInput{})
		require.NoError(t, err)
		require.Len(t, out.EventSubscriptionsList, 1)
		require.Len(t, out.EventSubscriptionsList[0].SourceIdsList, 1)
	})

	t.Run("db proxy auth", func(t *testing.T) {
		t.Parallel()

		h := newTestRDSHandler()
		client := newTestRDSClient(t, h)

		_, err := client.CreateDBProxy(t.Context(), &rdssdk.CreateDBProxyInput{
			DBProxyName:  aws.String("proxy1"),
			EngineFamily: types.EngineFamilyMysql,
			RoleArn:      aws.String("arn:aws:iam::123456789012:role/proxy-role"),
			VpcSubnetIds: []string{"subnet-1"},
			Auth: []types.UserAuthConfig{
				{
					AuthScheme: types.AuthSchemeSecrets,
					SecretArn:  aws.String("arn:aws:secretsmanager:us-east-1:123456789012:secret:s1"),
				},
			},
		})
		require.NoError(t, err)

		out, err := client.DescribeDBProxies(t.Context(), &rdssdk.DescribeDBProxiesInput{})
		require.NoError(t, err)
		require.Len(t, out.DBProxies, 1)
		require.Len(t, out.DBProxies[0].Auth, 1)
	})
}
