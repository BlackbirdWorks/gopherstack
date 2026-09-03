package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeDBInstances_OptionGroupMemberships_RealClient covers a layer-3
// bug (gopherstack-g8k9): DBInstance.OptionGroupName is real, settable state
// -- CreateDBInstance and ModifyDBInstance both store it (db_instances.go) --
// but toXMLInstance never emitted it in any response. A real client's
// DBInstance.OptionGroupMemberships was always empty regardless of what
// OptionGroupName had been set at creation. Real wrapper/item shape confirmed
// against rds@v1.124.1 deserializers.go: DescribeDBInstances' DBInstance
// deserializer reads "OptionGroupMemberships" (case-insensitive query
// protocol) wrapping a list of OptionGroupMembership{OptionGroupName,Status}
// (deserializers.go:48533, 48554).
func TestDescribeDBInstances_OptionGroupMemberships_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("db-with-option-group"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		OptionGroupName:      aws.String("my-custom-og"),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBInstances(ctx, &rdssdk.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("db-with-option-group"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBInstances, 1)

	inst := out.DBInstances[0]
	require.Len(t, inst.OptionGroupMemberships, 1,
		"OptionGroupMemberships must round-trip through Create -> Describe; pre-fix it was always empty")
	assert.Equal(t, "my-custom-og", aws.ToString(inst.OptionGroupMemberships[0].OptionGroupName))
	assert.Equal(t, "in-sync", aws.ToString(inst.OptionGroupMemberships[0].Status))
}

// TestDescribeDBClusters_HTTPEndpointEnabled_RealClient covers a layer-3 bug
// (gopherstack-g8k9): DBCluster.HTTPEndpointEnabled is real, live-toggled
// state -- EnableHttpEndpoint/DisableHttpEndpoint (data_api.go) flip it, and
// the RDS Data API presumably gates on it -- but toXMLCluster never emitted
// it, so a real client's DBCluster.HttpEndpointEnabled was always false
// (the Go zero value) regardless of what EnableHttpEndpoint had set. Real
// field name "HttpEndpointEnabled" confirmed against rds@v1.124.1
// deserializers.go's awsAwsquery_deserializeDocumentDBCluster.
func TestDescribeDBClusters_HTTPEndpointEnabled_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("http-endpoint-cluster"),
		Engine:              aws.String("aurora-postgresql"),
		MasterUsername:      aws.String("admin"),
	})
	require.NoError(t, err)

	describeBefore, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("http-endpoint-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, describeBefore.DBClusters, 1)
	assert.False(t, aws.ToBool(describeBefore.DBClusters[0].HttpEndpointEnabled))

	_, err = client.EnableHttpEndpoint(ctx, &rdssdk.EnableHttpEndpointInput{
		ResourceArn: describeBefore.DBClusters[0].DBClusterArn,
	})
	require.NoError(t, err)

	describeAfter, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("http-endpoint-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, describeAfter.DBClusters, 1)
	assert.True(t, aws.ToBool(describeAfter.DBClusters[0].HttpEndpointEnabled),
		"HttpEndpointEnabled must reflect EnableHttpEndpoint; pre-fix DescribeDBClusters never emitted it at all")
}

// TestDescribeEvents_SourceArn_RealClient covers a layer-3 bug (gopherstack-g8k9):
// Event.SourceArn is derivable from state the backend already holds -- the
// exact same (resource-type-token, identifier) pair that DBInstanceArn/
// DBClusterArn are built from elsewhere via the shared rdsARN helper -- but
// publishInstanceEventLocked/publishClusterEventLocked never set it, so
// DescribeEvents' SourceArn was always nil regardless of which resource an
// event was about. Real field name "SourceArn" confirmed against
// rds@v1.124.1 deserializers.go's Event EqualFold list.
func TestDescribeEvents_SourceArn_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	createOut, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("db-events-source-arn"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
	})
	require.NoError(t, err)
	wantArn := aws.ToString(createOut.DBInstance.DBInstanceArn)
	require.NotEmpty(t, wantArn)

	out, err := client.DescribeEvents(ctx, &rdssdk.DescribeEventsInput{
		SourceIdentifier: aws.String("db-events-source-arn"),
		SourceType:       rdstypes.SourceTypeDbInstance,
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Events)

	ev := out.Events[0]
	require.NotNil(t, ev.SourceArn,
		"SourceArn must be populated; pre-fix DescribeEvents never emitted it")
	assert.Equal(t, wantArn, aws.ToString(ev.SourceArn))
}

// TestDescribeEventSubscriptions_CustomerAwsId_RealClient covers a layer-3
// bug (gopherstack-g8k9): CustomerAwsId is the caller's account ID, already
// held by the backend (used to build every other ARN it emits), but
// toXMLEventSubscription never set it. Real field name "CustomerAwsId"
// confirmed against rds@v1.124.1 deserializers.go's EventSubscription
// EqualFold list.
func TestDescribeEventSubscriptions_CustomerAwsId_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateEventSubscription(ctx, &rdssdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("sub-customer-aws-id"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:123456789012:topic1"),
	})
	require.NoError(t, err)

	out, err := client.DescribeEventSubscriptions(ctx, &rdssdk.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("sub-customer-aws-id"),
	})
	require.NoError(t, err)
	require.Len(t, out.EventSubscriptionsList, 1)

	require.NotNil(t, out.EventSubscriptionsList[0].CustomerAwsId,
		"CustomerAwsId must be populated; pre-fix it was never emitted")
	assert.Equal(t, "123456789012", aws.ToString(out.EventSubscriptionsList[0].CustomerAwsId))
}

// TestCreateDBProxy_VpcConfig_RealClient covers a layer-3 bug (gopherstack-g8k9):
// VpcSubnetIds is a REQUIRED CreateDBProxyInput member (rds@v1.124.1
// validators.go's validateOpCreateDBProxyInput -- a real client refuses to
// send the request without it) and VpcSecurityGroupIds is a real sibling
// member, but the handler never read either from the form, and the domain
// DBProxy struct's VpcSubnetIDs/VpcSecurityGroupIDs fields (declared, but
// never populated) were never emitted either. Every real DBProxy's VPC
// placement was silently dropped on both the request and response side.
func TestCreateDBProxy_VpcConfig_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBProxy(ctx, &rdssdk.CreateDBProxyInput{
		DBProxyName:         aws.String("proxy-vpc-config"),
		EngineFamily:        rdstypes.EngineFamilyMysql,
		RoleArn:             aws.String("arn:aws:iam::123456789012:role/proxy-role"),
		VpcSubnetIds:        []string{"subnet-1", "subnet-2"},
		VpcSecurityGroupIds: []string{"sg-1"},
		Auth: []rdstypes.UserAuthConfig{
			{
				AuthScheme: rdstypes.AuthSchemeSecrets,
				SecretArn:  aws.String("arn:aws:secretsmanager:us-east-1:123456789012:secret:s1"),
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDBProxies(ctx, &rdssdk.DescribeDBProxiesInput{
		DBProxyName: aws.String("proxy-vpc-config"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBProxies, 1)

	proxy := out.DBProxies[0]
	require.ElementsMatch(t, []string{"subnet-1", "subnet-2"}, proxy.VpcSubnetIds,
		"VpcSubnetIds must round-trip; pre-fix it was never captured from the request nor emitted")
	require.ElementsMatch(t, []string{"sg-1"}, proxy.VpcSecurityGroupIds,
		"VpcSecurityGroupIds must round-trip; pre-fix it was never captured from the request nor emitted")
}

// TestRegisterDBProxyTargets_TargetHealth_RealClient covers a layer-2 bug:
// TargetHealth is a nested {State,Reason,Description} object on the wire
// (rds@v1.124.1 deserializers.go's TargetHealth EqualFold list), but
// gopherstack emitted it as a flat string. A real client's decoder never
// finds the nested State child element, so TargetHealth.State was always
// nil regardless of the backend's tracked health value.
func TestRegisterDBProxyTargets_TargetHealth_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBProxy(ctx, &rdssdk.CreateDBProxyInput{
		DBProxyName:  aws.String("proxy-target-health"),
		EngineFamily: rdstypes.EngineFamilyMysql,
		RoleArn:      aws.String("arn:aws:iam::123456789012:role/proxy-role"),
		VpcSubnetIds: []string{"subnet-1"},
	})
	require.NoError(t, err)

	_, err = client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("db-proxy-target"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
	})
	require.NoError(t, err)

	registerOut, err := client.RegisterDBProxyTargets(ctx, &rdssdk.RegisterDBProxyTargetsInput{
		DBProxyName:           aws.String("proxy-target-health"),
		DBInstanceIdentifiers: []string{"db-proxy-target"},
	})
	require.NoError(t, err)
	require.Len(t, registerOut.DBProxyTargets, 1)

	require.NotNil(t, registerOut.DBProxyTargets[0].TargetHealth,
		"TargetHealth must be populated as a nested object")
	assert.Equal(t, "AVAILABLE", string(registerOut.DBProxyTargets[0].TargetHealth.State),
		"TargetHealth.State must round-trip; pre-fix TargetHealth was emitted flat, so a real "+
			"client's nested-field decode never found the child State element")
}

// TestModifyDBProxyTargetGroup_SessionPinningFilters_RealClient covers a
// layer-3 bug (gopherstack-g8k9): ConnectionPoolConfig.SessionPinningFilters
// is a declared field on the domain ConnectionPoolConfig struct, but the
// handler never read it from the ModifyDBProxyTargetGroup request
// (ConnectionPoolConfig.SessionPinningFilters.member.N per rds@v1.124.1
// serializers.go's awsAwsquery_serializeDocumentConnectionPoolConfiguration)
// nor emitted it in DescribeDBProxyTargetGroups.
func TestModifyDBProxyTargetGroup_SessionPinningFilters_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBProxy(ctx, &rdssdk.CreateDBProxyInput{
		DBProxyName:  aws.String("proxy-session-pinning"),
		EngineFamily: rdstypes.EngineFamilyMysql,
		RoleArn:      aws.String("arn:aws:iam::123456789012:role/proxy-role"),
		VpcSubnetIds: []string{"subnet-1"},
	})
	require.NoError(t, err)

	_, err = client.ModifyDBProxyTargetGroup(ctx, &rdssdk.ModifyDBProxyTargetGroupInput{
		DBProxyName:     aws.String("proxy-session-pinning"),
		TargetGroupName: aws.String("default"),
		ConnectionPoolConfig: &rdstypes.ConnectionPoolConfiguration{
			SessionPinningFilters: []string{"EXCLUDE_VARIABLE_SETS"},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDBProxyTargetGroups(ctx, &rdssdk.DescribeDBProxyTargetGroupsInput{
		DBProxyName: aws.String("proxy-session-pinning"),
	})
	require.NoError(t, err)
	require.Len(t, out.TargetGroups, 1)

	assert.Equal(t, []string{"EXCLUDE_VARIABLE_SETS"}, out.TargetGroups[0].ConnectionPoolConfig.SessionPinningFilters,
		"SessionPinningFilters must round-trip; pre-fix it was never captured from the request nor emitted")
}

// TestDescribeDBClusterSnapshotAttributes_WrapperItemName_RealClient covers a
// gopherstack-6flj sibling-trap bug: DescribeDBClusterSnapshotAttributes and
// ModifyDBClusterSnapshotAttribute reused the plain-snapshot
// xmlDBSnapshotAttributeList type, whose member element is "DBSnapshotAttribute"
// -- correct for DescribeDBSnapshotAttributes, but the real
// DescribeDBClusterSnapshotAttributesOutput deserializer
// (rds@v1.124.1 deserializers.go:33216,
// awsAwsquery_deserializeDocumentDBClusterSnapshotAttributeList) reads the
// distinct element name "DBClusterSnapshotAttribute". The wrapper key itself
// ("DBClusterSnapshotAttributes") was already correct, so a real client's
// DBClusterSnapshotAttributes slice was always empty regardless of what
// ModifyDBClusterSnapshotAttribute had set.
func TestDescribeDBClusterSnapshotAttributes_WrapperItemName_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("cluster-snap-attrs"),
		Engine:              aws.String("aurora-postgresql"),
		MasterUsername:      aws.String("admin"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterSnapshot(ctx, &rdssdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("cluster-snap-1"),
		DBClusterIdentifier:         aws.String("cluster-snap-attrs"),
	})
	require.NoError(t, err)

	_, err = client.ModifyDBClusterSnapshotAttribute(ctx, &rdssdk.ModifyDBClusterSnapshotAttributeInput{
		DBClusterSnapshotIdentifier: aws.String("cluster-snap-1"),
		AttributeName:               aws.String("restore"),
		ValuesToAdd:                 []string{"123456789012"},
	})
	require.NoError(t, err)

	out, err := client.DescribeDBClusterSnapshotAttributes(ctx, &rdssdk.DescribeDBClusterSnapshotAttributesInput{
		DBClusterSnapshotIdentifier: aws.String("cluster-snap-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBClusterSnapshotAttributesResult)

	attrs := out.DBClusterSnapshotAttributesResult.DBClusterSnapshotAttributes
	require.Len(t, attrs, 1,
		"DBClusterSnapshotAttributes must round-trip; pre-fix the real deserializer's "+
			"element name never matched the emitted one, so this was always empty")
	assert.Equal(t, "restore", aws.ToString(attrs[0].AttributeName))
	assert.Equal(t, []string{"123456789012"}, attrs[0].AttributeValues)
}

// TestModifyDBSnapshotAttribute_ValuesToAddWireKey_RealClient covers a
// request-parsing sibling of the bug above: handleModifyDBSnapshotAttribute
// read "ValuesToAdd.member.N", but the real client serializes
// ValuesToAdd/ValuesToRemove with the list member's locationName
// "AttributeValue" (rds@v1.124.1 serializers.go:11546,
// awsAwsquery_serializeDocumentAttributeValueList's value.Array("AttributeValue")),
// giving "ValuesToAdd.AttributeValue.N" -- a real client's ValuesToAdd was
// silently dropped on every call regardless of what was requested.
func TestModifyDBSnapshotAttribute_ValuesToAddWireKey_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("db-snap-attrs"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBSnapshot(ctx, &rdssdk.CreateDBSnapshotInput{
		DBSnapshotIdentifier: aws.String("db-snap-1"),
		DBInstanceIdentifier: aws.String("db-snap-attrs"),
	})
	require.NoError(t, err)

	_, err = client.ModifyDBSnapshotAttribute(ctx, &rdssdk.ModifyDBSnapshotAttributeInput{
		DBSnapshotIdentifier: aws.String("db-snap-1"),
		AttributeName:        aws.String("restore"),
		ValuesToAdd:          []string{"123456789012"},
	})
	require.NoError(t, err)

	out, err := client.DescribeDBSnapshotAttributes(ctx, &rdssdk.DescribeDBSnapshotAttributesInput{
		DBSnapshotIdentifier: aws.String("db-snap-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBSnapshotAttributesResult)

	attrs := out.DBSnapshotAttributesResult.DBSnapshotAttributes
	require.Len(t, attrs, 1,
		"DBSnapshotAttributes must round-trip; pre-fix ValuesToAdd was parsed from the wrong "+
			"wire key so no attribute was ever recorded")
	assert.Equal(t, "restore", aws.ToString(attrs[0].AttributeName))
	assert.Equal(t, []string{"123456789012"}, attrs[0].AttributeValues)
}

// TestCreateDBProxyEndpoint_VpcConfig_RealClient covers a layer-3 bug
// (gopherstack-g8k9): CreateDBProxyEndpoint already correctly captures
// VpcSubnetIds/VpcSecurityGroupIds from the request into the domain
// DBProxyEndpoint struct, but toXMLProxyEndpoint never emitted either --
// a pure response-side gap, the cleanest form of this bug class since the
// request side already worked.
func TestCreateDBProxyEndpoint_VpcConfig_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBProxy(ctx, &rdssdk.CreateDBProxyInput{
		DBProxyName:  aws.String("proxy-endpoint-vpc"),
		EngineFamily: rdstypes.EngineFamilyMysql,
		RoleArn:      aws.String("arn:aws:iam::123456789012:role/proxy-role"),
		VpcSubnetIds: []string{"subnet-1"},
	})
	require.NoError(t, err)

	_, err = client.CreateDBProxyEndpoint(ctx, &rdssdk.CreateDBProxyEndpointInput{
		DBProxyEndpointName: aws.String("proxy-endpoint-vpc-ep"),
		DBProxyName:         aws.String("proxy-endpoint-vpc"),
		VpcSubnetIds:        []string{"subnet-2", "subnet-3"},
		VpcSecurityGroupIds: []string{"sg-2"},
	})
	require.NoError(t, err)

	out, err := client.DescribeDBProxyEndpoints(ctx, &rdssdk.DescribeDBProxyEndpointsInput{
		DBProxyEndpointName: aws.String("proxy-endpoint-vpc-ep"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBProxyEndpoints, 1)

	ep := out.DBProxyEndpoints[0]
	require.ElementsMatch(t, []string{"subnet-2", "subnet-3"}, ep.VpcSubnetIds,
		"VpcSubnetIds must round-trip; pre-fix DescribeDBProxyEndpoints never emitted it")
	require.ElementsMatch(t, []string{"sg-2"}, ep.VpcSecurityGroupIds,
		"VpcSecurityGroupIds must round-trip; pre-fix DescribeDBProxyEndpoints never emitted it")
}

// TestDescribeAccountAttributes_QuotaFields_RealClient covers a per-item
// field-name bug: xmlAccountAttribute swapped its wire tags. AccountQuota's
// real name field ("AccountQuotaName", rds@v1.124.1 deserializers.go's
// awsAwsquery_deserializeDocumentAccountQuota) was emitted under the Go
// field tagged "AttributeName" -- a name the real deserializer never
// matches -- while the tag "AccountQuotaName" was applied to the numeric
// Used count instead of the real "Used" member. Pre-fix, a real client's
// AccountQuotaName decoded a stringified count and Used stayed nil.
func TestDescribeAccountAttributes_QuotaFields_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	out, err := client.DescribeAccountAttributes(ctx, &rdssdk.DescribeAccountAttributesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.AccountQuotas)

	var found bool
	for _, q := range out.AccountQuotas {
		if aws.ToString(q.AccountQuotaName) == "DBInstances" {
			found = true
			assert.NotNil(t, q.Used, "Used must round-trip; pre-fix it was permanently nil")
			assert.NotEqual(t, int64(0), aws.ToInt64(q.Max), "Max must be non-zero for DBInstances quota")

			break
		}
	}
	require.True(t, found, "AccountQuotaName must round-trip; pre-fix it was emitted under the wrong tag")
}
