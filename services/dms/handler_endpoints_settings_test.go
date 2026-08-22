package dms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEndpointConnectionSettings_SDKRoundTrip proves CreateEndpoint/
// ModifyEndpoint accept and echo the top-level connection-settings members
// real DMS carries on types.Endpoint (databasemigrationservice@v1.66.4,
// types/types.go): CertificateArn, ExtraConnectionAttributes, KmsKeyId,
// ServiceAccessRoleArn, SslMode, ExternalTableDefinition. These were
// entirely absent from the wire on both directions before this fix --
// a real SDK client setting them got silent drops on Create and, since the
// response struct had no matching fields either, could never observe them
// on any subsequent Describe. KmsKeyId is create-only (real
// ModifyEndpointInput has no KmsKeyId member) and is asserted unchanged by
// Modify.
func TestEndpointConnectionSettings_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	created, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier:        aws.String("settings-ep"),
		EndpointType:              types.ReplicationEndpointTypeValueSource,
		EngineName:                aws.String("mysql"),
		CertificateArn:            aws.String("arn:aws:dms:us-east-1:000000000000:cert:abc123"),
		ExtraConnectionAttributes: aws.String("connectTimeout=30"),
		KmsKeyId:                  aws.String("arn:aws:kms:us-east-1:000000000000:key/abc123"),
		ServiceAccessRoleArn:      aws.String("arn:aws:iam::000000000000:role/dms-access"),
		SslMode:                   types.DmsSslModeValueRequire,
		ExternalTableDefinition:   aws.String("{\"TableCount\":\"1\"}"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Endpoint)

	ep := created.Endpoint
	assert.Equal(
		t,
		"arn:aws:dms:us-east-1:000000000000:cert:abc123",
		aws.ToString(ep.CertificateArn),
	)
	assert.Equal(t, "connectTimeout=30", aws.ToString(ep.ExtraConnectionAttributes))
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc123", aws.ToString(ep.KmsKeyId))
	assert.Equal(
		t,
		"arn:aws:iam::000000000000:role/dms-access",
		aws.ToString(ep.ServiceAccessRoleArn),
	)
	assert.Equal(t, types.DmsSslModeValueRequire, ep.SslMode)
	assert.JSONEq(t, "{\"TableCount\":\"1\"}", aws.ToString(ep.ExternalTableDefinition))

	described, err := client.DescribeEndpoints(t.Context(), &dmssdk.DescribeEndpointsInput{
		Filters: []types.Filter{{Name: aws.String("endpoint-id"), Values: []string{"settings-ep"}}},
	})
	require.NoError(t, err)
	require.Len(t, described.Endpoints, 1)
	assert.Equal(
		t,
		"arn:aws:dms:us-east-1:000000000000:cert:abc123",
		aws.ToString(described.Endpoints[0].CertificateArn),
	)
	assert.Equal(t, types.DmsSslModeValueRequire, described.Endpoints[0].SslMode)

	modified, err := client.ModifyEndpoint(t.Context(), &dmssdk.ModifyEndpointInput{
		EndpointArn:               ep.EndpointArn,
		CertificateArn:            aws.String("arn:aws:dms:us-east-1:000000000000:cert:new456"),
		ExtraConnectionAttributes: aws.String("connectTimeout=60"),
		ServiceAccessRoleArn:      aws.String("arn:aws:iam::000000000000:role/dms-access-2"),
		SslMode:                   types.DmsSslModeValueVerifyFull,
		ExternalTableDefinition:   aws.String("{\"TableCount\":\"2\"}"),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"arn:aws:dms:us-east-1:000000000000:cert:new456",
		aws.ToString(modified.Endpoint.CertificateArn),
	)
	assert.Equal(t, "connectTimeout=60", aws.ToString(modified.Endpoint.ExtraConnectionAttributes))
	assert.Equal(
		t,
		"arn:aws:iam::000000000000:role/dms-access-2",
		aws.ToString(modified.Endpoint.ServiceAccessRoleArn),
	)
	assert.Equal(t, types.DmsSslModeValueVerifyFull, modified.Endpoint.SslMode)
	assert.JSONEq(t, "{\"TableCount\":\"2\"}", aws.ToString(modified.Endpoint.ExternalTableDefinition))
	assert.Equal(
		t,
		"arn:aws:kms:us-east-1:000000000000:key/abc123",
		aws.ToString(modified.Endpoint.KmsKeyId),
		"KmsKeyId is create-only in the real API; Modify must not clear it",
	)
}

// TestEndpointSslMode_DefaultsToNone proves an Endpoint created without an
// explicit SslMode reports the real default value "none"
// (api_op_CreateEndpoint.go: "The SSL mode used to connect to the endpoint.
// The default value is none"), matching DmsSslModeValueNone rather than an
// empty string.
func TestEndpointSslMode_DefaultsToNone(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	created, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("default-ssl-ep"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("postgres"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.DmsSslModeValueNone, created.Endpoint.SslMode)
}

// TestEndpointSslMode_InvalidRejected proves an out-of-range SslMode is
// rejected with ValidationException rather than silently stored, matching
// types.DmsSslModeValue.Values() (none|require|verify-ca|verify-full).
func TestEndpointSslMode_InvalidRejected(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	_, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("bad-ssl-ep"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
		SslMode:            "bogus-mode",
	})
	require.Error(t, err)
}

// TestReplicationInstanceSettings_SDKRoundTrip proves CreateReplicationInstance/
// ModifyReplicationInstance accept and echo the top-level settings members
// real DMS carries on types.ReplicationInstance (databasemigrationservice
// @v1.66.4, types/types.go): KmsKeyId, DnsNameServers, NetworkType,
// PreferredMaintenanceWindow. These were entirely absent from the wire on
// both directions before this fix -- a real SDK client setting them got
// silent drops on Create, and the response struct had no matching fields
// either. KmsKeyId is create-only (real ModifyReplicationInstanceInput has
// no KmsKeyId member) and is asserted unchanged by Modify.
func TestReplicationInstanceSettings_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	created, err := client.CreateReplicationInstance(
		t.Context(),
		&dmssdk.CreateReplicationInstanceInput{
			ReplicationInstanceIdentifier: aws.String("settings-ri"),
			ReplicationInstanceClass:      aws.String("dms.t3.micro"),
			KmsKeyId: aws.String(
				"arn:aws:kms:us-east-1:000000000000:key/ri-key",
			),
			DnsNameServers:             aws.String("10.0.0.2 10.0.0.3"),
			NetworkType:                aws.String("IPV4"),
			PreferredMaintenanceWindow: aws.String("sun:05:00-sun:06:00"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, created.ReplicationInstance)

	ri := created.ReplicationInstance
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/ri-key", aws.ToString(ri.KmsKeyId))
	assert.Equal(t, "10.0.0.2 10.0.0.3", aws.ToString(ri.DnsNameServers))
	assert.Equal(t, "IPV4", aws.ToString(ri.NetworkType))
	assert.Equal(t, "sun:05:00-sun:06:00", aws.ToString(ri.PreferredMaintenanceWindow))

	modified, err := client.ModifyReplicationInstance(
		t.Context(),
		&dmssdk.ModifyReplicationInstanceInput{
			ReplicationInstanceArn:     ri.ReplicationInstanceArn,
			NetworkType:                aws.String("DUAL"),
			PreferredMaintenanceWindow: aws.String("mon:05:00-mon:06:00"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "DUAL", aws.ToString(modified.ReplicationInstance.NetworkType))
	assert.Equal(
		t,
		"mon:05:00-mon:06:00",
		aws.ToString(modified.ReplicationInstance.PreferredMaintenanceWindow),
	)
	assert.Equal(
		t,
		"arn:aws:kms:us-east-1:000000000000:key/ri-key",
		aws.ToString(modified.ReplicationInstance.KmsKeyId),
		"KmsKeyId is create-only in the real API; Modify must not clear it",
	)
}

// TestDescribeOrderableReplicationInstances_ReleaseStatus proves ReleaseStatus
// is one of the real types.ReleaseStatusValues enum members -- "beta" or
// "prod" (databasemigrationservice@v1.66.4, types/enums.go:628-634) -- rather
// than the fabricated value "GA", which is not a member of this enum at all.
func TestDescribeOrderableReplicationInstances_ReleaseStatus(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	out, err := client.DescribeOrderableReplicationInstances(
		t.Context(), &dmssdk.DescribeOrderableReplicationInstancesInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, out.OrderableReplicationInstances)

	for _, inst := range out.OrderableReplicationInstances {
		assert.Contains(
			t,
			[]types.ReleaseStatusValues{types.ReleaseStatusValuesBeta, types.ReleaseStatusValuesProd},
			inst.ReleaseStatus,
			"ReleaseStatus must be a real ReleaseStatusValues member, not a fabricated value like GA",
		)
	}
}
