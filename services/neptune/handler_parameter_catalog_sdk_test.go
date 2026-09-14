package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/neptune/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// Test_SDKRoundTrip_DescribeEngineDefaultParameters_WireShape proves the real
// SDK client can deserialize the (now non-empty, real-catalog) engine
// default parameters, and that the fields the real Parameter type carries
// (types/types.go:1320) actually come through: ApplyType, DataType,
// AllowedValues, IsModifiable, Source.
func Test_SDKRoundTrip_DescribeEngineDefaultParameters_WireShape(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	out, err := client.DescribeEngineDefaultParameters(
		t.Context(), &neptunesdk.DescribeEngineDefaultParametersInput{DBParameterGroupFamily: aws.String("neptune1.3")},
	)
	require.NoError(t, err)

	var dfe *types.Parameter
	for i := range out.EngineDefaults.Parameters {
		if aws.ToString(out.EngineDefaults.Parameters[i].ParameterName) == "neptune_dfe_query_engine" {
			dfe = &out.EngineDefaults.Parameters[i]
		}
	}
	require.NotNil(t, dfe, "neptune_dfe_query_engine (a real instance-level parameter) must be present")
	assert.Equal(t, "static", aws.ToString(dfe.ApplyType))
	assert.Equal(t, "string", aws.ToString(dfe.DataType))
	assert.Equal(t, "enabled,viaQueryHint", aws.ToString(dfe.AllowedValues))
	assert.True(t, aws.ToBool(dfe.IsModifiable))
	assert.Equal(t, "engine-default", aws.ToString(dfe.Source))

	for i := range out.EngineDefaults.Parameters {
		assert.NotEqual(t, "neptune_streams_expiry_days", aws.ToString(out.EngineDefaults.Parameters[i].ParameterName),
			"a cluster-only parameter must not leak into the instance-level engine defaults")
	}
}

// Test_SDKRoundTrip_DescribeEngineDefaultClusterParameters_WireShape is the
// cluster-level counterpart: neptune_streams_expiry_days is cluster-only and
// carries a real MinimumEngineVersion (introduced in engine 1.2.0.0).
func Test_SDKRoundTrip_DescribeEngineDefaultClusterParameters_WireShape(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	out, err := client.DescribeEngineDefaultClusterParameters(
		t.Context(),
		&neptunesdk.DescribeEngineDefaultClusterParametersInput{DBParameterGroupFamily: aws.String("neptune1.3")},
	)
	require.NoError(t, err)

	var expiry *types.Parameter
	for i := range out.EngineDefaults.Parameters {
		if aws.ToString(out.EngineDefaults.Parameters[i].ParameterName) == "neptune_streams_expiry_days" {
			expiry = &out.EngineDefaults.Parameters[i]
		}
	}
	require.NotNil(t, expiry, "neptune_streams_expiry_days (a real cluster-level parameter) must be present")
	assert.Equal(t, "1-90", aws.ToString(expiry.AllowedValues))
	assert.Equal(t, "1.2.0.0", aws.ToString(expiry.MinimumEngineVersion))

	for i := range out.EngineDefaults.Parameters {
		assert.NotEqual(t, "neptune_dfe_query_engine", aws.ToString(out.EngineDefaults.Parameters[i].ParameterName),
			"an instance-only parameter must not leak into the cluster-level engine defaults")
	}
}

// Test_SDKRoundTrip_ModifyDBParameterGroup_DisallowedValueRejected proves a
// value outside a parameter's documented AllowedValues is rejected rather
// than silently stored -- InvalidParameterValue has no dedicated typed fault
// in ModifyDBParameterGroup's declared error set (neptune@v1.48.4
// deserializers.go:6239 only declares DBParameterGroupNotFound and
// InvalidDBParameterGroupState), so a real client sees it as a
// *smithy.GenericAPIError, not a typed fault.
func Test_SDKRoundTrip_ModifyDBParameterGroup_DisallowedValueRejected(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	h := neptune.NewHandler(backend)
	client := newTestNeptuneClient(t, h)

	_, err := client.CreateDBParameterGroup(t.Context(), &neptunesdk.CreateDBParameterGroupInput{
		DBParameterGroupName:   aws.String("pg-disallowed-sdk"),
		DBParameterGroupFamily: aws.String("neptune1.3"),
		Description:            aws.String("test"),
	})
	require.NoError(t, err)

	_, err = client.ModifyDBParameterGroup(t.Context(), &neptunesdk.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String("pg-disallowed-sdk"),
		Parameters: []types.Parameter{
			{
				ParameterName:  aws.String("neptune_result_cache"),
				ParameterValue: aws.String("maybe"),
				ApplyMethod:    types.ApplyMethodPendingReboot,
			},
		},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidParameterValue", apiErr.ErrorCode())
}
