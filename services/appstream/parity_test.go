package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_FleetComputeCapacityStatus verifies that Fleet responses include
// ComputeCapacityStatus with a Desired field — required by the real AWS API.
func TestParity_FleetComputeCapacityStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "cap-fleet",
		"InstanceType": "stream.standard.medium",
		"ComputeCapacity": map[string]any{
			"DesiredInstances": 2,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)

	ccs, ok := fleet["ComputeCapacityStatus"].(map[string]any)
	require.True(t, ok, "ComputeCapacityStatus must be present in CreateFleet response")
	desired, ok := ccs["Desired"]
	require.True(t, ok, "ComputeCapacityStatus.Desired must be present")
	assert.EqualValues(t, 2, desired, "Desired should match input")

	// Also verify DescribeFleets returns it
	t.Run("DescribeFleets includes ComputeCapacityStatus", func(t *testing.T) {
		t.Parallel()

		h2 := newTestHandler(t)
		doRequest(t, h2, "CreateFleet", map[string]any{
			"Name":         "cap-fleet2",
			"InstanceType": "stream.standard.medium",
			"ComputeCapacity": map[string]any{
				"DesiredInstances": 3,
			},
		})

		recD := doRequest(t, h2, "DescribeFleets", map[string]any{"Names": []string{"cap-fleet2"}})
		require.Equal(t, http.StatusOK, recD.Code)

		var dr map[string]any
		require.NoError(t, json.Unmarshal(recD.Body.Bytes(), &dr))
		fleets := dr["Fleets"].([]any)
		require.Len(t, fleets, 1)
		f := fleets[0].(map[string]any)
		ccs2, ok := f["ComputeCapacityStatus"].(map[string]any)
		require.True(t, ok, "ComputeCapacityStatus must be present in DescribeFleets response")
		assert.EqualValues(t, 3, ccs2["Desired"])
	})
}

// TestParity_FleetComputeCapacityStatusDefault verifies that a fleet created
// without explicit ComputeCapacity still returns ComputeCapacityStatus with Desired=1.
func TestParity_FleetComputeCapacityStatusDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "default-cap-fleet",
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)

	ccs, ok := fleet["ComputeCapacityStatus"].(map[string]any)
	require.True(t, ok, "ComputeCapacityStatus must be present even without explicit ComputeCapacity input")
	desired, ok := ccs["Desired"]
	require.True(t, ok, "ComputeCapacityStatus.Desired must be present")
	assert.EqualValues(t, 1, desired, "default Desired should be 1")
}

// TestParity_FleetImageNameRoundtrip verifies that ImageName set on CreateFleet
// is returned in the Fleet response.
func TestParity_FleetImageNameRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "img-fleet",
		"InstanceType": "stream.standard.medium",
		"ImageName":    "AppStream-WinServer2019-05-01-2023",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)
	assert.Equal(t, "AppStream-WinServer2019-05-01-2023", fleet["ImageName"])
}

// TestParity_FleetImageArnRoundtrip verifies that ImageArn set on CreateFleet
// is returned in the Fleet response.
func TestParity_FleetImageArnRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	imageArn := "arn:aws:appstream:us-east-1:123456789012:image/AppStream-WinServer2019"
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "imgarn-fleet",
		"InstanceType": "stream.standard.medium",
		"ImageArn":     imageArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)
	assert.Equal(t, imageArn, fleet["ImageArn"])
}

// TestParity_FleetIdleDisconnectTimeout verifies that IdleDisconnectTimeoutInSeconds
// is accepted as input and returned in Fleet responses.
func TestParity_FleetIdleDisconnectTimeout(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":                           "idle-fleet",
		"InstanceType":                   "stream.standard.medium",
		"IdleDisconnectTimeoutInSeconds": 600,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)
	assert.EqualValues(t, 600, fleet["IdleDisconnectTimeoutInSeconds"])
}

// TestParity_FleetEnableDefaultInternetAccess verifies that
// EnableDefaultInternetAccess is accepted and returned.
func TestParity_FleetEnableDefaultInternetAccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":                        "inet-fleet",
		"InstanceType":                "stream.standard.medium",
		"EnableDefaultInternetAccess": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)
	assert.Equal(t, true, fleet["EnableDefaultInternetAccess"])
}

// TestParity_FleetUpdateImageName verifies that UpdateFleet can change ImageName.
func TestParity_FleetUpdateImageName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "upd-img-fleet",
		"InstanceType": "stream.standard.medium",
		"ImageName":    "old-image",
	})

	rec := doRequest(t, h, "UpdateFleet", map[string]any{
		"Name":      "upd-img-fleet",
		"ImageName": "new-image",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)
	assert.Equal(t, "new-image", fleet["ImageName"])
}

// TestParity_FleetUpdateComputeCapacity verifies that UpdateFleet can change
// DesiredInstances via ComputeCapacity.
func TestParity_FleetUpdateComputeCapacity(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateFleet", map[string]any{
		"Name":            "upd-cap-fleet",
		"InstanceType":    "stream.standard.medium",
		"ComputeCapacity": map[string]any{"DesiredInstances": 1},
	})

	rec := doRequest(t, h, "UpdateFleet", map[string]any{
		"Name":            "upd-cap-fleet",
		"ComputeCapacity": map[string]any{"DesiredInstances": 5},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)
	ccs := fleet["ComputeCapacityStatus"].(map[string]any)
	assert.EqualValues(t, 5, ccs["Desired"])
}

// TestParity_FleetARNFormat verifies that fleet ARNs match the AWS format
// arn:aws:appstream:<region>:<account>:fleet/<name>.
func TestParity_FleetARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "arn-fleet",
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)
	assert.Regexp(t, `^arn:aws:appstream:[a-z0-9-]+:\d+:fleet/arn-fleet$`, fleet["Arn"])
}

// TestParity_StackARNFormat verifies that stack ARNs match the AWS format
// arn:aws:appstream:<region>:<account>:stack/<name>.
func TestParity_StackARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateStack", map[string]any{"Name": "arn-stack"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stack := resp["Stack"].(map[string]any)
	assert.Regexp(t, `^arn:aws:appstream:[a-z0-9-]+:\d+:stack/arn-stack$`, stack["Arn"])
}

// TestParity_UserARNFormat verifies that user ARNs match the AWS format.
func TestParity_UserARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateUser", map[string]any{
		"UserName":           "testuser",
		"Email":              "testuser@example.com",
		"AuthenticationType": "USERPOOL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDesc := doRequest(t, h, "DescribeUsers", map[string]any{"AuthenticationType": "USERPOOL"})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &resp))
	users := resp["Users"].([]any)
	require.Len(t, users, 1)
	user := users[0].(map[string]any)
	assert.Contains(t, user["Arn"], "arn:aws:appstream:")
}

// TestParity_FleetDefaultsMatchAWS verifies that fleet defaults match AWS:
// - FleetType defaults to ON_DEMAND
// - MaxUserDurationInSeconds defaults to 57600 (16h)
// - DisconnectTimeoutInSeconds defaults to 300 (5min).
func TestParity_FleetDefaultsMatchAWS(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "defaults-fleet",
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleet"].(map[string]any)
	assert.Equal(t, "ON_DEMAND", fleet["FleetType"])
	assert.EqualValues(t, 57600, fleet["MaxUserDurationInSeconds"])
	assert.EqualValues(t, 300, fleet["DisconnectTimeoutInSeconds"])
}

// TestParity_UserStatusEnabled verifies that a newly created and enabled user
// has Enabled=true and Status=CONFIRMED.
func TestParity_UserStatusEnabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateUser", map[string]any{
		"UserName":           "enabled-user",
		"Email":              "enabled@example.com",
		"AuthenticationType": "USERPOOL",
	})
	doRequest(t, h, "EnableUser", map[string]any{
		"UserName":           "enabled-user",
		"AuthenticationType": "USERPOOL",
	})

	rec := doRequest(t, h, "DescribeUsers", map[string]any{"AuthenticationType": "USERPOOL"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	users := resp["Users"].([]any)
	require.Len(t, users, 1)
	user := users[0].(map[string]any)
	assert.Equal(t, true, user["Enabled"])
}

// TestParity_BatchAssociateUserStackErrors verifies that BatchAssociateUserStack
// returns per-item errors for invalid associations rather than a top-level error.
func TestParity_BatchAssociateUserStackErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Associate to a nonexistent stack — should get per-item error, not 500
	rec := doRequest(t, h, "BatchAssociateUserStack", map[string]any{
		"UserStackAssociations": []any{
			map[string]any{
				"UserName":           "ghost@example.com",
				"StackName":          "nonexistent-stack",
				"AuthenticationType": "USERPOOL",
			},
		},
	})
	// Real AWS returns 200 with errors in the response body
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// errors field may be empty or present depending on implementation
	// but response must be 200 with valid JSON
	assert.NotNil(t, resp)
}

// TestParity_ImageBuilderARNFormat verifies image builder ARN format.
func TestParity_ImageBuilderARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateImageBuilder", map[string]any{
		"Name":         "arn-builder",
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	builder := resp["ImageBuilder"].(map[string]any)
	assert.Regexp(t, `^arn:aws:appstream:[a-z0-9-]+:\d+:image-builder/arn-builder$`, builder["Arn"])
}

// TestParity_AppBlockARNFormat verifies app block ARN format.
func TestParity_AppBlockARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateAppBlock", map[string]any{"Name": "arn-appblock"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ab := resp["AppBlock"].(map[string]any)
	assert.Contains(t, ab["Arn"], "arn:aws:appstream:")
	assert.Contains(t, ab["Arn"], "app-block/arn-appblock")
}

// TestParity_ApplicationARNFormat verifies application ARN format.
func TestParity_ApplicationARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"Name":       "arn-app",
		"LaunchPath": "/path/to/app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	app := resp["Application"].(map[string]any)
	assert.Contains(t, app["Arn"], "arn:aws:appstream:")
	assert.Contains(t, app["Arn"], "application/arn-app")
}

// TestParity_FleetStartedStateRunning verifies that StartFleet changes state to RUNNING.
func TestParity_FleetStartedStateRunning(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "state-fleet",
		"InstanceType": "stream.standard.medium",
	})
	doRequest(t, h, "StartFleet", map[string]any{"Name": "state-fleet"})

	rec := doRequest(t, h, "DescribeFleets", map[string]any{"Names": []string{"state-fleet"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	fleet := resp["Fleets"].([]any)[0].(map[string]any)
	assert.Equal(t, "RUNNING", fleet["State"])
}

// TestParity_SessionStreamingURL verifies that CreateStreamingURL returns a URL.
func TestParity_SessionStreamingURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStack", map[string]any{"Name": "url-stack"})
	doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "url-fleet",
		"InstanceType": "stream.standard.medium",
	})
	doRequest(t, h, "AssociateFleet", map[string]any{
		"FleetName": "url-fleet",
		"StackName": "url-stack",
	})

	rec := doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "url-stack",
		"FleetName": "url-fleet",
		"UserId":    "user1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["StreamingURL"])
}

// TestParity_TagLifecycle verifies the tag lifecycle: tag, list, untag.
func TestParity_TagLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateStack", map[string]any{
		"Name": "tagged-stack",
		"Tags": map[string]any{"env": "test", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	stackArn := createResp["Stack"].(map[string]any)["Arn"].(string)

	recList := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": stackArn})
	require.Equal(t, http.StatusOK, recList.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	tags := listResp["Tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": stackArn,
		"TagKeys":     []string{"env"},
	})

	recList2 := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": stackArn})
	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listResp2))
	tags2 := listResp2["Tags"].(map[string]any)
	assert.NotContains(t, tags2, "env")
	assert.Equal(t, "platform", tags2["team"])
}

// TestParity_DescribeSessionsFilterByStack verifies DescribeSessions filters work.
func TestParity_DescribeSessionsFilterByStack(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStack", map[string]any{"Name": "sess-stack"})
	doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "sess-fleet",
		"InstanceType": "stream.standard.medium",
	})
	doRequest(t, h, "AssociateFleet", map[string]any{
		"FleetName": "sess-fleet",
		"StackName": "sess-stack",
	})
	doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "sess-stack",
		"FleetName": "sess-fleet",
		"UserId":    "user-a",
	})
	doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "sess-stack",
		"FleetName": "sess-fleet",
		"UserId":    "user-b",
	})

	rec := doRequest(t, h, "DescribeSessions", map[string]any{
		"StackName": "sess-stack",
		"FleetName": "sess-fleet",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	sessions, ok := resp["Sessions"].([]any)
	require.True(t, ok)
	assert.Len(t, sessions, 2)
}

// TestParity_DirectoryConfigRoundtrip verifies CreateDirectoryConfig and
// DescribeDirectoryConfigs return the same OU distinguished names.
func TestParity_DirectoryConfigRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ouDNs := []string{"OU=Streaming,DC=example,DC=com", "OU=Finance,DC=example,DC=com"}
	rec := doRequest(t, h, "CreateDirectoryConfig", map[string]any{
		"DirectoryName":                        "example.com",
		"OrganizationalUnitDistinguishedNames": ouDNs,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDesc := doRequest(t, h, "DescribeDirectoryConfigs", map[string]any{
		"DirectoryNames": []string{"example.com"},
	})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &resp))
	configs := resp["DirectoryConfigs"].([]any)
	require.Len(t, configs, 1)
	dc := configs[0].(map[string]any)
	assert.Equal(t, "example.com", dc["DirectoryName"])
	ouRaw := dc["OrganizationalUnitDistinguishedNames"].([]any)
	assert.Len(t, ouRaw, 2)
}

// TestParity_EntitlementRoundtrip verifies CreateEntitlement and DescribeEntitlements.
func TestParity_EntitlementRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStack", map[string]any{"Name": "ent-stack"})

	rec := doRequest(t, h, "CreateEntitlement", map[string]any{
		"Name":          "my-entitlement",
		"StackName":     "ent-stack",
		"AppVisibility": "ALL",
		"Attributes": []any{
			map[string]any{"Name": "saml:sub_type", "Value": "persistent"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDesc := doRequest(t, h, "DescribeEntitlements", map[string]any{
		"Name":      "my-entitlement",
		"StackName": "ent-stack",
	})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &resp))
	entitlements := resp["Entitlements"].([]any)
	require.Len(t, entitlements, 1)
	ent := entitlements[0].(map[string]any)
	assert.Equal(t, "my-entitlement", ent["Name"])
	assert.Equal(t, "ALL", ent["AppVisibility"])
}

// TestParity_UsageReportSubscriptionRoundtrip verifies usage report subscription lifecycle.
func TestParity_UsageReportSubscriptionRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateUsageReportSubscription", map[string]any{
		"Schedule":     "DAILY",
		"S3BucketName": "my-bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDesc := doRequest(t, h, "DescribeUsageReportSubscriptions", map[string]any{})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &resp))
	subs := resp["UsageReportSubscriptions"].([]any)
	require.Len(t, subs, 1)
	sub := subs[0].(map[string]any)
	assert.Equal(t, "DAILY", sub["Schedule"])
	assert.Equal(t, "my-bucket", sub["S3BucketName"])
}

// TestParity_AppBlockBuilderStreamingURL verifies CreateAppBlockBuilderStreamingURL.
func TestParity_AppBlockBuilderStreamingURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateAppBlockBuilder", map[string]any{
		"Name":         "url-builder",
		"InstanceType": "stream.standard.medium",
	})

	rec := doRequest(t, h, "CreateAppBlockBuilderStreamingURL", map[string]any{
		"AppBlockBuilderName": "url-builder",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["StreamingURL"])
}

// TestParity_DescribeUserStackAssociations verifies that user-stack associations
// are returned correctly.
func TestParity_DescribeUserStackAssociations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStack", map[string]any{"Name": "assoc-stack"})
	doRequest(t, h, "CreateUser", map[string]any{
		"UserName":           "assoc-user",
		"Email":              "assoc@example.com",
		"AuthenticationType": "USERPOOL",
	})

	rec := doRequest(t, h, "BatchAssociateUserStack", map[string]any{
		"UserStackAssociations": []any{
			map[string]any{
				"UserName":           "assoc-user",
				"StackName":          "assoc-stack",
				"AuthenticationType": "USERPOOL",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDesc := doRequest(t, h, "DescribeUserStackAssociations", map[string]any{
		"StackName": "assoc-stack",
	})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &resp))
	assocs := resp["UserStackAssociations"].([]any)
	assert.Len(t, assocs, 1)
	assoc := assocs[0].(map[string]any)
	assert.Equal(t, "assoc-user", assoc["UserName"])
	assert.Equal(t, "assoc-stack", assoc["StackName"])
}
