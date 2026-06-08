package ec2_test

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Gap 1: UserData ----

func TestAccuracy_UserData_RunInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	userData := "IyEvYmluL2Jhc2gKZWNobyBoZWxsbw==" // base64 "#!/bin/bash\necho hello"

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-12345678"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"1"},
		"UserData":     {userData},
	}

	resp, err := dispatchHandler(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "RunInstancesResponse")

	// Extract instance ID from response to call DescribeInstanceAttribute.
	id := accuracyExtractXMLValue(resp, "instanceId")
	require.NotEmpty(t, id)

	attrVals := url.Values{
		"Action":     {"DescribeInstanceAttribute"},
		"Version":    {"2016-11-15"},
		"InstanceId": {id},
		"Attribute":  {"userData"},
	}

	attrResp, attrErr := dispatchHandler(h, attrVals)
	require.NoError(t, attrErr)
	assert.Contains(t, attrResp, userData)
}

// ---- Gap 2: Public IP ----

func TestAccuracy_PublicIP_SubnetMapPublicIpOnLaunch(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vpc, err := b.CreateVpc("10.1.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.1.1.0/24", "us-east-1a")
	require.NoError(t, err)

	// Before enabling MapPublicIpOnLaunch, instance should have no public IP.
	instances, err := b.RunInstances("ami-123", "t3.micro", subnet.ID, 1)
	require.NoError(t, err)
	assert.Empty(
		t,
		instances[0].PublicIPAddress,
		"no public IP before enabling MapPublicIpOnLaunch",
	)

	// Enable MapPublicIpOnLaunch.
	err = b.ModifySubnetAttribute(subnet.ID, "mapPublicIpOnLaunch", true)
	require.NoError(t, err)

	// Now instance should get a public IP.
	instances2, err := b.RunInstances("ami-123", "t3.micro", subnet.ID, 1)
	require.NoError(t, err)
	assert.NotEmpty(
		t,
		instances2[0].PublicIPAddress,
		"public IP should be assigned when MapPublicIpOnLaunch=true",
	)
	assert.NotEmpty(
		t,
		instances2[0].PublicDNSName,
		"public DNS should be assigned when MapPublicIpOnLaunch=true",
	)
}

func TestAccuracy_ModifySubnetAttribute_PersistsValue(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vpc, err := b.CreateVpc("10.2.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.2.1.0/24", "us-east-1a")
	require.NoError(t, err)
	assert.False(t, subnet.MapPublicIPOnLaunch)

	err = b.ModifySubnetAttribute(subnet.ID, "mapPublicIpOnLaunch", true)
	require.NoError(t, err)

	subnets := b.DescribeSubnets([]string{subnet.ID})
	require.Len(t, subnets, 1)
	assert.True(t, subnets[0].MapPublicIPOnLaunch)
}

// ---- Gap 3: Pagination ----

func TestAccuracy_DescribeInstances_Pagination(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	// Create 10 instances.
	for range 10 {
		_, err := b.RunInstances("ami-123", "t3.micro", "", 1)
		require.NoError(t, err)
	}

	// First page: max 5 results (minimum allowed by AWS).
	vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"5"},
	}

	resp1, err := dispatchHandler(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp1, "DescribeInstancesResponse")

	token := accuracyExtractXMLValue(resp1, "nextToken")
	assert.NotEmpty(t, token, "first page should return a next token")

	// Second page.
	vals2 := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"5"},
		"NextToken":  {token},
	}

	resp2, err := dispatchHandler(h, vals2)
	require.NoError(t, err)
	assert.Contains(t, resp2, "DescribeInstancesResponse")
}

// ---- Gap 5: Enhanced networking ----

func TestAccuracy_EnaSupport_DefaultTrue(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)
	assert.True(t, instances[0].EnaSupport, "EnaSupport should default to true for new instances")
}

func TestAccuracy_DescribeInstanceAttribute_EnaSupport(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	instances, err := h.Backend.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":     {"DescribeInstanceAttribute"},
		"Version":    {"2016-11-15"},
		"InstanceId": {instances[0].ID},
		"Attribute":  {"enaSupport"},
	}

	resp, err := dispatchHandler(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "DescribeInstanceAttributeResponse")
	assert.Contains(t, resp, "true")
}

// ---- Gap 6: SG rule references ----

func TestAccuracy_SecurityGroupRule_SGReference(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	sg1, err := b.CreateSecurityGroup("sg-source", "source sg", "vpc-default")
	require.NoError(t, err)

	sg2, err := b.CreateSecurityGroup("sg-target", "target sg", "vpc-default")
	require.NoError(t, err)

	rule := ec2.SecurityGroupRule{
		Protocol:      "tcp",
		FromPort:      80,
		ToPort:        80,
		SourceGroupID: sg1.ID,
	}

	err = b.AuthorizeSecurityGroupIngress(sg2.ID, []ec2.SecurityGroupRule{rule})
	require.NoError(t, err)

	sgs := b.DescribeSecurityGroups([]string{sg2.ID})
	require.Len(t, sgs, 1)
	require.Len(t, sgs[0].IngressRules, 1)
	assert.Equal(t, sg1.ID, sgs[0].IngressRules[0].SourceGroupID)
}

// ---- Gap 7: DryRun ----

func TestAccuracy_DryRun_Returns412(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-12345678"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"1"},
		"DryRun":       {"true"},
	}

	// dispatchHandler calls the backend directly; test via the error sentinel.
	_, err := dispatchHandler(h, vals)
	require.Error(t, err)
	assert.ErrorIs(t, err, ec2.ErrDryRunOperation)
}

// ---- Gap 13: EBS encryption ----

func TestAccuracy_CreateVolume_Encrypted(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp3", 100)
	require.NoError(t, err)

	err = b.SetVolumeEncryption(vol.ID, true, "arn:aws:kms:us-east-1:123456789012:key/my-key")
	require.NoError(t, err)

	vols := b.DescribeVolumes([]string{vol.ID})
	require.Len(t, vols, 1)
	assert.True(t, vols[0].Encrypted)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/my-key", vols[0].KmsKeyID)
}

func TestAccuracy_CreateSnapshot_InheritsEncryption(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp3", 100)
	require.NoError(t, err)

	err = b.SetVolumeEncryption(vol.ID, true, "alias/aws/ebs")
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(vol.ID, "test snapshot")
	require.NoError(t, err)
	assert.True(t, snap.Encrypted, "snapshot should inherit volume encryption")
	assert.Equal(t, "alias/aws/ebs", snap.KmsKeyID)
}

func TestAccuracy_SetVolumeEncryption_DefaultKMSKey(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, err)

	// Encrypted=true with no explicit key → default AWS-managed key.
	err = b.SetVolumeEncryption(vol.ID, true, "")
	require.NoError(t, err)

	vols := b.DescribeVolumes([]string{vol.ID})
	require.Len(t, vols, 1)
	assert.True(t, vols[0].Encrypted)
	assert.Equal(t, "alias/aws/ebs", vols[0].KmsKeyID)
}

// ---- Gap 14: ModifyInstanceAttribute persistence ----

func TestAccuracy_SetInstanceAttribute_UserData(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	id := instances[0].ID
	// userData can be set on running instances (via RunInstances initial setup path).
	err = b.SetInstanceAttribute(id, "userData", "dXNlci1kYXRh")
	require.NoError(t, err)

	insts := b.DescribeInstances([]string{id}, "")
	require.Len(t, insts, 1)
	assert.Equal(t, "dXNlci1kYXRh", insts[0].UserData)
}

func TestAccuracy_SetInstanceAttribute_InstanceType_RequiresStopped(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending → running

	id := instances[0].ID

	// Should fail on running instance.
	err = b.SetInstanceAttribute(id, "instanceType", "t3.large")
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrInvalidInstanceState)

	// Stop then modify — should succeed.
	_, stopErr := b.StopInstances([]string{id})
	require.NoError(t, stopErr)
	b.TickLifecycleForTest() // stopping → stopped

	err = b.SetInstanceAttribute(id, "instanceType", "t3.large")
	require.NoError(t, err)

	insts := b.DescribeInstances([]string{id}, "")
	require.Len(t, insts, 1)
	assert.Equal(t, "t3.large", insts[0].InstanceType)
}

func TestAccuracy_SetInstanceAttribute_NotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	err := b.SetInstanceAttribute("i-nonexistent", "userData", "abc")
	require.Error(t, err)
	assert.ErrorIs(t, err, ec2.ErrInstanceNotFound)
}

// ---- Gap 15: Spot price history ----

func TestAccuracy_SpotPriceHistory_Deterministic(t *testing.T) {
	t.Parallel()

	// use deterministic time since history drops records older than 24h
	now := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC).UTC().Add(-24 * time.Hour)

	records1 := ec2.GenerateSpotPriceHistory(
		[]string{"t3.micro"},
		[]string{"us-east-1a"},
		[]string{"Linux/UNIX"},
		now,
		"us-east-1",
	)

	records2 := ec2.GenerateSpotPriceHistory(
		[]string{"t3.micro"},
		[]string{"us-east-1a"},
		[]string{"Linux/UNIX"},
		now,
		"us-east-1",
	)

	require.NotEmpty(t, records1)
	require.Len(t, records1, len(records2))
	assert.Equal(t, records1[0].SpotPrice, records2[0].SpotPrice, "price must be deterministic")
}

func TestAccuracy_SpotPriceHistory_DefaultsPopulated(t *testing.T) {
	t.Parallel()

	records := ec2.GenerateSpotPriceHistory(nil, nil, nil, time.Time{}, "us-east-1")
	assert.NotEmpty(t, records, "default instance types should produce records")
}

// ---- Optimization: CIDR overlap ----

func TestAccuracy_CreateVpc_CIDRConflict(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateVpc("192.168.0.0/16")
	require.NoError(t, err)

	// Exact same CIDR should conflict.
	_, err = b.CreateVpc("192.168.0.0/16")
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCIDRConflict)

	// Overlapping CIDR should also conflict.
	_, err = b.CreateVpc("192.168.1.0/24")
	require.Error(t, err)
	assert.ErrorIs(t, err, ec2.ErrCIDRConflict)
}

func TestAccuracy_CreateSubnet_CIDRConflict(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vpc, err := b.CreateVpc("10.3.0.0/16")
	require.NoError(t, err)

	_, err = b.CreateSubnet(vpc.ID, "10.3.1.0/24", "us-east-1a")
	require.NoError(t, err)

	// Same CIDR in same VPC should conflict.
	_, err = b.CreateSubnet(vpc.ID, "10.3.1.0/24", "us-east-1b")
	require.Error(t, err)
	assert.ErrorIs(t, err, ec2.ErrCIDRConflict)
}

func TestAccuracy_CreateSubnet_NotInVPC(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vpc, err := b.CreateVpc("10.4.0.0/16")
	require.NoError(t, err)

	// Subnet outside VPC CIDR should fail.
	_, err = b.CreateSubnet(vpc.ID, "192.168.1.0/24", "us-east-1a")
	require.Error(t, err)
	assert.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

// ---- Optimization: spotFleetHistory cap ----

func TestAccuracy_SpotFleetHistory_Capped(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	config := ec2.SpotFleetRequestConfig{
		SpotPrice:      "0.05",
		TargetCapacity: 1,
		LaunchSpecifications: []ec2.SpotFleetLaunchSpecification{
			{ImageID: "ami-123", InstanceType: "t3.micro"},
		},
	}

	fleet, err := b.RequestSpotFleet(config)
	require.NoError(t, err)

	// Trigger many history records via ModifySpotFleetRequest.
	for i := range 600 {
		newCap := 1 + (i % 3)
		_, modErr := b.ModifySpotFleetRequest(fleet.SpotFleetRequestID, newCap, "")
		require.NoError(t, modErr)
	}

	// History should be capped.
	history, err := b.DescribeSpotFleetRequestHistory(fleet.SpotFleetRequestID, time.Time{})
	require.NoError(t, err)
	assert.LessOrEqual(
		t,
		len(history),
		500,
		"history should be capped at maxSpotFleetHistoryEntries",
	)
}

// ---- helpers ----

// newTestHandler creates a fresh Handler with an InMemoryBackend.
func newTestHandler() *ec2.Handler {
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	return h
}

// dispatchHandler calls ec2.ExportDispatch (exported for tests) via vals.
func dispatchHandler(h *ec2.Handler, vals url.Values) (string, error) {
	return ec2.ExportDispatch(h, vals)
}

// accuracyExtractXMLValue pulls the first text node matching the given XML element name.
func accuracyExtractXMLValue(xmlStr, tagName string) string {
	open := "<" + tagName + ">"
	closeTag := "</" + tagName + ">"
	start := accuracyIndexOf(xmlStr, open)

	if start < 0 {
		return ""
	}

	start += len(open)
	end := accuracyIndexOf(xmlStr[start:], closeTag)

	if end < 0 {
		return ""
	}

	return xmlStr[start : start+end]
}

func accuracyIndexOf(s, substr string) int {
	for i := range len(s) - len(substr) + 1 {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}

	return -1
}
