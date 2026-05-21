package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Gap A: DescribeInstances named filter support ----

func TestAccuracy_DescribeInstances_FilterByStateName(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 2)
	require.NoError(t, err)

	// Stop one instance.
	_, err = b.StopInstances([]string{insts[0].ID})
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"instance-state-name"},
		"Filter.1.Value.1": {"running"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts[1].ID, "running instance should appear")
	assert.NotContains(t, resp, insts[0].ID, "stopped instance should not appear")
}

func TestAccuracy_DescribeInstances_FilterByImageID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts1, err := b.RunInstances("ami-aaaa", "t3.micro", "", 1)
	require.NoError(t, err)

	_, err = b.RunInstances("ami-bbbb", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"image-id"},
		"Filter.1.Value.1": {"ami-aaaa"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts1[0].ID)
	assert.NotContains(t, resp, "ami-bbbb")
}

func TestAccuracy_DescribeInstances_FilterByInstanceType(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts1, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	insts2, err := b.RunInstances("ami-123", "m5.xlarge", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"instance-type"},
		"Filter.1.Value.1": {"t3.micro"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts1[0].ID)
	assert.NotContains(t, resp, insts2[0].ID)
}

func TestAccuracy_DescribeInstances_FilterByVpcID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc1, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sub1, err := b.CreateSubnet(vpc1.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)

	vpc2, err := b.CreateVpc("10.1.0.0/16")
	require.NoError(t, err)

	sub2, err := b.CreateSubnet(vpc2.ID, "10.1.1.0/24", "us-east-1a")
	require.NoError(t, err)

	insts1, err := b.RunInstances("ami-123", "t3.micro", sub1.ID, 1)
	require.NoError(t, err)

	insts2, err := b.RunInstances("ami-123", "t3.micro", sub2.ID, 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"vpc-id"},
		"Filter.1.Value.1": {vpc1.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts1[0].ID)
	assert.NotContains(t, resp, insts2[0].ID)
}

func TestAccuracy_DescribeInstances_FilterBySubnetID(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sub1, err := b.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)

	sub2, err := b.CreateSubnet(vpc.ID, "10.0.2.0/24", "us-east-1b")
	require.NoError(t, err)

	insts1, err := b.RunInstances("ami-123", "t3.micro", sub1.ID, 1)
	require.NoError(t, err)

	insts2, err := b.RunInstances("ami-123", "t3.micro", sub2.ID, 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"subnet-id"},
		"Filter.1.Value.1": {sub1.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts1[0].ID)
	assert.NotContains(t, resp, insts2[0].ID)
}

func TestAccuracy_DescribeInstances_FilterByPrivateIP(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 2)
	require.NoError(t, err)

	ip1 := insts[0].PrivateIP

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"private-ip-address"},
		"Filter.1.Value.1": {ip1},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts[0].ID)
	assert.NotContains(t, resp, insts[1].ID)
}

func TestAccuracy_DescribeInstances_FilterByKeyName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals1 := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"1"},
		"KeyName":      {"my-key"},
	}
	resp1, err := ec2.ExportDispatch(h, vals1)
	require.NoError(t, err)
	id1 := accuracyExtractXMLValue(resp1, "instanceId")

	vals2 := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"1"},
	}
	resp2, err := ec2.ExportDispatch(h, vals2)
	require.NoError(t, err)
	id2 := accuracyExtractXMLValue(resp2, "instanceId")

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"key-name"},
		"Filter.1.Value.1": {"my-key"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, id1)
	assert.NotContains(t, resp, id2)
}

func TestAccuracy_DescribeInstances_FilterByTag(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals1 := url.Values{
		"Action":                          {"RunInstances"},
		"Version":                         {"2016-11-15"},
		"ImageId":                         {"ami-123"},
		"InstanceType":                    {"t3.micro"},
		"MinCount":                        {"1"},
		"MaxCount":                        {"1"},
		"TagSpecification.1.ResourceType": {"instance"},
		"TagSpecification.1.Tag.1.Key":    {"Env"},
		"TagSpecification.1.Tag.1.Value":  {"prod"},
	}
	resp1, err := ec2.ExportDispatch(h, vals1)
	require.NoError(t, err)
	id1 := accuracyExtractXMLValue(resp1, "instanceId")
	require.NotEmpty(t, id1)

	vals2 := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"1"},
	}
	resp2, err := ec2.ExportDispatch(h, vals2)
	require.NoError(t, err)
	id2 := accuracyExtractXMLValue(resp2, "instanceId")

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"tag:Env"},
		"Filter.1.Value.1": {"prod"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, id1)
	assert.NotContains(t, resp, id2)
}

func TestAccuracy_DescribeInstances_MultipleFiltersAND(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts1, err := b.RunInstances("ami-aaaa", "t3.micro", "", 1)
	require.NoError(t, err)

	insts2, err := b.RunInstances("ami-aaaa", "m5.xlarge", "", 1)
	require.NoError(t, err)

	// Filter by both image-id AND instance-type: should only match insts1.
	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"image-id"},
		"Filter.1.Value.1": {"ami-aaaa"},
		"Filter.2.Name":    {"instance-type"},
		"Filter.2.Value.1": {"t3.micro"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts1[0].ID)
	assert.NotContains(t, resp, insts2[0].ID)
}

func TestAccuracy_DescribeInstances_FilterMultipleValuesOR(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts1, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	_, err = b.StopInstances([]string{insts1[0].ID})
	require.NoError(t, err)

	insts2, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	// Filter for both running AND stopped — should return both.
	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"instance-state-name"},
		"Filter.1.Value.1": {"running"},
		"Filter.1.Value.2": {"stopped"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts1[0].ID)
	assert.Contains(t, resp, insts2[0].ID)
}

// ---- Gap B: DescribeInstances MaxResults bounds ----

func TestAccuracy_DescribeInstances_MaxResults_TooSmall(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"4"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxResults")
}

func TestAccuracy_DescribeInstances_MaxResults_TooLarge(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"1001"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxResults")
}

func TestAccuracy_DescribeInstances_MaxResults_AtMinimum(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"5"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "DescribeInstancesResponse")
}

func TestAccuracy_DescribeInstances_MaxResults_AtMaximum(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"1000"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "DescribeInstancesResponse")
}

func TestAccuracy_DescribeInstances_MaxResults_Zero_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"0"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
}

func TestAccuracy_DescribeInstances_MaxResults_Pagination(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	for range 15 {
		_, err := b.RunInstances("ami-123", "t3.micro", "", 1)
		require.NoError(t, err)
	}

	vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"5"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	token := accuracyExtractXMLValue(resp, "nextToken")
	assert.NotEmpty(t, token)

	page2Vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"5"},
		"NextToken":  {token},
	}

	resp2, err := ec2.ExportDispatch(h, page2Vals)
	require.NoError(t, err)
	token2 := accuracyExtractXMLValue(resp2, "nextToken")
	assert.NotEmpty(t, token2)

	page3Vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"5"},
		"NextToken":  {token2},
	}

	resp3, err := ec2.ExportDispatch(h, page3Vals)
	require.NoError(t, err)
	token3 := accuracyExtractXMLValue(resp3, "nextToken")
	assert.Empty(t, token3, "last page should have no nextToken")
}

// ---- Gap C: RunInstances SecurityGroupId.N ----

func TestAccuracy_RunInstances_SecurityGroupIds_Stored(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("my-sg", "My SG", vpc.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {sg.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, sg.ID, "security group ID should appear in response")
}

func TestAccuracy_RunInstances_SecurityGroupIds_InDescribeInstances(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("web-sg", "Web SG", vpc.ID)
	require.NoError(t, err)

	runVals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {sg.ID},
	}

	runResp, err := ec2.ExportDispatch(h, runVals)
	require.NoError(t, err)
	instanceID := accuracyExtractXMLValue(runResp, "instanceId")
	require.NotEmpty(t, instanceID)

	descVals := url.Values{
		"Action":       {"DescribeInstances"},
		"Version":      {"2016-11-15"},
		"InstanceId.1": {instanceID},
	}

	descResp, err := ec2.ExportDispatch(h, descVals)
	require.NoError(t, err)
	assert.Contains(t, descResp, sg.ID, "security group ID should be in DescribeInstances groupSet")
}

func TestAccuracy_RunInstances_SecurityGroupIds_MultipleGroups(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg1, err := b.CreateSecurityGroup("web", "Web", vpc.ID)
	require.NoError(t, err)

	sg2, err := b.CreateSecurityGroup("db", "DB", vpc.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {sg1.ID},
		"SecurityGroupId.2": {sg2.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, sg1.ID)
	assert.Contains(t, resp, sg2.ID)
}

func TestAccuracy_RunInstances_InvalidSecurityGroupId_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {"sg-doesnotexist"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NotFound")
}

// ---- Gap D: RunInstances MinCount/MaxCount validation ----

func TestAccuracy_RunInstances_MaxCount_LessThanMinCount_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"3"},
		"MaxCount":     {"1"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxCount")
}

func TestAccuracy_RunInstances_MaxCount_Zero_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"0"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
}

func TestAccuracy_RunInstances_MinCount_Zero_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"0"},
		"MaxCount":     {"1"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
}

func TestAccuracy_RunInstances_EqualMinMaxCount_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"2"},
		"MaxCount":     {"2"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "RunInstancesResponse")
}

func TestAccuracy_RunInstances_MaxCountGreaterThanMinCount_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"5"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "RunInstancesResponse")
}

// ---- Gap E: AssociateAddress NetworkInterfaceId support ----

func TestAccuracy_AssociateAddress_NetworkInterfaceId_Accepted(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	// Create an instance so its ENI exists.
	insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":             {"AssociateAddress"},
		"Version":            {"2016-11-15"},
		"AllocationId":       {addr.AllocationID},
		"NetworkInterfaceId": {insts[0].ID}, // use instance ID as target (backend accepts it)
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "AssociateAddressResponse")
}

func TestAccuracy_AssociateAddress_NoTarget_Rejected(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	vals := url.Values{
		"Action":       {"AssociateAddress"},
		"Version":      {"2016-11-15"},
		"AllocationId": {addr.AllocationID},
		// Neither InstanceId nor NetworkInterfaceId provided.
	}

	_, err = ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required")
}

func TestAccuracy_AssociateAddress_NoAllocationId_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":     {"AssociateAddress"},
		"Version":    {"2016-11-15"},
		"InstanceId": {"i-12345678"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AllocationId")
}

func TestAccuracy_AssociateAddress_InstanceId_StillWorks(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":       {"AssociateAddress"},
		"Version":      {"2016-11-15"},
		"AllocationId": {addr.AllocationID},
		"InstanceId":   {insts[0].ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "AssociateAddressResponse")
}

// ---- Gap F: instanceItem groupSet in DescribeInstances response ----

func TestAccuracy_DescribeInstances_GroupSetInResponse(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("app-sg", "App SG", vpc.ID)
	require.NoError(t, err)

	runVals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {sg.ID},
	}

	runResp, err := ec2.ExportDispatch(h, runVals)
	require.NoError(t, err)
	instanceID := accuracyExtractXMLValue(runResp, "instanceId")
	require.NotEmpty(t, instanceID)

	// DescribeInstances should include groupSet.
	descVals := url.Values{
		"Action":       {"DescribeInstances"},
		"Version":      {"2016-11-15"},
		"InstanceId.1": {instanceID},
	}

	descResp, err := ec2.ExportDispatch(h, descVals)
	require.NoError(t, err)
	assert.Contains(t, descResp, "<groupSet>")
	assert.Contains(t, descResp, sg.ID)
}

func TestAccuracy_RunInstances_NoSecurityGroups_GroupSetEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"1"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	// No SGs specified — groupSet should have no groupId items.
	assert.NotContains(t, resp, "<groupId>")
}

// ---- Gap G: DescribeInstances filter combined with instance IDs ----

func TestAccuracy_DescribeInstances_FilterWithInstanceIds(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 2)
	require.NoError(t, err)

	// Stop first instance.
	_, err = b.StopInstances([]string{insts[0].ID})
	require.NoError(t, err)

	// Request both IDs but filter to running only — should return only the running one.
	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"InstanceId.1":     {insts[0].ID},
		"InstanceId.2":     {insts[1].ID},
		"Filter.1.Name":    {"instance-state-name"},
		"Filter.1.Value.1": {"running"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts[1].ID)
	assert.NotContains(t, resp, insts[0].ID)
}

// ---- Gap H: RunInstances defaults ----

func TestAccuracy_RunInstances_NoMaxCount_DefaultsToMinCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"2"},
		// MaxCount omitted — should default to MinCount.
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "RunInstancesResponse")
}

func TestAccuracy_RunInstances_NoMinCount_DefaultsToOne(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		// MinCount omitted — should default to 1.
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "RunInstancesResponse")
}

// ---- Gap I: DescribeSecurityGroups filter support ----

func TestAccuracy_DescribeSecurityGroups_FilterByVpcId(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc1, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	vpc2, err := b.CreateVpc("10.1.0.0/16")
	require.NoError(t, err)

	sg1, err := b.CreateSecurityGroup("sg-vpc1", "SG in vpc1", vpc1.ID)
	require.NoError(t, err)

	sg2, err := b.CreateSecurityGroup("sg-vpc2", "SG in vpc2", vpc2.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeSecurityGroups"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"vpc-id"},
		"Filter.1.Value.1": {vpc1.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, sg1.ID)
	assert.NotContains(t, resp, sg2.ID)
}

func TestAccuracy_DescribeSecurityGroups_FilterByGroupName(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg1, err := b.CreateSecurityGroup("web-sg", "Web SG", vpc.ID)
	require.NoError(t, err)

	sg2, err := b.CreateSecurityGroup("db-sg", "DB SG", vpc.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeSecurityGroups"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"group-name"},
		"Filter.1.Value.1": {"web-sg"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, sg1.ID)
	assert.NotContains(t, resp, sg2.ID)
}

// ---- Gap J: DescribeInstances returns empty result for no-match filter ----

func TestAccuracy_DescribeInstances_NoMatchFilter_EmptyResult(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	_, err := b.RunInstances("ami-123", "t3.micro", "", 3)
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"image-id"},
		"Filter.1.Value.1": {"ami-nonexistent"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "DescribeInstancesResponse")
	assert.NotContains(t, resp, "<instanceId>")
}

// ---- Helper: check for default SGs in default VPC ----

func TestAccuracy_RunInstances_WithDefaultVPC_HasDefaultSG(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	sgs := b.DescribeSecurityGroups(nil)

	// Default VPC comes with a default security group.
	found := false

	for _, sg := range sgs {
		if sg.Name == "default" {
			found = true

			break
		}
	}

	assert.True(t, found, "default VPC should have a default security group")
}

// ---- Gap K: DescribeInstances with no filters returns all instances ----

func TestAccuracy_DescribeInstances_NoFilters_ReturnsAll(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 3)
	require.NoError(t, err)

	vals := url.Values{
		"Action":  {"DescribeInstances"},
		"Version": {"2016-11-15"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)

	for _, inst := range insts {
		assert.Contains(t, resp, inst.ID)
	}
}

// ---- Gap L: DescribeInstances filter ip-address (public IP) ----

func TestAccuracy_DescribeInstances_FilterByPublicIP(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sub, err := b.CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)

	// Enable MapPublicIpOnLaunch.
	err = b.ModifySubnetAttribute(sub.ID, "mapPublicIpOnLaunch", true)
	require.NoError(t, err)

	insts, err := b.RunInstances("ami-123", "t3.micro", sub.ID, 1)
	require.NoError(t, err)
	require.NotEmpty(t, insts[0].PublicIPAddress)

	// Run another instance without public IP.
	vpc2, err := b.CreateVpc("10.1.0.0/16")
	require.NoError(t, err)

	sub2, err := b.CreateSubnet(vpc2.ID, "10.1.1.0/24", "us-east-1b")
	require.NoError(t, err)

	insts2, err := b.RunInstances("ami-123", "t3.micro", sub2.ID, 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"ip-address"},
		"Filter.1.Value.1": {insts[0].PublicIPAddress},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts[0].ID)
	assert.NotContains(t, resp, insts2[0].ID)
}

// ---- Regression: existing Filter.1.Value.1 state filter still works ----

func TestAccuracy_DescribeInstances_LegacyStateFilter_StillWorks(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 2)
	require.NoError(t, err)

	_, err = b.StopInstances([]string{insts[0].ID})
	require.NoError(t, err)

	// Use Filter.1.Name/Value style (preferred).
	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"instance-state-name"},
		"Filter.1.Value.1": {"stopped"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts[0].ID)
	assert.NotContains(t, resp, insts[1].ID)
}

// ---- Edge cases ----

func TestAccuracy_RunInstances_NegativeMinCount_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"-1"},
		"MaxCount":     {"1"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
}

func TestAccuracy_RunInstances_NegativeMaxCount_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-123"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"-5"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
}

func TestAccuracy_DescribeInstances_MaxResults_InvalidString_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":     {"DescribeInstances"},
		"Version":    {"2016-11-15"},
		"MaxResults": {"notanumber"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
}

func TestAccuracy_AssociateAddress_BothInstanceAndNetworkInterface_UsesInstance(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":             {"AssociateAddress"},
		"Version":            {"2016-11-15"},
		"AllocationId":       {addr.AllocationID},
		"InstanceId":         {insts[0].ID},
		"NetworkInterfaceId": {"eni-12345678"},
	}

	// When both are given, InstanceId takes precedence (InstanceId is checked first).
	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "AssociateAddressResponse")
}

// ---- runTestHandlerWithBackend helper ----

func newTestHandlerWithBackend(b *ec2.InMemoryBackend) *ec2.Handler {
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	return h
}

// ---- Gap M: DescribeInstances grouped filters preserve state filter semantics ----

func TestAccuracy_DescribeInstances_StateFilterNamedStyle_PreservesTerminated(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 2)
	require.NoError(t, err)

	_, err = b.TerminateInstances([]string{insts[0].ID})
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"instance-state-name"},
		"Filter.1.Value.1": {"terminated"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts[0].ID)
	assert.NotContains(t, resp, insts[1].ID)
}

// ---- Gap N: RunInstances groupSet populated in RunInstancesResponse ----

func TestAccuracy_RunInstances_GroupSetPresentInResponse(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("test-sg", "Test SG", vpc.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":            {"RunInstances"},
		"Version":           {"2016-11-15"},
		"ImageId":           {"ami-123"},
		"InstanceType":      {"t3.micro"},
		"MinCount":          {"1"},
		"MaxCount":          {"1"},
		"SecurityGroupId.1": {sg.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "<groupSet>")
	assert.Contains(t, resp, "<groupId>"+sg.ID+"</groupId>")
}

// ---- parseEC2Filters helper verification ----

func TestAccuracy_ParseFilters_Empty(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	_, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":  {"DescribeInstances"},
		"Version": {"2016-11-15"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "DescribeInstancesResponse")
}

func TestAccuracy_DescribeInstances_FilterTerminated_Visible(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	_, err = b.TerminateInstances([]string{insts[0].ID})
	require.NoError(t, err)

	// Terminated instances remain visible for ~1hr before janitor sweeps them.
	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"instance-state-name"},
		"Filter.1.Value.1": {"terminated"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, insts[0].ID)
}

// ---- DescribeSecurityGroups existing behaviour regression ----

func TestAccuracy_DescribeSecurityGroups_ByID_NoFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("test-sg", "Test", vpc.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":    {"DescribeSecurityGroups"},
		"Version":   {"2016-11-15"},
		"GroupId.1": {sg.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, sg.ID)
}

// ---- Verify filter names work case-sensitively ----

func TestAccuracy_DescribeInstances_UnknownFilter_PassThrough(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	_, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	// Unknown filter names pass through (lenient mock behaviour).
	vals := url.Values{
		"Action":           {"DescribeInstances"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"some-unknown-filter"},
		"Filter.1.Value.1": {"some-value"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "DescribeInstancesResponse")
}

// ---- DescribeSecurityGroups filter by group-id ----

func TestAccuracy_DescribeSecurityGroups_FilterByGroupId(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg1, err := b.CreateSecurityGroup("sg1", "SG1", vpc.ID)
	require.NoError(t, err)

	sg2, err := b.CreateSecurityGroup("sg2", "SG2", vpc.ID)
	require.NoError(t, err)

	vals := url.Values{
		"Action":           {"DescribeSecurityGroups"},
		"Version":          {"2016-11-15"},
		"Filter.1.Name":    {"group-id"},
		"Filter.1.Value.1": {sg1.ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, sg1.ID)
	assert.NotContains(t, resp, sg2.ID)
}

// ---- DescribeInstances response XML structure ----

func TestAccuracy_DescribeInstances_ResponseContainsXMLNamespace(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":  {"DescribeInstances"},
		"Version": {"2016-11-15"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(resp, "<DescribeInstancesResponse"))
}

func TestAccuracy_DescribeInstances_ReservationSetPresent(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	_, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":  {"DescribeInstances"},
		"Version": {"2016-11-15"},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "<reservationSet>")
	assert.Contains(t, resp, "<instancesSet>")
}
