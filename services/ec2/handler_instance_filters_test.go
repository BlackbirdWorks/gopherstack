package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestDescribeInstances_FilterByStateName(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 2)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending → running

	// Stop one instance.
	_, err = b.StopInstances([]string{insts[0].ID})
	require.NoError(t, err)
	b.TickLifecycleForTest() // stopping → stopped

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

func TestDescribeInstances_FilterByImageID(t *testing.T) {
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

func TestDescribeInstances_FilterByInstanceType(t *testing.T) {
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

func TestDescribeInstances_FilterByVpcID(t *testing.T) {
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

func TestDescribeInstances_FilterBySubnetID(t *testing.T) {
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

func TestDescribeInstances_FilterByPrivateIP(t *testing.T) {
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

func TestDescribeInstances_FilterByKeyName(t *testing.T) {
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

func TestDescribeInstances_FilterByTag(t *testing.T) {
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

func TestDescribeInstances_MultipleFiltersAND(t *testing.T) {
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

func TestDescribeInstances_FilterMultipleValuesOR(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts1, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending → running

	_, err = b.StopInstances([]string{insts1[0].ID})
	require.NoError(t, err)
	b.TickLifecycleForTest() // stopping → stopped

	insts2, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending → running

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

func TestDescribeInstances_MaxResults_TooSmall(t *testing.T) {
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

func TestDescribeInstances_MaxResults_TooLarge(t *testing.T) {
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

func TestDescribeInstances_MaxResults_AtMinimum(t *testing.T) {
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

func TestDescribeInstances_MaxResults_AtMaximum(t *testing.T) {
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

func TestDescribeInstances_MaxResults_Zero_Rejected(t *testing.T) {
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

func TestDescribeInstances_MaxResults_Pagination(t *testing.T) {
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

func TestDescribeInstances_GroupSetInResponse(t *testing.T) {
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

func TestDescribeInstances_FilterWithInstanceIds(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 2)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending → running

	// Stop first instance.
	_, err = b.StopInstances([]string{insts[0].ID})
	require.NoError(t, err)
	b.TickLifecycleForTest() // stopping → stopped

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

func TestDescribeInstances_NoMatchFilter_EmptyResult(t *testing.T) {
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

func TestDescribeInstances_NoFilters_ReturnsAll(t *testing.T) {
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

func TestDescribeInstances_FilterByPublicIP(t *testing.T) {
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

func TestDescribeInstances_LegacyStateFilter_StillWorks(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 2)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending → running

	_, err = b.StopInstances([]string{insts[0].ID})
	require.NoError(t, err)
	b.TickLifecycleForTest() // stopping → stopped

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
// (negative MinCount/MaxCount rejection is covered by the
// TestRunInstances_MinMaxCountValidation table above)

func TestDescribeInstances_MaxResults_InvalidString_Rejected(t *testing.T) {
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

func TestDescribeInstances_StateFilterNamedStyle_PreservesTerminated(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 2)
	require.NoError(t, err)

	_, err = b.TerminateInstances([]string{insts[0].ID})
	require.NoError(t, err)
	b.TickLifecycleForTest() // shutting-down → terminated

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

func TestDescribeInstances_FilterTerminated_Visible(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	_, err = b.TerminateInstances([]string{insts[0].ID})
	require.NoError(t, err)
	b.TickLifecycleForTest() // shutting-down → terminated

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

func TestDescribeInstances_UnknownFilter_PassThrough(t *testing.T) {
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

func TestDescribeInstances_ResponseContainsXMLNamespace(t *testing.T) {
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

func TestDescribeInstances_ReservationSetPresent(t *testing.T) {
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
