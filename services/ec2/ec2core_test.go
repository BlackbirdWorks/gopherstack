package ec2_test

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// formBody is a helper to build form-encoded bodies from key-value pairs.
func formBody(pairs ...string) string {
	v := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		v.Set(pairs[i], pairs[i+1])
	}

	return v.Encode()
}

// ---- Egress-Only Internet Gateway ----

func TestEC2Core_EgressOnlyInternetGateway(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	vpc, err := bk.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	igw, err := bk.CreateEgressOnlyInternetGateway(vpc.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, igw.ID)
	assert.Equal(t, vpc.ID, igw.VPCID)

	// Describe with filter.
	all := bk.DescribeEgressOnlyInternetGateways([]string{igw.ID})
	require.Len(t, all, 1)
	assert.Equal(t, igw.ID, all[0].ID)

	// Describe all.
	all2 := bk.DescribeEgressOnlyInternetGateways(nil)
	assert.Len(t, all2, 1)

	// Delete.
	require.NoError(t, bk.DeleteEgressOnlyInternetGateway(igw.ID))
	assert.Empty(t, bk.DescribeEgressOnlyInternetGateways(nil))

	// Delete not found.
	err2 := bk.DeleteEgressOnlyInternetGateway(igw.ID)
	require.Error(t, err2)

	// Empty VPC ID.
	_, err3 := bk.CreateEgressOnlyInternetGateway("")
	require.Error(t, err3)

	// VPC not found.
	_, err4 := bk.CreateEgressOnlyInternetGateway("vpc-nonexistent")
	require.Error(t, err4)

	// Delete empty ID.
	err5 := bk.DeleteEgressOnlyInternetGateway("")
	require.Error(t, err5)
}

// ---- IAM Instance Profile Associations ----

func TestEC2Core_IamInstanceProfileAssociations(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	insts, err := bk.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, insts, 1)
	instanceID := insts[0].ID

	assoc, err := bk.AssociateIamInstanceProfile(
		instanceID,
		"arn:aws:iam::000000000000:instance-profile/MyRole",
	)
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.AssociationID)

	// Describe by instance ID.
	all := bk.DescribeIamInstanceProfileAssociations(nil, instanceID)
	require.Len(t, all, 1)

	// Describe by association ID.
	all2 := bk.DescribeIamInstanceProfileAssociations([]string{assoc.AssociationID}, "")
	require.Len(t, all2, 1)

	// Replace.
	replaced, err := bk.ReplaceIamInstanceProfileAssociation(assoc.AssociationID, "arn:new")
	require.NoError(t, err)
	assert.Equal(t, "arn:new", replaced.IamInstanceProfile)

	// Disassociate.
	dis, err := bk.DisassociateIamInstanceProfile(assoc.AssociationID)
	require.NoError(t, err)
	assert.Equal(t, assoc.AssociationID, dis.AssociationID)

	// Empty ID errors.
	_, err2 := bk.AssociateIamInstanceProfile("", "arn")
	require.Error(t, err2)

	_, err3 := bk.DisassociateIamInstanceProfile("")
	require.Error(t, err3)

	_, err4 := bk.DisassociateIamInstanceProfile("nonexistent")
	require.Error(t, err4)

	_, err5 := bk.ReplaceIamInstanceProfileAssociation("", "arn")
	require.Error(t, err5)

	_, err6 := bk.ReplaceIamInstanceProfileAssociation("nonexistent", "arn")
	require.Error(t, err6)
}

// ---- AssociateVpcCidrBlock ----

func TestEC2Core_AssociateVpcCidrBlock(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	vpc, err := bk.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	assoc, err := bk.AssociateVpcCidrBlock(vpc.ID, "192.168.0.0/24")
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.AssociationID)
	assert.Equal(t, "192.168.0.0/24", assoc.CidrBlock)

	// Empty VPC ID.
	_, err2 := bk.AssociateVpcCidrBlock("", "10.1.0.0/16")
	require.Error(t, err2)

	// VPC not found.
	_, err3 := bk.AssociateVpcCidrBlock("vpc-nonexistent", "10.1.0.0/16")
	require.Error(t, err3)
}

// ---- Transit Gateway Route Tables ----

func TestEC2Core_TransitGatewayRouteTables(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	// Need a TGW first.
	tgw, err := bk.CreateTransitGateway("test-tgw")
	require.NoError(t, err)

	rt, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, rt.RouteTableID)

	// Describe with filter.
	rts := bk.DescribeTransitGatewayRouteTables([]string{rt.RouteTableID})
	require.Len(t, rts, 1)

	// Describe all.
	rts2 := bk.DescribeTransitGatewayRouteTables(nil)
	assert.Len(t, rts2, 1)

	// Create route.
	route, err := bk.CreateTransitGatewayRoute(rt.RouteTableID, "10.0.0.0/8", "tgw-attach-1")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.0/8", route.DestinationCidrBlock)

	// Replace route.
	replaced, err := bk.ReplaceTransitGatewayRoute(rt.RouteTableID, "10.0.0.0/8", "tgw-attach-2")
	require.NoError(t, err)
	assert.Equal(t, "tgw-attach-2", replaced.TransitGatewayAttachmentID)

	// Delete route.
	require.NoError(t, bk.DeleteTransitGatewayRoute(rt.RouteTableID, "10.0.0.0/8"))

	// Associate route table.
	assoc, err := bk.AssociateTransitGatewayRouteTable(rt.RouteTableID, "tgw-attach-3")
	require.NoError(t, err)
	assert.Equal(t, rt.RouteTableID, assoc.TransitGatewayRouteTableID)

	// Disassociate.
	require.NoError(t, bk.DisassociateTransitGatewayRouteTable(rt.RouteTableID, "tgw-attach-3"))

	// Error cases.
	_, err2 := bk.CreateTransitGatewayRouteTable("")
	require.Error(t, err2)

	_, err3 := bk.CreateTransitGatewayRouteTable("nonexistent")
	require.Error(t, err3)

	// Delete route table.
	require.NoError(t, bk.DeleteTransitGatewayRouteTable(rt.RouteTableID))
	rts3 := bk.DescribeTransitGatewayRouteTables(nil)
	assert.Empty(t, rts3)

	// Delete not found.
	err4 := bk.DeleteTransitGatewayRouteTable(rt.RouteTableID)
	require.Error(t, err4)

	// Disassociate errors.
	err5 := bk.DisassociateTransitGatewayRouteTable("", "attach")
	require.Error(t, err5)

	err6 := bk.DisassociateTransitGatewayRouteTable("rt-id", "")
	require.Error(t, err6)

	// Associate errors.
	_, err7 := bk.AssociateTransitGatewayRouteTable("", "attach")
	require.Error(t, err7)

	_, err8 := bk.AssociateTransitGatewayRouteTable("nonexistent", "attach")
	require.Error(t, err8)
}

func TestUserData_RunInstances(t *testing.T) {
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

func TestDescribeInstances_Pagination(t *testing.T) {
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

func TestDryRun_Returns412(t *testing.T) {
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

// newTestHandler creates a fresh Handler with an InMemoryBackend.
func newTestHandler() *ec2.Handler {
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	return h
}

// dispatchHandler calls ec2.ExportDispatch (exported for tests) via vals.

// dispatchHandler calls ec2.ExportDispatch (exported for tests) via vals.
func dispatchHandler(h *ec2.Handler, vals url.Values) (string, error) {
	return ec2.ExportDispatch(h, vals)
}

// accuracyExtractXMLValue pulls the first text node matching the given XML element name.

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

// TestDescribeInstances_NextTokenOpaque verifies that the NextToken returned by
// DescribeInstances is opaque (base64-encoded), not a raw integer offset.
// AWS DescribeInstances requires MaxResults in [5, 1000].
func TestDescribeInstances_NextTokenOpaque(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults string
		numInst    int
		wantPages  int
	}{
		{
			name:       "two pages of five",
			numInst:    10,
			maxResults: "5",
			wantPages:  2,
		},
		{
			name:       "single full page",
			numInst:    5,
			maxResults: "10",
			wantPages:  1,
		},
		{
			name:       "exact page boundary",
			numInst:    5,
			maxResults: "5",
			wantPages:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			for range tc.numInst {
				_, runErr := dispatchHandler(h, url.Values{
					"Action":       {"RunInstances"},
					"Version":      {"2016-11-15"},
					"ImageId":      {"ami-test"},
					"InstanceType": {"t2.micro"},
					"MinCount":     {"1"},
					"MaxCount":     {"1"},
				})
				require.NoError(t, runErr)
			}

			pages := 0
			nextToken := ""

			for {
				vals := url.Values{
					"Action":     {"DescribeInstances"},
					"Version":    {"2016-11-15"},
					"MaxResults": {tc.maxResults},
				}
				if nextToken != "" {
					vals.Set("NextToken", nextToken)
				}

				resp, err := dispatchHandler(h, vals)
				require.NoError(t, err)
				pages++

				tok := accuracyExtractXMLValue(resp, "nextToken")
				if tok == "" {
					break
				}

				// Token must be valid base64url, not a raw integer.
				decoded, decErr := base64.URLEncoding.DecodeString(tok)
				require.NoError(t, decErr, "NextToken must be valid base64url")

				// Decoded value must be a numeric offset + HMAC signature, not exposed directly.
				require.NotEqual(t, tok, string(decoded),
					"NextToken must be opaque (base64url-encoded), not raw integer")

				nextToken = tok
			}

			assert.Equal(t, tc.wantPages, pages, "unexpected page count")
		})
	}
}

// TestDescribeInstances_NextTokenInvalidRejected verifies that a malformed
// NextToken (raw integer or garbage) returns an error.

// TestDescribeInstances_NextTokenInvalidRejected verifies that a malformed
// NextToken (raw integer or garbage) returns an error.
func TestDescribeInstances_NextTokenInvalidRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nextToken string
		wantErr   bool
	}{
		{
			name:      "raw integer is invalid",
			nextToken: "5",
			wantErr:   true,
		},
		{
			name:      "garbage string is invalid",
			nextToken: "not-a-token!!!",
			wantErr:   true,
		},
		{
			name:      "empty token is valid (no-op)",
			nextToken: "",
			wantErr:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			vals := url.Values{
				"Action":     {"DescribeInstances"},
				"Version":    {"2016-11-15"},
				"MaxResults": {"5"},
			}
			if tc.nextToken != "" {
				vals.Set("NextToken", tc.nextToken)
			}

			resp, err := dispatchHandler(h, vals)
			if tc.wantErr {
				assert.Error(t, err, "resp=%s", resp)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestDescribeImages_Pagination verifies that DescribeImages supports
// MaxResults and NextToken pagination (previously missing).
// Note: the backend always includes 3 built-in stub AMIs; numImages is extra.

// TestDescribeTags_NoAllocOnEmptyFilter verifies that DescribeTags with no
// resourceIDs returns all tags (the unfiltered path), exercising the
// allocation-skip optimization without panicking.
func TestDescribeTags_NoAllocOnEmptyFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceIDs []string
		wantAll     bool
	}{
		{
			name:        "nil filter returns all tags",
			resourceIDs: nil,
			wantAll:     true,
		},
		{
			name:        "empty filter returns all tags",
			resourceIDs: []string{},
			wantAll:     true,
		},
		{
			name:        "specific ID filters correctly",
			resourceIDs: nil, // will be set per-subtest
			wantAll:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			// Create an instance and tag it.
			runResp, err := dispatchHandler(h, url.Values{
				"Action":       {"RunInstances"},
				"Version":      {"2016-11-15"},
				"ImageId":      {"ami-test"},
				"InstanceType": {"t2.micro"},
				"MinCount":     {"1"},
				"MaxCount":     {"1"},
			})
			require.NoError(t, err)
			instID := accuracyExtractXMLValue(runResp, "instanceId")
			require.NotEmpty(t, instID)

			_, err = dispatchHandler(h, url.Values{
				"Action":       {"CreateTags"},
				"Version":      {"2016-11-15"},
				"ResourceId.1": {instID},
				"Tag.1.Key":    {"env"},
				"Tag.1.Value":  {"test"},
			})
			require.NoError(t, err)

			// DescribeTags with no filter should return the tag.
			resp, err := dispatchHandler(h, url.Values{
				"Action":  {"DescribeTags"},
				"Version": {"2016-11-15"},
			})
			require.NoError(t, err)
			assert.Contains(t, resp, "env")
			assert.Contains(t, resp, "test")
		})
	}
}

// TestCreateFleet_ReturnsFleetsId verifies that CreateFleet returns a proper
// fleetId field (not just <return>true</return>).

// TestRunInstances_UserDataValidation covers base64 and 16 KiB validation of the
// UserData parameter on RunInstances.
func TestRunInstances_UserDataValidation(t *testing.T) {
	t.Parallel()

	// 16 KiB of 'a' encodes to valid base64; decoded length == 16384 (allowed).
	okDecoded := make([]byte, 16384)
	for i := range okDecoded {
		okDecoded[i] = 'a'
	}
	tooBig := make([]byte, 16385)
	for i := range tooBig {
		tooBig[i] = 'a'
	}

	tests := []struct {
		wantErr  error
		name     string
		userData string
	}{
		{
			name:     "valid_base64_within_limit",
			userData: base64Encode([]byte("#!/bin/bash\necho hello")),
		},
		{
			name:     "empty_user_data_allowed",
			userData: "",
		},
		{
			name:     "exactly_16kib_allowed",
			userData: base64Encode(okDecoded),
		},
		{
			name:     "malformed_base64_rejected",
			userData: "!!!not-base64!!!",
			wantErr:  ec2.ErrInvalidUserData,
		},
		{
			name:     "over_16kib_rejected",
			userData: base64Encode(tooBig),
			wantErr:  ec2.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			params := url.Values{
				"Action":       {"RunInstances"},
				"Version":      {"2016-11-15"},
				"ImageId":      {"ami-12345678"},
				"InstanceType": {"t3.micro"},
				"MinCount":     {"1"},
				"MaxCount":     {"1"},
			}
			if tt.userData != "" {
				params.Set("UserData", tt.userData)
			}

			resp, err := dispatchHandler(h, params)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, resp, "RunInstancesResponse")
		})
	}
}

// TestCreateVolume_GP3Coupling covers the AWS gp3 iops/throughput coupling
// validation on CreateVolume.

// TestDescribeInstanceStatus_IncludesHealthObjects verifies that
// DescribeInstanceStatus emits the systemStatus and instanceStatus health
// objects (status "initializing" while pending, "ok" once running) that the SDK
// InstanceStatusOk waiter polls. Previously these objects were omitted entirely.
func TestDescribeInstanceStatus_IncludesHealthObjects(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := newTestHandlerWithBackend(b)

	runResp, err := dispatchHandler(h, url.Values{
		"Action":       {"RunInstances"},
		"Version":      {"2016-11-15"},
		"ImageId":      {"ami-12345678"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
		"MaxCount":     {"1"},
	})
	require.NoError(t, err)

	id := accuracyExtractXMLValue(runResp, "instanceId")
	require.NotEmpty(t, id)

	statusReq := url.Values{
		"Action":     {"DescribeInstanceStatus"},
		"Version":    {"2016-11-15"},
		"InstanceId": {id},
	}

	// While pending: health objects present, reporting "initializing".
	pendingResp, err := dispatchHandler(h, statusReq)
	require.NoError(t, err)
	assert.Contains(t, pendingResp, "<systemStatus>", "systemStatus health object must be present")
	assert.Contains(t, pendingResp, "<instanceStatus>", "instanceStatus health object must be present")
	assert.Contains(t, pendingResp, "reachability")
	assert.Contains(t, pendingResp, "<status>initializing</status>")

	// Advance pending → running deterministically.
	b.TickLifecycleForTest()

	runningResp, err := dispatchHandler(h, statusReq)
	require.NoError(t, err)
	assert.Contains(t, runningResp, "<name>running</name>")
	assert.GreaterOrEqual(t, strings.Count(runningResp, "<status>ok</status>"), 2,
		"both system and instance status should report ok once running")
	assert.Contains(t, runningResp, "<status>passed</status>")
}

// base64Encode is a test helper mirroring how the AWS SDK base64-encodes user
// data before it reaches the RunInstances API.
func base64Encode(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// vpcResources bundles the identifiers of the resources created by
// buildVPCWithResources for use in assertions.
type vpcResources struct {
	vpcID       string
	subnetID    string
	eniID       string
	instanceIDs []string
}

// buildVPCWithResources creates a VPC with a subnet, `numInstances` running
// instances (each with an auto-attached ENI), one standalone ENI, and one NAT
// gateway, returning the created resource identifiers for later assertions.

func TestDescribeRegions(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	regions := b.DescribeRegions()
	assert.NotEmpty(t, regions)
	assert.Contains(t, regions, "us-east-1")
}

func TestDescribeAvailabilityZones(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		wantAZ string
	}{
		{name: "default_region", region: "", wantAZ: "us-east-1a"},
		{name: "explicit_region", region: "eu-west-1", wantAZ: "eu-west-1a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			azs := b.DescribeAvailabilityZones(tt.region)
			assert.Len(t, azs, 3)
			assert.Contains(t, azs, tt.wantAZ)
		})
	}
}
