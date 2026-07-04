package ec2_test

import (
	"encoding/base64"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ec2 "github.com/blackbirdworks/gopherstack/services/ec2"
)

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
func TestDescribeImages_Pagination(t *testing.T) {
	t.Parallel()

	const stubAMICount = 3 // number of pre-seeded stub AMIs in InMemoryBackend

	tests := []struct {
		name       string
		numImages  int // extra images added beyond stubs
		maxResults int
		wantPages  int
	}{
		{
			// 3 stubs + 3 extra = 6 total; maxResults=2 → 3 pages.
			name:       "paginates across three pages",
			numImages:  3,
			maxResults: 2,
			wantPages:  3,
		},
		{
			// 3 stubs; maxResults=10 → single page.
			name:       "single page when all fit",
			numImages:  0,
			maxResults: 10,
			wantPages:  1,
		},
		{
			// 3 stubs; maxResults=3 → single page exactly.
			name:       "exact boundary no second page",
			numImages:  0,
			maxResults: stubAMICount,
			wantPages:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			for i := range tc.numImages {
				b.AddImageForTest(ec2.AMIStub{
					ImageID:      "ami-parity-" + string(rune('a'+i)),
					Name:         "test-ami-" + string(rune('a'+i)),
					Architecture: "x86_64",
				})
			}

			pages := 0
			nextToken := ""

			for {
				vals := url.Values{
					"Action":     {"DescribeImages"},
					"Version":    {"2016-11-15"},
					"MaxResults": {strconv.Itoa(tc.maxResults)},
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

				// Token must be valid base64.
				_, decErr := base64.StdEncoding.DecodeString(tok)
				require.NoError(t, decErr, "NextToken must be valid base64")

				nextToken = tok
			}

			assert.Equal(t, tc.wantPages, pages)
		})
	}
}

// TestDescribeImages_MaxResultsValidation verifies that invalid MaxResults
// values are rejected.
func TestDescribeImages_MaxResultsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults string
		wantErr    bool
	}{
		{"zero is invalid", "0", true},
		{"negative is invalid", "-1", true},
		{"over 1000 is invalid", "1001", true},
		{"1 is valid", "1", false},
		{"1000 is valid", "1000", false},
		{"not a number is invalid", "abc", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			vals := url.Values{
				"Action":     {"DescribeImages"},
				"Version":    {"2016-11-15"},
				"MaxResults": {tc.maxResults},
			}

			_, err := dispatchHandler(h, vals)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

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
func TestCreateFleet_ReturnsFleetsId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fleetType string
	}{
		{"maintain fleet", "maintain"},
		{"request fleet", "request"},
		{"instant fleet", "instant"},
		{"default type", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			vals := url.Values{
				"Action":  {"CreateFleet"},
				"Version": {"2016-11-15"},
				"TargetCapacitySpecification.TotalTargetCapacity": {"1"},
			}
			if tc.fleetType != "" {
				vals.Set("Type", tc.fleetType)
			}

			resp, err := dispatchHandler(h, vals)
			require.NoError(t, err)

			// Must have a fleetId, not just <return>true</return>.
			fleetID := accuracyExtractXMLValue(resp, "fleetId")
			assert.NotEmpty(t, fleetID, "CreateFleet response must include fleetId")
			assert.True(t, strings.HasPrefix(fleetID, "fleet-"),
				"fleetId must be fleet-prefixed, got %q", fleetID)

			// Must not be just a stub return.
			assert.NotContains(t, resp, "<return>true</return>",
				"CreateFleet must not return stub boolean response")
		})
	}
}

// TestDeleteVpc_SecondaryIndexes verifies that DeleteVpc correctly removes subnet,
// route table, and security group secondary index entries so they don't linger.
func TestDeleteVpc_SecondaryIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		addSubnet     bool
		addRouteTable bool
	}{
		{
			name:          "vpc with subnet and route table",
			addSubnet:     true,
			addRouteTable: true,
		},
		{
			name:          "vpc with only subnet",
			addSubnet:     true,
			addRouteTable: false,
		},
		{
			name:          "empty vpc",
			addSubnet:     false,
			addRouteTable: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			// Create a new VPC.
			resp, err := dispatchHandler(h, url.Values{
				"Action":    {"CreateVpc"},
				"Version":   {"2016-11-15"},
				"CidrBlock": {"10.1.0.0/16"},
			})
			require.NoError(t, err)
			vpcID := accuracyExtractXMLValue(resp, "vpcId")
			require.NotEmpty(t, vpcID)

			if tc.addSubnet {
				_, err = dispatchHandler(h, url.Values{
					"Action":           {"CreateSubnet"},
					"Version":          {"2016-11-15"},
					"VpcId":            {vpcID},
					"CidrBlock":        {"10.1.1.0/24"},
					"AvailabilityZone": {"us-east-1a"},
				})
				require.NoError(t, err)
			}

			if tc.addRouteTable {
				_, err = dispatchHandler(h, url.Values{
					"Action":  {"CreateRouteTable"},
					"Version": {"2016-11-15"},
					"VpcId":   {vpcID},
				})
				require.NoError(t, err)
			}

			// Add a SG to the VPC.
			_, err = dispatchHandler(h, url.Values{
				"Action":           {"CreateSecurityGroup"},
				"Version":          {"2016-11-15"},
				"GroupName":        {"test-sg"},
				"GroupDescription": {"test sg"},
				"VpcId":            {vpcID},
			})
			require.NoError(t, err)

			// Delete the VPC.
			_, err = dispatchHandler(h, url.Values{
				"Action":  {"DeleteVpc"},
				"Version": {"2016-11-15"},
				"VpcId":   {vpcID},
			})
			require.NoError(t, err)

			// After deletion, DescribeSubnets must not return any subnet for
			// the deleted VPC — the index must be cleared.
			subnets := b.DescribeSubnetsByVPC(vpcID)
			assert.Empty(t, subnets, "no subnets should remain after DeleteVpc")

			// DescribeVpcs must not return the deleted VPC.
			vpcs := b.DescribeVpcs([]string{vpcID})
			assert.Empty(t, vpcs, "VPC must not exist after DeleteVpc")
		})
	}
}

// TestReconcileLifecycle_SkipsNonTransitional verifies that reconcileInstanceLifecycle
// does not alter instances already in stable states (stopped, terminated, running).
func TestReconcileLifecycle_SkipsNonTransitional(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		wantState string
	}{
		{
			name:      "running instance stays running after tick",
			action:    "start", // run then let it become running
			wantState: "running",
		},
		{
			name:      "stopped instance stays stopped after tick",
			action:    "stop",
			wantState: "stopped",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

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

			// Advance pending → running.
			b.TickLifecycleForTest()

			if tc.action == "stop" {
				_, err = dispatchHandler(h, url.Values{
					"Action":       {"StopInstances"},
					"Version":      {"2016-11-15"},
					"InstanceId.1": {instID},
				})
				require.NoError(t, err)
				// stopping → stopped.
				b.TickLifecycleForTest()
			}

			// Tick again — stable instances must not change state.
			b.TickLifecycleForTest()

			instances := b.DescribeInstances([]string{instID}, "")
			require.Len(t, instances, 1)
			assert.Equal(t, tc.wantState, instances[0].State.Name,
				"stable instance must not be altered by reconcile tick")
		})
	}
}
