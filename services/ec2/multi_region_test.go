package ec2_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *ec2.Handler {
	t.Helper()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", mrHome))
	h.Region = mrHome
	h.EnableRegions(t.Context())

	return h
}

func regionCall(t *testing.T, h *ec2.Handler, region, action string, kv ...string) string {
	t.Helper()

	form := url.Values{"Action": {action}, "Version": {"2016-11-15"}}
	for i := 0; i+1 < len(kv); i += 2 {
		form.Set(kv[i], kv[i+1])
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return rec.Body.String()
}

func firstMatch(t *testing.T, pattern, body string) string {
	t.Helper()

	m := regexp.MustCompile(pattern).FindStringSubmatch(body)
	require.Len(t, m, 2, "pattern %q not in %s", pattern, body)

	return m[1]
}

func keyNames(t *testing.T, h *ec2.Handler, region string) []string {
	t.Helper()

	body := regionCall(t, h, region, "DescribeKeyPairs")
	matches := regexp.MustCompile(`<keyName>([^<]*)</keyName>`).FindAllStringSubmatch(body, -1)
	out := make([]string, 0, len(matches))

	for _, m := range matches {
		out = append(out, m[1])
	}

	return out
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{"us-east-1", "eu-west-1"}},
		{name: "three-regions", regions: []string{"us-east-1", "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)

			for _, r := range tc.regions {
				regionCall(t, h, r, "CreateKeyPair", "KeyName", "shared")
				regionCall(t, h, r, "CreateKeyPair", "KeyName", "only-"+r)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, keyNames(t, h, r))
			}
		})
	}
}

func TestHandler_MultiRegionDefaultNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home", region: mrHome},
		{name: "peer", region: "eu-west-1"},
		{name: "other-peer", region: "ap-south-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)

			subnets := regionCall(t, h, tc.region, "DescribeSubnets")
			assert.Equal(t, tc.region+"a", firstMatch(t, `<availabilityZone>([^<]*)</availabilityZone>`, subnets))
			assert.Contains(t, subnets, "<defaultForAz>true</defaultForAz>")

			zones := regionCall(t, h, tc.region, "DescribeAvailabilityZones")
			for _, suffix := range []string{"a", "b", "c"} {
				assert.Contains(t, zones, "<zoneName>"+tc.region+suffix+"</zoneName>")
			}

			assert.Contains(t, regionCall(t, h, tc.region, "DescribeVpcs"), "<isDefault>true</isDefault>")
			assert.Contains(t, regionCall(t, h, tc.region, "DescribeSecurityGroups"), "<groupName>default</groupName>")

			vpc := firstMatch(t, `<vpcId>([^<]*)</vpcId>`,
				regionCall(t, h, tc.region, "CreateVpc", "CidrBlock", "10.9.0.0/16"))
			assert.Contains(t, regionCall(t, h, tc.region, "DescribeVpcs"), vpc)

			other := "us-west-2"
			assert.NotContains(t, regionCall(t, h, other, "DescribeVpcs"), vpc)
		})
	}
}

func TestHandler_MultiRegionPeering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create   func(t *testing.T, h *ec2.Handler) string
		describe string
		accept   string
		remove   string
		idKey    string
		state    string
		wantOpen string
		wantDone string
		name     string
	}{
		{
			name: "vpc-peering",
			create: func(t *testing.T, h *ec2.Handler) string {
				t.Helper()

				vpc := firstMatch(t, `<vpcId>([^<]*)</vpcId>`,
					regionCall(t, h, mrHome, "CreateVpc", "CidrBlock", "10.1.0.0/16"))

				return firstMatch(t, `<vpcPeeringConnectionId>([^<]*)</vpcPeeringConnectionId>`,
					regionCall(t, h, mrHome, "CreateVpcPeeringConnection",
						"VpcId", vpc, "PeerVpcId", "vpc-remote", "PeerRegion", "eu-west-1"))
			},
			describe: "DescribeVpcPeeringConnections",
			accept:   "AcceptVpcPeeringConnection",
			remove:   "DeleteVpcPeeringConnection",
			idKey:    "VpcPeeringConnectionId",
			state:    `<code>([^<]*)</code>`,
			wantOpen: "pending-acceptance",
			wantDone: "active",
		},
		{
			name: "tgw-peering",
			create: func(t *testing.T, h *ec2.Handler) string {
				t.Helper()

				return firstMatch(t, `<transitGatewayAttachmentId>([^<]*)</transitGatewayAttachmentId>`,
					regionCall(t, h, mrHome, "CreateTransitGatewayPeeringAttachment",
						"TransitGatewayId", "tgw-local", "PeerTransitGatewayId", "tgw-remote",
						"PeerAccountId", "000000000000", "PeerRegion", "eu-west-1"))
			},
			describe: "DescribeTransitGatewayPeeringAttachments",
			accept:   "AcceptTransitGatewayPeeringAttachment",
			remove:   "DeleteTransitGatewayPeeringAttachment",
			idKey:    "TransitGatewayAttachmentId",
			state:    `<state>([^<]*)</state>`,
			wantOpen: "pendingAcceptance",
			wantDone: "available",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)
			id := tc.create(t, h)

			for _, region := range []string{mrHome, "eu-west-1"} {
				body := regionCall(t, h, region, tc.describe)
				assert.Contains(t, body, id)
				assert.Equal(t, tc.wantOpen, firstMatch(t, tc.state, body))
			}

			assert.NotContains(t, regionCall(t, h, "ap-south-1", tc.describe), id)

			regionCall(t, h, "eu-west-1", tc.accept, tc.idKey, id)

			for _, region := range []string{mrHome, "eu-west-1"} {
				assert.Equal(t, tc.wantDone, firstMatch(t, tc.state, regionCall(t, h, region, tc.describe)))
			}

			regionCall(t, h, "eu-west-1", tc.remove, tc.idKey, id)

			for _, region := range []string{mrHome, "eu-west-1"} {
				assert.NotContains(t, regionCall(t, h, region, tc.describe), id)
			}
		})
	}
}

func TestHandler_MultiRegionCrossRegionCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		register   func(t *testing.T, h *ec2.Handler, snap string) string
		name       string
		copyAction string
		sourceKey  string
		idPattern  string
		describe   string
	}{
		{
			name:       "snapshot",
			copyAction: "CopySnapshot",
			sourceKey:  "SourceSnapshotId",
			idPattern:  `<snapshotId>([^<]*)</snapshotId>`,
			describe:   "DescribeSnapshots",
			register:   func(_ *testing.T, _ *ec2.Handler, snap string) string { return snap },
		},
		{
			name:       "image",
			copyAction: "CopyImage",
			sourceKey:  "SourceImageId",
			idPattern:  `<imageId>([^<]*)</imageId>`,
			describe:   "DescribeImages",
			register: func(t *testing.T, h *ec2.Handler, snap string) string {
				t.Helper()

				return firstMatch(t, `<imageId>([^<]*)</imageId>`, regionCall(t, h, mrHome, "RegisterImage",
					"Name", "src", "RootDeviceName", "/dev/xvda",
					"BlockDeviceMapping.1.DeviceName", "/dev/xvda",
					"BlockDeviceMapping.1.Ebs.SnapshotId", snap))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)
			vol := firstMatch(t, `<volumeId>([^<]*)</volumeId>`,
				regionCall(t, h, mrHome, "CreateVolume", "AvailabilityZone", mrHome+"a", "Size", "8"))
			snap := firstMatch(t, `<snapshotId>([^<]*)</snapshotId>`,
				regionCall(t, h, mrHome, "CreateSnapshot", "VolumeId", vol))
			source := tc.register(t, h, snap)

			copied := firstMatch(t, tc.idPattern, regionCall(t, h, "eu-west-1", tc.copyAction,
				tc.sourceKey, source, "SourceRegion", mrHome, "Name", "copy"))

			assert.Contains(t, regionCall(t, h, "eu-west-1", tc.describe, "Owner.1", "self"), copied)
			assert.NotContains(t, regionCall(t, h, mrHome, tc.describe, "Owner.1", "self"), copied)
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler(t)
			regionCall(t, src, mrHome, "CreateKeyPair", "KeyName", "home")

			if tc.remote {
				regionCall(t, src, "eu-west-1", "CreateKeyPair", "KeyName", "eu")
			}

			snap := src.Snapshot(t.Context())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.(*ec2.InMemoryBackend).Snapshot(t.Context()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Equal(t, []string{"home"}, keyNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(keyNames(t, dst, "eu-west-1")) == 1)

			old := newRegionHandler(t)
			require.NoError(t, old.Backend.(*ec2.InMemoryBackend).Restore(t.Context(), snap))
			assert.Equal(t, []string{"home"}, keyNames(t, old, mrHome))
		})
	}
}
