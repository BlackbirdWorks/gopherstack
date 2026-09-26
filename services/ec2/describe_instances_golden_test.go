package ec2_test

import (
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// normalizeDescribeInstancesGolden blanks out EC2 wire elements whose content
// is randomly generated per process run (resource IDs, request IDs,
// wall-clock timestamps), so they can never byte-match the checked-in golden
// file regardless of code correctness. Normalizing them is what makes a
// golden-bytes comparison meaningful: it still catches any change to
// tag/security-group/IAM-profile projection, filtering, or ordering, while
// ignoring values that were never deterministic to begin with.
func normalizeDescribeInstancesGolden(b []byte) []byte {
	volatileTags := []string{
		"requestId", "reservationId", "instanceId", "vpcId", "subnetId", "groupId", "launchTime",
	}

	for _, tag := range volatileTags {
		re := regexp.MustCompile(`<` + tag + `>[^<]*</` + tag + `>`)
		b = re.ReplaceAll(b, []byte(`<`+tag+`>NORMALIZED</`+tag+`>`))
	}

	return b
}

// seedDescribeInstancesGoldenBackend seeds a fixed backend across 3 VPCs and
// 5 security groups. Must stay byte-identical to the seed used to generate
// testdata/describe_instances_golden_*.xml (from HEAD before the batch-lookup
// optimization in this package) — see PARITY.md Notes. Returns instance IDs
// in creation order: callers must describe by explicit InstanceId.N, never
// an unfiltered describe, since store.Table.All()'s Go-map order is
// unspecified and would make the golden non-reproducible.
func seedDescribeInstancesGoldenBackend(tb testing.TB) (*ec2.Handler, []string) {
	tb.Helper()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpcIDs := make([]string, 3)
	subnetIDs := make([]string, 3)

	for i := range 3 {
		vpc, err := bk.CreateVpc("10."+strconv.Itoa(i)+".0.0/16", "default")
		require.NoError(tb, err)

		sub, err := bk.CreateSubnet(vpc.ID, "10."+strconv.Itoa(i)+".0.0/24", "us-east-1a")
		require.NoError(tb, err)

		vpcIDs[i] = vpc.ID
		subnetIDs[i] = sub.ID
	}

	sgIDs := make([]string, 5)

	for i := range 5 {
		sg, err := bk.CreateSecurityGroup("bench-sg-"+strconv.Itoa(i), "bench security group", vpcIDs[i%3])
		require.NoError(tb, err)

		sgIDs[i] = sg.ID
	}

	const total = 40

	ids := make([]string, total)

	for i := range total {
		insts, err := bk.RunInstances("ami-bench", "t3.micro", subnetIDs[i%3], 1)
		require.NoError(tb, err)

		inst := insts[0]
		ids[i] = inst.ID

		groups := []string{sgIDs[i%5], sgIDs[(i+1)%5]}
		require.NoError(tb, bk.SetInstanceLaunchConfig(inst.ID, "bench-key", groups))

		tags := map[string]string{
			"Name":        "bench-instance-" + strconv.Itoa(i),
			"Environment": []string{"prod", "staging", "dev"}[i%3],
			"Team":        []string{"platform", "data", "web"}[i%3],
		}
		require.NoError(tb, bk.CreateTags([]string{inst.ID}, tags))

		if i%4 == 0 {
			_, assocErr := bk.AssociateIamInstanceProfile(
				inst.ID, "arn:aws:iam::000000000000:instance-profile/bench-profile",
			)
			require.NoError(tb, assocErr)
		}
	}

	return h, ids
}

func describeInstancesGolden(tb testing.TB, h *ec2.Handler, form url.Values) []byte {
	tb.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(tb, h.Handler()(c))
	require.Equal(tb, http.StatusOK, rec.Code, rec.Body.String())

	return normalizeDescribeInstancesGolden(rec.Body.Bytes())
}

// TestDescribeInstances_WireOutputUnchanged pins the DescribeInstances wire
// output (tags, security-group names, IAM instance profile, filtering) to a
// golden file captured from the pre-optimization code, so a future edit to
// the batched tag/security-group/IAM lookups in handler_instances_lifecycle.go
// is caught the moment it changes what a client actually receives.
func TestDescribeInstances_WireOutputUnchanged(t *testing.T) {
	t.Parallel()

	h, ids := seedDescribeInstancesGoldenBackend(t)

	all := url.Values{"Action": {"DescribeInstances"}, "Version": {"2016-11-15"}}
	for i, id := range ids {
		all.Set(fmt.Sprintf("InstanceId.%d", i+1), id)
	}

	filtered := url.Values{}
	maps.Copy(filtered, all)

	filtered.Set("Filter.1.Name", "tag:Environment")
	filtered.Set("Filter.1.Value.1", "prod")

	cases := []struct {
		name     string
		form     url.Values
		testdata string
	}{
		{"all instances by explicit id", all, "testdata/describe_instances_golden_all.xml"},
		{"filtered by tag", filtered, "testdata/describe_instances_golden_filtered.xml"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			want, err := os.ReadFile(tc.testdata)
			require.NoError(t, err)

			got := describeInstancesGolden(t, h, tc.form)
			require.Equal(t, string(want), string(got))
		})
	}
}
