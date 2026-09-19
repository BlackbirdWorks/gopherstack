package ec2_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// seedDescribeInstancesBenchBackend seeds n instances spread across a handful
// of VPCs/subnets and security groups, each tagged and every 5th one carrying
// an IAM instance profile association -- representative of the tag/SG/IAM
// per-instance lookups handleDescribeInstances performs.
func seedDescribeInstancesBenchBackend(b *testing.B, n int) *ec2.Handler {
	b.Helper()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	const numVPCs = 5

	subnetIDs := make([]string, numVPCs)
	sgIDs := make([]string, numVPCs*2)

	for i := range numVPCs {
		vpc, err := bk.CreateVpc("10."+strconv.Itoa(i)+".0.0/16", "default")
		require.NoError(b, err)

		sub, err := bk.CreateSubnet(vpc.ID, "10."+strconv.Itoa(i)+".0.0/24", "us-east-1a")
		require.NoError(b, err)

		subnetIDs[i] = sub.ID

		for j := range 2 {
			sg, sgErr := bk.CreateSecurityGroup(
				"bench-sg-"+strconv.Itoa(i)+"-"+strconv.Itoa(j), "bench security group", vpc.ID,
			)
			require.NoError(b, sgErr)

			sgIDs[i*2+j] = sg.ID
		}
	}

	for i := range n {
		insts, err := bk.RunInstances("ami-bench", "t3.micro", subnetIDs[i%numVPCs], 1)
		require.NoError(b, err)

		inst := insts[0]

		groups := []string{sgIDs[i%len(sgIDs)], sgIDs[(i+1)%len(sgIDs)]}
		require.NoError(b, bk.SetInstanceLaunchConfig(inst.ID, "bench-key", groups))

		tags := map[string]string{
			"Name":        "bench-instance-" + strconv.Itoa(i),
			"Environment": []string{"prod", "staging", "dev"}[i%3],
			"Team":        []string{"platform", "data", "web"}[i%3],
		}
		require.NoError(b, bk.CreateTags([]string{inst.ID}, tags))

		if i%5 == 0 {
			_, assocErr := bk.AssociateIamInstanceProfile(
				inst.ID, "arn:aws:iam::000000000000:instance-profile/bench-profile",
			)
			require.NoError(b, assocErr)
		}
	}

	return h
}

func benchmarkDescribeInstances(b *testing.B, n int) {
	b.Helper()

	h := seedDescribeInstancesBenchBackend(b, n)
	e := echo.New()
	const body = "Action=DescribeInstances&Version=2016-11-15"

	b.ResetTimer()

	for range b.N {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		if err := h.Handler()(c); err != nil {
			b.Fatal(err)
		}

		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d", rec.Code)
		}
	}
}

func BenchmarkDescribeInstances_100(b *testing.B)  { benchmarkDescribeInstances(b, 100) }
func BenchmarkDescribeInstances_1000(b *testing.B) { benchmarkDescribeInstances(b, 1000) }
