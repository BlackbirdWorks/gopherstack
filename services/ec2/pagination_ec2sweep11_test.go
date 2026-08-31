package ec2_test

// Pagination fixes for the eleven ops named by the ec2sweep11 audit: each of
// DescribeInstanceCreditSpecifications, DescribeInstanceTopology,
// DescribeInstanceConnectEndpoints, DescribeInstanceEventWindows,
// DescribeElasticGpus, DescribeImportImageTasks, DescribeExportImageTasks,
// ListImagesInRecycleBin, DescribeFastLaunchImages, DescribeImageReferences,
// and DescribeInstanceImageMetadata defines MaxResults/NextToken on its real
// SDK input but the handler ignored both, always returning every item in one
// page with no NextToken. That's the "unbounded single page" form, not the
// medialive infinite-loop form: no cap was ever applied, so a real paginator
// stopped after one call instead of looping forever.
//
// Each test below drives the real aws-sdk-go-v2 generated Paginator across at
// least three pages over five seeded items and asserts the returned IDs are
// disjoint across pages -- the assertion shape that fails pre-fix, because
// pre-fix the first (only) page contains all five IDs already.

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	ec2sweep11SeedCount  = 5
	ec2sweep11MaxResults = 2
	ec2sweep11LoopGuard  = 10 // defensive cap; none of these ops are the infinite-loop bug class
)

// assertDisjointPages verifies every ID across all pages is unique and the
// total matches wantTotal -- the shape that catches "ignored MaxResults,
// dumped everything in page one".
func assertDisjointPages(t *testing.T, pages [][]string, wantTotal int) {
	t.Helper()

	seen := make(map[string]bool, wantTotal)
	total := 0

	for _, p := range pages {
		for _, id := range p {
			require.Falsef(t, seen[id], "id %q returned on more than one page", id)
			seen[id] = true
			total++
		}
	}

	assert.Equal(t, wantTotal, total)
	assert.GreaterOrEqual(t, len(pages), 2, "expected pagination to split results across multiple pages")
}

func newTestBackendAndClient(t *testing.T) (*ec2.InMemoryBackend, *ec2sdk.Client) {
	t.Helper()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	return b, newTestEC2Client(t, h)
}

func TestDescribeInstanceCreditSpecifications_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	_, err := b.RunInstances("ami-parity-test", "t3.micro", "", ec2sweep11SeedCount)
	require.NoError(t, err)

	paginator := ec2sdk.NewDescribeInstanceCreditSpecificationsPaginator(
		client, &ec2sdk.DescribeInstanceCreditSpecificationsInput{},
		func(o *ec2sdk.DescribeInstanceCreditSpecificationsPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, ec2sweep11LoopGuard, "paginator did not terminate")

		out, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.InstanceCreditSpecifications))
		for _, s := range out.InstanceCreditSpecifications {
			ids = append(ids, aws.ToString(s.InstanceId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep11SeedCount)
}

func TestDescribeInstanceTopology_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	_, err := b.RunInstances("ami-parity-test", "t3.micro", "", ec2sweep11SeedCount)
	require.NoError(t, err)

	paginator := ec2sdk.NewDescribeInstanceTopologyPaginator(
		client, &ec2sdk.DescribeInstanceTopologyInput{},
		func(o *ec2sdk.DescribeInstanceTopologyPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, ec2sweep11LoopGuard, "paginator did not terminate")

		out, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.Instances))
		for _, inst := range out.Instances {
			ids = append(ids, aws.ToString(inst.InstanceId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep11SeedCount)
}

func TestDescribeInstanceConnectEndpoints_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	for range ec2sweep11SeedCount {
		_, err := b.CreateInstanceConnectEndpoint("subnet-default", nil, false)
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeInstanceConnectEndpointsPaginator(
		client, &ec2sdk.DescribeInstanceConnectEndpointsInput{},
		func(o *ec2sdk.DescribeInstanceConnectEndpointsPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, ec2sweep11LoopGuard, "paginator did not terminate")

		out, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.InstanceConnectEndpoints))
		for _, ep := range out.InstanceConnectEndpoints {
			ids = append(ids, aws.ToString(ep.InstanceConnectEndpointId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep11SeedCount)
}

func TestDescribeInstanceEventWindows_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	// The real SDK doc puts MaxResults' floor at 20 ("between 20 and 500"),
	// so unlike the other ops here five seeded items can never force a
	// second page: seed enough windows to exceed one page at the minimum
	// legal MaxResults instead.
	const (
		eventWindowsMaxResults = 20 // api_op_DescribeInstanceEventWindows.go: "between 20 and 500"
		eventWindowsSeedCount  = 45
	)

	for i := range eventWindowsSeedCount {
		_, err := b.CreateInstanceEventWindow(fmt.Sprintf("window-%d", i), "")
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeInstanceEventWindowsPaginator(
		client, &ec2sdk.DescribeInstanceEventWindowsInput{},
		func(o *ec2sdk.DescribeInstanceEventWindowsPaginatorOptions) {
			o.Limit = eventWindowsMaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, ec2sweep11LoopGuard, "paginator did not terminate")

		out, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.InstanceEventWindows))
		for _, ew := range out.InstanceEventWindows {
			ids = append(ids, aws.ToString(ew.InstanceEventWindowId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, eventWindowsSeedCount)
}

func TestDescribeElasticGpus_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	// DescribeElasticGpus has no generated SDK Paginator (Elastic Graphics
	// was retired in 2023 -- see InMemoryBackend.DescribeElasticGpus's doc
	// comment) even though its Input still carries MaxResults/NextToken.
	// The backend never populates any entries, so there is no way to seed a
	// second page; this exercises the now-honored MaxResults validation and
	// NextToken decoding directly instead.
	out, err := client.DescribeElasticGpus(t.Context(), &ec2sdk.DescribeElasticGpusInput{
		MaxResults: aws.Int32(5), // real SDK doc: "between 5 and 1000"
	})
	require.NoError(t, err)
	assert.Empty(t, out.ElasticGpuSet)
	assert.Nil(t, out.NextToken)

	_, err = client.DescribeElasticGpus(t.Context(), &ec2sdk.DescribeElasticGpusInput{
		MaxResults: aws.Int32(1), // below the real SDK doc's "between 5 and 1000"
	})
	require.Error(t, err)
}

func TestDescribeImportImageTasks_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	for range ec2sweep11SeedCount {
		_, err := b.ImportImage("", "x86_64", "", false, "")
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeImportImageTasksPaginator(
		client, &ec2sdk.DescribeImportImageTasksInput{},
		func(o *ec2sdk.DescribeImportImageTasksPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, ec2sweep11LoopGuard, "paginator did not terminate")

		out, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.ImportImageTasks))
		for _, task := range out.ImportImageTasks {
			ids = append(ids, aws.ToString(task.ImportTaskId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep11SeedCount)
}

func TestDescribeExportImageTasks_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	for range ec2sweep11SeedCount {
		_, err := b.ExportImage("ami-parity-test", "", "", "", "", "")
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeExportImageTasksPaginator(
		client, &ec2sdk.DescribeExportImageTasksInput{},
		func(o *ec2sdk.DescribeExportImageTasksPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, ec2sweep11LoopGuard, "paginator did not terminate")

		out, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.ExportImageTasks))
		for _, task := range out.ExportImageTasks {
			ids = append(ids, aws.ToString(task.ExportImageTaskId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep11SeedCount)
}

func TestDescribeFastLaunchImages_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	for i := range ec2sweep11SeedCount {
		imageID := "ami-fastlaunch-" + string(rune('a'+i))
		require.NoError(t, b.EnableFastLaunch(imageID, ec2.FastLaunchConfig{}))
	}

	paginator := ec2sdk.NewDescribeFastLaunchImagesPaginator(
		client, &ec2sdk.DescribeFastLaunchImagesInput{},
		func(o *ec2sdk.DescribeFastLaunchImagesPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, ec2sweep11LoopGuard, "paginator did not terminate")

		out, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.FastLaunchImages))
		for _, img := range out.FastLaunchImages {
			ids = append(ids, aws.ToString(img.ImageId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep11SeedCount)
}

func TestDescribeImageReferences_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	const imageID = "ami-referenced"

	_, err := b.RunInstances(imageID, "t3.micro", "", ec2sweep11SeedCount)
	require.NoError(t, err)

	paginator := ec2sdk.NewDescribeImageReferencesPaginator(
		client, &ec2sdk.DescribeImageReferencesInput{
			ImageIds: []string{imageID},
		},
		func(o *ec2sdk.DescribeImageReferencesPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, ec2sweep11LoopGuard, "paginator did not terminate")

		out, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.ImageReferences))
		for j, ref := range out.ImageReferences {
			// DescribeImageReferences entries aren't individually IDed on the
			// wire (they're all the same ImageId); disambiguate by resource
			// ARN plus a page-local index so disjointness still catches
			// "every page contains all five".
			ids = append(ids, aws.ToString(ref.Arn)+"#"+string(rune('0'+j)))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep11SeedCount)
}

func TestDescribeInstanceImageMetadata_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	_, err := b.RunInstances("ami-parity-test", "t3.micro", "", ec2sweep11SeedCount)
	require.NoError(t, err)

	paginator := ec2sdk.NewDescribeInstanceImageMetadataPaginator(
		client, &ec2sdk.DescribeInstanceImageMetadataInput{},
		func(o *ec2sdk.DescribeInstanceImageMetadataPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for i := 0; paginator.HasMorePages(); i++ {
		require.Lessf(t, i, ec2sweep11LoopGuard, "paginator did not terminate")

		out, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(out.InstanceImageMetadata))
		for _, m := range out.InstanceImageMetadata {
			ids = append(ids, aws.ToString(m.InstanceId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep11SeedCount)
}

// TestListImagesInRecycleBin_Pagination covers MaxResults/NextToken
// validation only, not disjoint multi-page IDs: this backend has no
// SDK-reachable way to populate the recycle bin at all (DeregisterImage
// deletes AMIs outright rather than moving them to it -- a pre-existing gap,
// not something this pagination fix should also invent), so it can never
// return more than zero items through any real client.
func TestListImagesInRecycleBin_Pagination(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	out, err := client.ListImagesInRecycleBin(t.Context(), &ec2sdk.ListImagesInRecycleBinInput{
		MaxResults: aws.Int32(ec2sweep11MaxResults),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Images)
	assert.Nil(t, out.NextToken)

	_, err = client.ListImagesInRecycleBin(t.Context(), &ec2sdk.ListImagesInRecycleBinInput{
		NextToken: aws.String("not-a-real-token"),
	})
	require.Error(t, err, "a forged/invalid NextToken must be rejected")
}
