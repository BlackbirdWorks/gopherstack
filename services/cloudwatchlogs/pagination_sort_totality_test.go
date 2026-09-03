package cloudwatchlogs_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/require"
)

// walkAttempts is how many times each paginated walk is repeated against the
// same, unchanged backend state. Go randomises map iteration order per
// range, not per map instance, so a non-total sort over store.Table.All()
// can (and, per the glue precedent, reliably does) disagree with itself
// across separate calls with nothing changed in between. One walk can pass
// by luck; the bug is about instability *across* calls.
const walkAttempts = 30

// walkAndVerify repeats a small-page paginated walk walkAttempts times,
// failing if any attempt drops or duplicates an item relative to want, or
// returns the same id on two different pages within one walk.
func walkAndVerify(t *testing.T, want map[string]bool, listPage func(token string) (ids []string, next string)) {
	t.Helper()

	for attempt := range walkAttempts {
		got := make(map[string]bool, len(want))
		token := ""

		for {
			ids, next := listPage(token)
			for _, id := range ids {
				require.Falsef(t, got[id], "attempt %d: id %q returned on more than one page", attempt, id)
				got[id] = true
			}

			if next == "" {
				break
			}

			token = next
		}

		require.Equalf(t, want, got, "attempt %d: paginated walk did not reproduce the created set exactly", attempt)
	}
}

func TestListLogAnomalyDetectorsSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	const tie = 1700000000000

	want := make(map[string]bool, 3)
	for i := range 3 {
		arn := fmt.Sprintf("arn:aws:logs:us-east-1:111111111111:anomaly-detector:d-%03d", i)
		cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
			AnomalyDetectorArn: arn,
			CreationTimeStamp:  tie,
		})
		want[arn] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, err := b.ListLogAnomalyDetectors(nil, 1, token)
		require.NoError(t, err)
		ids := make([]string, len(page))
		for i, d := range page {
			ids[i] = d.AnomalyDetectorArn
		}

		return ids, next
	})
}

func TestListAnomaliesSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	const tie = 1700000000000
	detectorArn := "arn:aws:logs:us-east-1:111111111111:anomaly-detector:d-1"
	cloudwatchlogs.AddLogAnomalyDetectorInternal(b, cloudwatchlogs.LogAnomalyDetector{
		AnomalyDetectorArn: detectorArn,
		CreationTimeStamp:  tie,
	})

	want := make(map[string]bool, 3)
	for i := range 3 {
		id := fmt.Sprintf("anomaly-%03d", i)
		cloudwatchlogs.AddAnomalyInternal(b, cloudwatchlogs.Anomaly{
			AnomalyID:          id,
			AnomalyDetectorArn: detectorArn,
			FirstSeen:          tie,
		})
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, err := b.ListAnomalies(detectorArn, 1, token)
		require.NoError(t, err)
		ids := make([]string, len(page))
		for i, a := range page {
			ids[i] = a.AnomalyID
		}

		return ids, next
	})
}

func TestDescribeDeliveriesSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	const tie = 1700000000000

	want := make(map[string]bool, 3)
	for i := range 3 {
		id := fmt.Sprintf("delivery-%03d", i)
		cloudwatchlogs.AddDeliveryInternal(b, cloudwatchlogs.Delivery{
			ID:                     id,
			Arn:                    "arn:aws:logs:us-east-1:111111111111:delivery:" + id,
			DeliverySourceName:     "src",
			DeliveryDestinationArn: "arn:aws:logs:us-east-1:111111111111:delivery-destination:dst",
			CreationTime:           tie,
		})
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, err := b.DescribeDeliveries(1, token)
		require.NoError(t, err)
		ids := make([]string, len(page))
		for i, d := range page {
			ids[i] = d.ID
		}

		return ids, next
	})
}

func TestDescribeExportTasksSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	const tie = 1700000000000

	want := make(map[string]bool, 3)
	for i := range 3 {
		id := fmt.Sprintf("task-%03d", i)
		cloudwatchlogs.AddExportTaskInternal(b, cloudwatchlogs.ExportTask{
			TaskID:       id,
			LogGroupName: "/lg",
			Destination:  "bucket",
			Status:       "PENDING",
			CreationTime: tie,
		})
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, err := b.DescribeExportTasks("", "", 1, token)
		require.NoError(t, err)
		ids := make([]string, len(page))
		for i, task := range page {
			ids[i] = task.TaskID
		}

		return ids, next
	})
}

func TestDescribeImportTasksSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	const tie = 1700000000000

	want := make(map[string]bool, 3)
	for i := range 3 {
		id := fmt.Sprintf("import-%03d", i)
		cloudwatchlogs.AddImportTaskInternal(b, cloudwatchlogs.ImportTask{
			ImportID:             id,
			ImportSourceArn:      "arn:aws:cloudtrail:us-east-1:111111111111:eventdatastore/x",
			ImportDestinationArn: "arn:aws:logs:us-east-1:111111111111:log-group:/aws/cloudtrail/" + id,
			Status:               "IN_PROGRESS",
			CreationTime:         tie,
		})
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, err := b.DescribeImportTasks("", 1, token)
		require.NoError(t, err)
		ids := make([]string, len(page))
		for i, task := range page {
			ids[i] = task.ImportID
		}

		return ids, next
	})
}

func TestListSourcesForS3TableIntegrationSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	const tie = 1700000000000
	integrationArn := "arn:aws:logs:us-east-1:111111111111:integration:s3tables"

	want := make(map[string]bool, 3)
	for i := range 3 {
		id := fmt.Sprintf("source-%03d", i)
		cloudwatchlogs.AddS3TableIntegrationSourceInternal(b, id, integrationArn, "ds-name", "S3", tie)
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, err := b.ListSourcesForS3TableIntegration(integrationArn, token, 1)
		require.NoError(t, err)
		ids := make([]string, len(page))
		for i, e := range page {
			ids[i] = e.ID
		}

		return ids, next
	})
}

// TestDescribeLogStreamsOrderByLastEventTimeSortIsTotal covers the "caller
// selects the sort attribute" shape: DescribeLogStreams accepts orderBy, and
// only its default (LogStreamName, the table's own primary key) was total.
// orderBy=LastEventTime had no secondary key, and streams with no events
// share LastEventTimestamp==nil (0) by construction, so a tie needs no
// contrivance -- it is the common case for freshly created streams.
func TestDescribeLogStreamsOrderByLastEventTimeSortIsTotal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.CreateLogGroup(ctx, "/lg", "", "")
	require.NoError(t, err)

	want := make(map[string]bool, 3)
	for i := range 3 {
		name := fmt.Sprintf("stream-%03d", i)
		_, createErr := b.CreateLogStream(ctx, "/lg", name)
		require.NoError(t, createErr)
		want[name] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, listErr := b.DescribeLogStreams(ctx, "/lg", "", token, "LastEventTime", false, 1)
		require.NoError(t, listErr)
		ids := make([]string, len(page))
		for i, s := range page {
			ids[i] = s.LogStreamName
		}

		return ids, next
	})
}

func TestListScheduledQueriesSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	const tie = 1700000000000

	want := make(map[string]bool, 3)
	for i := range 3 {
		arn := fmt.Sprintf("arn:aws:logs:us-east-1:111111111111:scheduled-query:q-%03d", i)
		cloudwatchlogs.AddScheduledQueryInternal(b, cloudwatchlogs.ScheduledQuery{
			ScheduledQueryArn: arn,
			Name:              fmt.Sprintf("query-%03d", i),
			QueryString:       "fields @timestamp",
			State:             "ACTIVE",
			CreationTime:      tie,
		})
		want[arn] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, err := b.ListScheduledQueries(1, token)
		require.NoError(t, err)
		ids := make([]string, len(page))
		for i, q := range page {
			ids[i] = q.ScheduledQueryArn
		}

		return ids, next
	})
}

// TestDescribeResourcePoliciesSortIsTotal covers a tie that AWS itself
// permits: the same PolicyName can legitimately exist once per resource
// scope (PutResourcePolicy's key is policyName+resourceArn), so two
// RESOURCE-scoped policies with an identical PolicyName is not a contrived
// edge case.
func TestDescribeResourcePoliciesSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()

	want := make(map[string]bool, 3)
	for i := range 3 {
		resourceArn := fmt.Sprintf("arn:aws:logs:us-east-1:111111111111:log-group:/lg-%03d", i)
		_, err := b.PutResourcePolicy("dup-name", "{}", resourceArn, nil)
		require.NoError(t, err)
		want[resourceArn] = true
	}

	// DescribeResourcePolicies has no per-page maxResults override for the
	// RESOURCE scope path other than limit; use a small limit to force a
	// multi-page walk. The returned identity key is ResourceArn since
	// PolicyName is deliberately identical for all three.
	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next := b.DescribeResourcePolicies("RESOURCE", "", token, 1)
		ids := make([]string, len(page))
		for i, p := range page {
			ids[i] = p.ResourceArn
		}

		return ids, next
	})
}

func TestDescribeQueryDefinitionsSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()

	want := make(map[string]bool, 3)
	for range 3 {
		id, err := b.PutQueryDefinition("dup-name", "fields @timestamp", "", nil, nil)
		require.NoError(t, err)
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, err := b.DescribeQueryDefinitions("", 1, token)
		require.NoError(t, err)
		ids := make([]string, len(page))
		for i, qd := range page {
			ids[i] = qd.QueryDefinitionID
		}

		return ids, next
	})
}

// TestDescribeAccountPoliciesSortIsTotal proves gopherstack-wksweep-cwl-4:
// AccountPolicy is keyed by PolicyName+":"+PolicyType (accountPolicyKeyFn,
// store_setup.go) -- a caller can legitimately have several account
// policies sharing one PolicyName across different PolicyTypes (e.g. one
// DATA_PROTECTION_POLICY and one SUBSCRIPTION_FILTER_POLICY both named
// "default"). DescribeAccountPolicies sorted only by PolicyName, a key that
// is deliberately non-unique in that scenario, over store.Table.All()'s
// unordered map walk -- the same non-total-sort shape already fixed for
// DescribeResourcePolicies (ResourceArn tiebreak) and DescribeQueryDefinitions.
func TestDescribeAccountPoliciesSortIsTotal(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()

	policyTypes := []string{"DATA_PROTECTION_POLICY", "SUBSCRIPTION_FILTER_POLICY", "FIELD_INDEX_POLICY"}

	want := make(map[string]bool, len(policyTypes))
	for _, pt := range policyTypes {
		_, err := b.PutAccountPolicy("dup-name", pt, "{}", "", "")
		require.NoError(t, err)
		want["dup-name:"+pt] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		page, next, err := b.DescribeAccountPolicies("", "", nil, 1, token)
		require.NoError(t, err)
		ids := make([]string, len(page))
		for i, p := range page {
			ids[i] = p.PolicyName + ":" + p.PolicyType
		}

		return ids, next
	})
}
