package redshift_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestRealClient_DescribePagination proves the eighteen redshift Describe
// operations the reqfielddiff tier-1 sweep found silently dropping
// MaxRecords/Marker (gopherstack-xhu2t) now enforce the documented
// MaxRecords range (20-100) and, where the backing data can exceed a page,
// actually truncate and hand back a usable continuation Marker. It also
// proves DescribeOrderableClusterOptions' ClusterVersion filter and
// DescribeEvents' Duration filter, dropped by the same sweep.
func TestRealClient_DescribePagination(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testDescribeMaxRecordsValidationRealClient, "max_records_validation"},
		{testDescribeHsmClientCertificatesTruncationRealClient, "hsm_client_certificates_truncation"},
		{testDescribeClusterParameterGroupsTruncationRealClient, "cluster_parameter_groups_truncation"},
		{testDescribeOrderableClusterOptionsFilterRealClient, "orderable_cluster_options_cluster_version_filter"},
		{testDescribeEventsDurationFilterRealClient, "describe_events_duration_filter"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func newPaginationBackendAndClient(t *testing.T) (*redshift.InMemoryBackend, *redshiftsdk.Client) {
	t.Helper()

	backend := redshift.NewInMemoryBackend("000000000000", rtTestRegion)
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))

	return backend, client
}

// testDescribeMaxRecordsValidationRealClient proves each of the eighteen
// ops now actually reads and enforces MaxRecords (Constraints: minimum 20,
// maximum 100) by rejecting an out-of-range value -- the observable proof
// available uniformly across all eighteen regardless of how much backing
// data each one has (several are small fixed catalogs too short to prove
// truncation directly).
func testDescribeMaxRecordsValidationRealClient(t *testing.T) {
	t.Helper()

	_, client := newPaginationBackendAndClient(t)
	ctx := t.Context()

	badMaxRecords := aws.Int32(5)

	opCases := []struct {
		call func(ctx context.Context) error
		name string
	}{
		{func(ctx context.Context) error {
			_, err := client.DescribeClusterParameterGroups(
				ctx,
				&redshiftsdk.DescribeClusterParameterGroupsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeClusterParameterGroups"},
		{func(ctx context.Context) error {
			_, err := client.DescribeClusterParameters(ctx, &redshiftsdk.DescribeClusterParametersInput{
				ParameterGroupName: aws.String("default.redshift-1.0"), MaxRecords: badMaxRecords,
			})

			return err
		}, "DescribeClusterParameters"},
		{func(ctx context.Context) error {
			_, err := client.DescribeClusterVersions(
				ctx,
				&redshiftsdk.DescribeClusterVersionsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeClusterVersions"},
		{func(ctx context.Context) error {
			_, err := client.DescribeDefaultClusterParameters(ctx, &redshiftsdk.DescribeDefaultClusterParametersInput{
				ParameterGroupFamily: aws.String("redshift-1.0"), MaxRecords: badMaxRecords,
			})

			return err
		}, "DescribeDefaultClusterParameters"},
		{func(ctx context.Context) error {
			_, err := client.DescribeEventSubscriptions(
				ctx,
				&redshiftsdk.DescribeEventSubscriptionsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeEventSubscriptions"},
		{func(ctx context.Context) error {
			_, err := client.DescribeEvents(ctx, &redshiftsdk.DescribeEventsInput{MaxRecords: badMaxRecords})

			return err
		}, "DescribeEvents"},
		{func(ctx context.Context) error {
			_, err := client.DescribeHsmClientCertificates(
				ctx,
				&redshiftsdk.DescribeHsmClientCertificatesInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeHsmClientCertificates"},
		{func(ctx context.Context) error {
			_, err := client.DescribeHsmConfigurations(
				ctx,
				&redshiftsdk.DescribeHsmConfigurationsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeHsmConfigurations"},
		{func(ctx context.Context) error {
			_, err := client.DescribeInboundIntegrations(
				ctx,
				&redshiftsdk.DescribeInboundIntegrationsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeInboundIntegrations"},
		{func(ctx context.Context) error {
			_, err := client.DescribeIntegrations(
				ctx,
				&redshiftsdk.DescribeIntegrationsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeIntegrations"},
		{func(ctx context.Context) error {
			_, err := client.DescribeNodeConfigurationOptions(ctx, &redshiftsdk.DescribeNodeConfigurationOptionsInput{
				ActionType: types.ActionTypeRecommendNodeConfig, MaxRecords: badMaxRecords,
			})

			return err
		}, "DescribeNodeConfigurationOptions"},
		{func(ctx context.Context) error {
			_, err := client.DescribeOrderableClusterOptions(
				ctx,
				&redshiftsdk.DescribeOrderableClusterOptionsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeOrderableClusterOptions"},
		{func(ctx context.Context) error {
			_, err := client.DescribeReservedNodeOfferings(
				ctx,
				&redshiftsdk.DescribeReservedNodeOfferingsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeReservedNodeOfferings"},
		{func(ctx context.Context) error {
			_, err := client.DescribeReservedNodes(
				ctx,
				&redshiftsdk.DescribeReservedNodesInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeReservedNodes"},
		{func(ctx context.Context) error {
			_, err := client.DescribeScheduledActions(
				ctx,
				&redshiftsdk.DescribeScheduledActionsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeScheduledActions"},
		{func(ctx context.Context) error {
			_, err := client.DescribeSnapshotCopyGrants(
				ctx,
				&redshiftsdk.DescribeSnapshotCopyGrantsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeSnapshotCopyGrants"},
		{func(ctx context.Context) error {
			_, err := client.DescribeUsageLimits(ctx, &redshiftsdk.DescribeUsageLimitsInput{MaxRecords: badMaxRecords})

			return err
		}, "DescribeUsageLimits"},
		{func(ctx context.Context) error {
			_, err := client.DescribeClusterDbRevisions(
				ctx,
				&redshiftsdk.DescribeClusterDbRevisionsInput{MaxRecords: badMaxRecords},
			)

			return err
		}, "DescribeClusterDbRevisions"},
	}

	for _, oc := range opCases {
		t.Run(oc.name, func(t *testing.T) {
			t.Parallel()

			err := oc.call(ctx)
			require.Error(t, err)
		})
	}
}

// testDescribeHsmClientCertificatesTruncationRealClient proves real
// truncation and Marker-based continuation, not just range validation: with
// 25 certificates and MaxRecords=20, the first page returns exactly 20 with
// a non-empty Marker, and following that Marker returns the remaining 5
// with no further Marker.
func testDescribeHsmClientCertificatesTruncationRealClient(t *testing.T) {
	t.Helper()

	_, client := newPaginationBackendAndClient(t)
	ctx := t.Context()

	const total = 25

	for i := range total {
		_, err := client.CreateHsmClientCertificate(ctx, &redshiftsdk.CreateHsmClientCertificateInput{
			HsmClientCertificateIdentifier: aws.String(fmt.Sprintf("hsm-cert-%02d", i)),
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribeHsmClientCertificates(ctx, &redshiftsdk.DescribeHsmClientCertificatesInput{
		MaxRecords: aws.Int32(20),
	})
	require.NoError(t, err)
	assert.Len(t, page1.HsmClientCertificates, 20)
	require.NotEmpty(t, aws.ToString(page1.Marker))

	page2, err := client.DescribeHsmClientCertificates(ctx, &redshiftsdk.DescribeHsmClientCertificatesInput{
		MaxRecords: aws.Int32(20),
		Marker:     page1.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, page2.HsmClientCertificates, total-20)
	assert.Empty(t, aws.ToString(page2.Marker))
}

// testDescribeClusterParameterGroupsTruncationRealClient is a second real
// truncation proof against a different Describe op and a different
// underlying store shape (a store.Table-backed resource, not a slice
// built by CreateHsmClientCertificate's simpler path).
func testDescribeClusterParameterGroupsTruncationRealClient(t *testing.T) {
	t.Helper()

	_, client := newPaginationBackendAndClient(t)
	ctx := t.Context()

	const total = 22

	for i := range total {
		_, err := client.CreateClusterParameterGroup(ctx, &redshiftsdk.CreateClusterParameterGroupInput{
			ParameterGroupName:   aws.String(fmt.Sprintf("pg-trunc-%02d", i)),
			ParameterGroupFamily: aws.String("redshift-1.0"),
			Description:          aws.String("truncation test"),
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribeClusterParameterGroups(ctx, &redshiftsdk.DescribeClusterParameterGroupsInput{
		MaxRecords: aws.Int32(20),
	})
	require.NoError(t, err)
	assert.Len(t, page1.ParameterGroups, 20)
	require.NotEmpty(t, aws.ToString(page1.Marker))

	page2, err := client.DescribeClusterParameterGroups(ctx, &redshiftsdk.DescribeClusterParameterGroupsInput{
		MaxRecords: aws.Int32(20),
		Marker:     page1.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, page2.ParameterGroups, total-20)
	assert.Empty(t, aws.ToString(page2.Marker))
}

// testDescribeOrderableClusterOptionsFilterRealClient proves the documented
// ClusterVersion filter (previously dropped entirely, "no strong signal"
// finding notwithstanding -- the field was verifiably unread).
func testDescribeOrderableClusterOptionsFilterRealClient(t *testing.T) {
	t.Helper()

	_, client := newPaginationBackendAndClient(t)
	ctx := t.Context()

	matching, err := client.DescribeOrderableClusterOptions(ctx, &redshiftsdk.DescribeOrderableClusterOptionsInput{
		ClusterVersion: aws.String("1.0"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, matching.OrderableClusterOptions)

	nonMatching, err := client.DescribeOrderableClusterOptions(ctx, &redshiftsdk.DescribeOrderableClusterOptionsInput{
		ClusterVersion: aws.String("9.9"),
	})
	require.NoError(t, err)
	assert.Empty(t, nonMatching.OrderableClusterOptions)
}

// testDescribeEventsDurationFilterRealClient proves the documented Duration
// filter (minutes prior to now, default 60) via AddEventInternal-seeded
// events -- production code never generates event history itself, so this
// is the only way to exercise DescribeEvents against real records.
func testDescribeEventsDurationFilterRealClient(t *testing.T) {
	t.Helper()

	backend, client := newPaginationBackendAndClient(t)
	ctx := t.Context()

	backend.AddEventInternal(&redshift.Event{
		EventID:          "evt-recent",
		Date:             time.Now().Add(-10 * time.Minute),
		SourceIdentifier: "src-1",
		SourceType:       "cluster",
		Message:          "recent event",
	})
	backend.AddEventInternal(&redshift.Event{
		EventID:          "evt-old",
		Date:             time.Now().Add(-2 * time.Hour),
		SourceIdentifier: "src-1",
		SourceType:       "cluster",
		Message:          "old event",
	})

	defaultWindow, err := client.DescribeEvents(ctx, &redshiftsdk.DescribeEventsInput{})
	require.NoError(t, err)

	defaultIDs := make([]string, 0, len(defaultWindow.Events))
	for _, e := range defaultWindow.Events {
		defaultIDs = append(defaultIDs, aws.ToString(e.EventId))
	}

	assert.Contains(t, defaultIDs, "evt-recent")
	assert.NotContains(t, defaultIDs, "evt-old")

	wideWindow, err := client.DescribeEvents(ctx, &redshiftsdk.DescribeEventsInput{
		Duration: aws.Int32(200),
	})
	require.NoError(t, err)

	wideIDs := make([]string, 0, len(wideWindow.Events))
	for _, e := range wideWindow.Events {
		wideIDs = append(wideIDs, aws.ToString(e.EventId))
	}

	assert.Contains(t, wideIDs, "evt-recent")
	assert.Contains(t, wideIDs, "evt-old")
}
