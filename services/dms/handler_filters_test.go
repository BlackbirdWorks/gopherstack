package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeCertificatesFilterNarrows proves the Filters field genuinely
// narrows a multi-item result set, not merely parses without effect.
func TestDescribeCertificatesFilterNarrows(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	rec1 := doDMS(t, h, "ImportCertificate", map[string]any{
		"CertificateIdentifier": "cert-a",
		"CertificatePem":        "pem-a",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doDMS(t, h, "ImportCertificate", map[string]any{
		"CertificateIdentifier": "cert-b",
		"CertificatePem":        "pem-b",
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	certB := parseJSON(t, rec2)["Certificate"].(map[string]any)
	certBArn := certB["CertificateArn"].(string)

	unfilteredRec := doDMS(t, h, "DescribeCertificates", map[string]any{})
	require.Equal(t, http.StatusOK, unfilteredRec.Code)
	require.Len(t, parseJSON(t, unfilteredRec)["Certificates"].([]any), 2)

	tests := []struct {
		name    string
		filters []map[string]any
		want    int
	}{
		{
			name:    "certificate_id_narrows_to_one",
			filters: []map[string]any{{"Name": "certificate-id", "Values": []string{"cert-b"}}},
			want:    1,
		},
		{
			name:    "certificate_arn_narrows_to_one",
			filters: []map[string]any{{"Name": "certificate-arn", "Values": []string{certBArn}}},
			want:    1,
		},
		{
			name:    "no_match_narrows_to_zero",
			filters: []map[string]any{{"Name": "certificate-id", "Values": []string{"nonexistent"}}},
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doDMS(t, h, "DescribeCertificates", map[string]any{"Filters": tt.filters})
			require.Equal(t, http.StatusOK, rec.Code)
			certs := parseJSON(t, rec)["Certificates"].([]any)
			assert.Len(t, certs, tt.want)
		})
	}
}

func TestDescribeEventCategoriesFilterNarrows(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	allRec := doDMS(t, h, "DescribeEventCategories", map[string]any{})
	require.Equal(t, http.StatusOK, allRec.Code)
	require.Len(t, parseJSON(t, allRec)["EventCategoryGroupList"].([]any), 2)

	filteredRec := doDMS(t, h, "DescribeEventCategories", map[string]any{
		"Filters": []map[string]any{
			{"Name": "source-type", "Values": []string{"replication-task"}},
		},
	})
	require.Equal(t, http.StatusOK, filteredRec.Code)
	groups := parseJSON(t, filteredRec)["EventCategoryGroupList"].([]any)
	require.Len(t, groups, 1)
	assert.Equal(t, "replication-task", groups[0].(map[string]any)["SourceType"])
}

func TestDescribeEventSubscriptionsFilterNarrows(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventSubscriptionInternal("sub-a", "arn:aws:sns:us-east-1:123:topic-a")
	h.Backend.AddEventSubscriptionInternal("sub-b", "arn:aws:sns:us-east-1:123:topic-b")

	allRec := doDMS(t, h, "DescribeEventSubscriptions", map[string]any{})
	require.Equal(t, http.StatusOK, allRec.Code)
	require.Len(t, parseJSON(t, allRec)["EventSubscriptionsList"].([]any), 2)

	filteredRec := doDMS(t, h, "DescribeEventSubscriptions", map[string]any{
		"Filters": []map[string]any{
			{"Name": "event-subscription-id", "Values": []string{"sub-b"}},
		},
	})
	require.Equal(t, http.StatusOK, filteredRec.Code)
	subs := parseJSON(t, filteredRec)["EventSubscriptionsList"].([]any)
	require.Len(t, subs, 1)
	assert.Equal(t, "sub-b", subs[0].(map[string]any)["CustSubscriptionId"])
}

func TestDescribeEventsFilterNarrows(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	ep1Rec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "evt-ep-1",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, ep1Rec.Code)
	ep1Arn := parseJSON(t, ep1Rec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	ep2Rec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "evt-ep-2",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, ep2Rec.Code)

	allRec := doDMS(t, h, "DescribeEvents", map[string]any{})
	require.Equal(t, http.StatusOK, allRec.Code)
	require.Len(t, parseJSON(t, allRec)["Events"].([]any), 2)

	filteredRec := doDMS(t, h, "DescribeEvents", map[string]any{
		"Filters": []map[string]any{
			{"Name": "replication-instance-id", "Values": []string{ep1Arn}},
		},
	})
	require.Equal(t, http.StatusOK, filteredRec.Code)
	events := parseJSON(t, filteredRec)["Events"].([]any)
	require.Len(t, events, 1)
	assert.Equal(t, ep1Arn, events[0].(map[string]any)["SourceIdentifier"])
}

func TestDescribeDataProvidersFilterNarrows(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataProviderInternal("dp-a", "mysql")
	h.Backend.AddDataProviderInternal("dp-b", "postgres")

	allRec := doDMS(t, h, "DescribeDataProviders", map[string]any{})
	require.Equal(t, http.StatusOK, allRec.Code)
	require.Len(t, parseJSON(t, allRec)["DataProviders"].([]any), 2)

	filteredRec := doDMS(t, h, "DescribeDataProviders", map[string]any{
		"Filters": []map[string]any{
			{"Name": "data-provider-identifier", "Values": []string{"dp-b"}},
		},
	})
	require.Equal(t, http.StatusOK, filteredRec.Code)
	dps := parseJSON(t, filteredRec)["DataProviders"].([]any)
	require.Len(t, dps, 1)
	assert.Equal(t, "dp-b", dps[0].(map[string]any)["DataProviderName"])
}

func TestDescribeDataMigrationsFilterNarrows(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("dm-a", "full-load")
	h.Backend.AddDataMigrationInternal("dm-b", "cdc")

	allRec := doDMS(t, h, "DescribeDataMigrations", map[string]any{})
	require.Equal(t, http.StatusOK, allRec.Code)
	require.Len(t, parseJSON(t, allRec)["DataMigrations"].([]any), 2)

	filteredRec := doDMS(t, h, "DescribeDataMigrations", map[string]any{
		"Filters": []map[string]any{
			{"Name": "data-migration-identifier", "Values": []string{"dm-b"}},
		},
	})
	require.Equal(t, http.StatusOK, filteredRec.Code)
	dms := parseJSON(t, filteredRec)["DataMigrations"].([]any)
	require.Len(t, dms, 1)
	assert.Equal(t, "dm-b", dms[0].(map[string]any)["DataMigrationName"])
}

// TestMetadataModelDescribeFiltersNarrow covers the seven schema-conversion
// Describe* operations that share the request-id/status Filters shape.
func TestMetadataModelDescribeFiltersNarrow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startBody      map[string]any
		startAction    string
		describeAction string
		name           string
	}{
		{
			name:           "extension_pack_associations",
			startAction:    "StartExtensionPackAssociation",
			describeAction: "DescribeExtensionPackAssociations",
			startBody:      map[string]any{"MigrationProjectIdentifier": "proj-mm"},
		},
		{
			name:           "assessments",
			startAction:    "StartMetadataModelAssessment",
			describeAction: "DescribeMetadataModelAssessments",
			startBody:      map[string]any{"MigrationProjectIdentifier": "proj-mm", "SelectionRules": "{}"},
		},
		{
			name:           "conversions",
			startAction:    "StartMetadataModelConversion",
			describeAction: "DescribeMetadataModelConversions",
			startBody:      map[string]any{"MigrationProjectIdentifier": "proj-mm", "SelectionRules": "{}"},
		},
		{
			name:        "creations",
			startAction: "StartMetadataModelCreation",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj-mm", "MetadataModelName": "m", "SelectionRules": "{}",
			},
			describeAction: "DescribeMetadataModelCreations",
		},
		{
			name:        "exports_as_script",
			startAction: "StartMetadataModelExportAsScript",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj-mm", "Origin": "SOURCE", "SelectionRules": "{}",
			},
			describeAction: "DescribeMetadataModelExportsAsScript",
		},
		{
			name:           "exports_to_target",
			startAction:    "StartMetadataModelExportToTarget",
			describeAction: "DescribeMetadataModelExportsToTarget",
			startBody:      map[string]any{"MigrationProjectIdentifier": "proj-mm", "SelectionRules": "{}"},
		},
		{
			name:        "imports",
			startAction: "StartMetadataModelImport",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj-mm", "Origin": "SOURCE", "SelectionRules": "{}",
			},
			describeAction: "DescribeMetadataModelImports",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			start1 := doDMS(t, h, tt.startAction, tt.startBody)
			require.Equal(t, http.StatusOK, start1.Code)
			reqID1 := parseJSON(t, start1)["RequestIdentifier"].(string)

			start2 := doDMS(t, h, tt.startAction, tt.startBody)
			require.Equal(t, http.StatusOK, start2.Code)

			describeBody := map[string]any{"MigrationProjectIdentifier": "proj-mm"}

			allRec := doDMS(t, h, tt.describeAction, describeBody)
			require.Equal(t, http.StatusOK, allRec.Code)
			require.Len(t, parseJSON(t, allRec)["Requests"].([]any), 2)

			byIDBody := map[string]any{
				"MigrationProjectIdentifier": "proj-mm",
				"Filters": []map[string]any{
					{"Name": "request-id", "Values": []string{reqID1}},
				},
			}
			byIDRec := doDMS(t, h, tt.describeAction, byIDBody)
			require.Equal(t, http.StatusOK, byIDRec.Code)
			byID := parseJSON(t, byIDRec)["Requests"].([]any)
			require.Len(t, byID, 1)
			assert.Equal(t, reqID1, byID[0].(map[string]any)["RequestIdentifier"])

			byStatusBody := map[string]any{
				"MigrationProjectIdentifier": "proj-mm",
				"Filters": []map[string]any{
					{"Name": "status", "Values": []string{"FAILED"}},
				},
			}
			byStatusRec := doDMS(t, h, tt.describeAction, byStatusBody)
			require.Equal(t, http.StatusOK, byStatusRec.Code)
			assert.Empty(t, parseJSON(t, byStatusRec)["Requests"].([]any))
		})
	}
}

// TestDescribeReplicationTableStatisticsFiltersAccepted documents that
// Filters is accepted on the wire but has no effect: ReplicationConfig
// carries no TableMappings state in this emulation, so
// ReplicationTableStatistics is always empty regardless of any filter.
func TestDescribeReplicationTableStatisticsFiltersAccepted(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "rts-src",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "rts-dst",
		"EndpointType":       "target",
		"EngineName":         "s3",
	})
	require.Equal(t, http.StatusOK, dstRec.Code)
	dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	cfgRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
		"ReplicationConfigIdentifier": "rts-cfg",
		"ReplicationType":             "full-load",
		"SourceEndpointArn":           srcArn,
		"TargetEndpointArn":           dstArn,
	})
	require.Equal(t, http.StatusOK, cfgRec.Code)
	cfgArn := parseJSON(t, cfgRec)["ReplicationConfig"].(map[string]any)["ReplicationConfigArn"].(string)

	rec := doDMS(t, h, "DescribeReplicationTableStatistics", map[string]any{
		"ReplicationConfigArn": cfgArn,
		"Filters": []map[string]any{
			{"Name": "schema-name", "Values": []string{"public"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, parseJSON(t, rec)["ReplicationTableStatistics"].([]any))
}
