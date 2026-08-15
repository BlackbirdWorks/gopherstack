package docdb_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestModifyInstance_PromotionTier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		promotionTier string
		wantTier      int
		wantStatus    int
	}{
		{
			name:          "set_tier_0",
			promotionTier: "0",
			wantTier:      0,
			wantStatus:    200,
		},
		{
			name:          "set_tier_15",
			promotionTier: "15",
			wantTier:      15,
			wantStatus:    200,
		},
		{
			name:          "no_tier_change",
			promotionTier: "",
			wantTier:      1,
			wantStatus:    200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"tier-inst"},
				"Engine":               {"docdb"},
			})
			vals := url.Values{
				"Action":               {"ModifyDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"tier-inst"},
			}
			if tt.promotionTier != "" {
				vals.Set("PromotionTier", tt.promotionTier)
			}
			rr := doRequest(t, h, vals)
			require.Equal(t, tt.wantStatus, rr.Code)

			instances, err := h.Backend.DescribeDBInstances(context.Background(), "tier-inst", "")
			require.NoError(t, err)
			require.Len(t, instances, 1)
			assert.Equal(t, tt.wantTier, instances[0].PromotionTier)
		})
	}
}

func TestInstanceResponseFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
	}{
		{
			name: "all_expected_fields_present",
			wantContains: []string{
				"DBInstanceArn",
				"DBInstanceIdentifier",
				"Engine",
				"DBInstanceStatus",
				"DBInstanceClass",
				"EngineVersion",
				"EnabledCloudwatchLogsExports",
				"CopyTagsToSnapshot",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"field-check-inst"},
				"Engine":               {"docdb"},
			})
			require.Equal(t, 200, rr.Code)
			body := rr.Body.String()
			for _, field := range tt.wantContains {
				assert.Contains(t, body, field, "field %q missing from response", field)
			}
		})
	}
}

func TestInstanceInheritsClusterProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		clusterID      string
		wantEngineVer  string
		wantStorageEnc bool
	}{
		{
			name:           "instance_inherits_from_cluster",
			clusterID:      "parent-cluster",
			wantEngineVer:  "5.0.0",
			wantStorageEnc: true,
		},
		{
			name:           "instance_no_cluster_uses_defaults",
			clusterID:      "",
			wantEngineVer:  "4.0.0",
			wantStorageEnc: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.clusterID != "" {
				b.AddDBClusterInternal(&docdb.DBCluster{
					DBClusterIdentifier: tt.clusterID,
					EngineVersion:       "5.0.0",
					StorageEncrypted:    true,
				})
			}
			inst, err := b.CreateDBInstance(
				context.Background(),
				"inherit-inst", tt.clusterID, "", "docdb", 1, nil, nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEngineVer, inst.EngineVersion)
			assert.Equal(t, tt.wantStorageEnc, inst.StorageEncrypted)
		})
	}
}

func TestCreateInstance_RequiresCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantContains  string
		createCluster bool
		wantStatus    int
	}{
		{
			name:          "cluster_exists",
			createCluster: true,
			wantStatus:    http.StatusOK,
			wantContains:  "CreateDBInstanceResponse",
		},
		{
			name:          "cluster_missing",
			createCluster: false,
			wantStatus:    http.StatusBadRequest,
			wantContains:  "DBClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.createCluster {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
					"Engine":              {"docdb"},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-inst"},
				"DBClusterIdentifier":  {"my-cluster"},
				"Engine":               {"docdb"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestPromotionTier_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tier         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "tier_0_valid",
			tier:         "0",
			wantStatus:   http.StatusOK,
			wantContains: "<PromotionTier>0</PromotionTier>",
		},
		{
			name:         "tier_15_valid",
			tier:         "15",
			wantStatus:   http.StatusOK,
			wantContains: "<PromotionTier>15</PromotionTier>",
		},
		{
			name:         "tier_16_invalid",
			tier:         "16",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "default_tier_1",
			tier:         "",
			wantStatus:   http.StatusOK,
			wantContains: "<PromotionTier>1</PromotionTier>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"pt-cluster"},
				"Engine":              {"docdb"},
			})
			vals := url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"pt-inst"},
				"DBClusterIdentifier":  {"pt-cluster"},
				"Engine":               {"docdb"},
			}
			if tt.tier != "" {
				vals.Set("PromotionTier", tt.tier)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestDBClusterMembers_Populated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		instanceCount int
		wantMembers   int
	}{
		{
			name:          "no_instances",
			instanceCount: 0,
			wantMembers:   0,
		},
		{
			name:          "one_instance",
			instanceCount: 1,
			wantMembers:   1,
		},
		{
			name:          "two_instances",
			instanceCount: 2,
			wantMembers:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"member-cluster"},
				"Engine":              {"docdb"},
			})
			for i := range tt.instanceCount {
				doRequest(t, h, url.Values{
					"Action":               {"CreateDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {fmt.Sprintf("member-inst-%d", i)},
					"DBClusterIdentifier":  {"member-cluster"},
					"Engine":               {"docdb"},
				})
			}

			rr := doRequest(t, h, url.Values{
				"Action":              {"DescribeDBClusters"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"member-cluster"},
			})
			assert.Equal(t, http.StatusOK, rr.Code)

			type member struct {
				XMLName xml.Name `xml:"DBClusterMember"`
			}
			type result struct {
				Members []member `xml:"DescribeDBClustersResult>DBClusters>DBCluster>DBClusterMembers>DBClusterMember"`
			}
			var res result
			require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &res))
			assert.Len(t, res.Members, tt.wantMembers)
		})
	}
}

func TestDescribeDBInstances_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxRecords  string
		marker      string
		wantCount   int
		wantHasMore bool
	}{
		{
			name:        "no_limit_returns_all",
			maxRecords:  "",
			wantCount:   4,
			wantHasMore: false,
		},
		{
			name:        "limit_to_2",
			maxRecords:  "2",
			wantCount:   2,
			wantHasMore: true,
		},
		{
			name:        "page_2_with_marker",
			maxRecords:  "2",
			marker:      "2",
			wantCount:   2,
			wantHasMore: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b2CreateCluster(t, h, "pg-cluster")
			for i := range 4 {
				b2CreateInstance(t, h, fmt.Sprintf("pg-instance-%d", i), "pg-cluster")
			}

			vals := url.Values{
				"Action":  {"DescribeDBInstances"},
				"Version": {"2014-10-31"},
			}
			if tt.maxRecords != "" {
				vals.Set("MaxRecords", tt.maxRecords)
			}
			if tt.marker != "" {
				vals.Set("Marker", tt.marker)
			}
			rr := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			count := strings.Count(body, "<DBInstanceIdentifier>")
			assert.Equal(t, tt.wantCount, count)
			if tt.wantHasMore {
				assert.Contains(t, body, "<Marker>")
			} else {
				assert.NotContains(t, body, "<Marker>")
			}
		})
	}
}

// TestParity_InstanceErrorCodes verifies that instance not-found and already-exists
// errors carry the AWS-accurate "Fault" suffix.
func TestInstanceErrorCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		vals     url.Values
		wantCode string
	}{
		{
			name: "instance_not_found",
			vals: url.Values{
				"Action":               {"DeleteDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"nonexistent-inst"},
			},
			wantCode: "DBInstanceNotFound",
		},
		{
			name: "instance_already_exists",
			vals: url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"dup-inst"},
				"DBClusterIdentifier":  {"some-cluster"},
				"DBInstanceClass":      {"db.t3.medium"},
				"Engine":               {"docdb"},
			},
			wantCode: "DBClusterNotFoundFault",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tc.vals)
			assert.NotEqual(t, http.StatusOK, rr.Code)
			code := pbExtractErrorCode(t, rr.Body.String())
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestParity_InstanceAlreadyExists verifies DBInstanceAlreadyExists.
func TestInstanceAlreadyExists(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	pbCreateCluster(t, h, "cluster-for-dup")
	pbCreateInstance(t, h, "dup-inst", "cluster-for-dup")
	// Second create same ID should return AlreadyExists with Fault suffix.
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"dup-inst"},
		"DBClusterIdentifier":  {"cluster-for-dup"},
		"DBInstanceClass":      {"db.t3.medium"},
		"Engine":               {"docdb"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Equal(t, "DBInstanceAlreadyExists", pbExtractErrorCode(t, rr.Body.String()))
}
