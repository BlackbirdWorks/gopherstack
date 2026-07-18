package docdb_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestHandler_DBInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_instance",
			vals: url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
				"DBClusterIdentifier":  {"my-cluster"},
				"DBInstanceClass":      {"db.t3.medium"},
				"Engine":               {"docdb"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-instance",
		},
		{
			name: "describe_instances_all",
			vals: url.Values{
				"Action":  {"DescribeDBInstances"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBInstancesResponse",
		},
		{
			name: "describe_instances_by_id",
			vals: url.Values{
				"Action":               {"DescribeDBInstances"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-instance",
		},
		{
			name: "modify_instance",
			vals: url.Values{
				"Action":               {"ModifyDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
				"DBInstanceClass":      {"db.r5.large"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "db.r5.large",
		},
		{
			name: "reboot_instance",
			vals: url.Values{
				"Action":               {"RebootDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "RebootDBInstanceResponse",
		},
		{
			name: "delete_instance",
			vals: url.Values{
				"Action":               {"DeleteDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"my-instance"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBInstanceResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
				"Engine":              {"docdb"},
			})
			if tt.name != "create_instance" {
				doRequest(t, h, url.Values{
					"Action":               {"CreateDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {"my-instance"},
					"DBClusterIdentifier":  {"my-cluster"},
					"Engine":               {"docdb"},
				})
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestSortedDescribeInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []string
		want []string
	}{
		{
			name: "sorted_order",
			ids:  []string{"i-z", "i-a", "i-m"},
			want: []string{"i-a", "i-m", "i-z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, id := range tt.ids {
				b.AddDBInstanceInternal(&docdb.DBInstance{DBInstanceIdentifier: id})
			}

			got, err := b.DescribeDBInstances(context.Background(), "", "")
			require.NoError(t, err)

			gotIDs := make([]string, len(got))
			for i, inst := range got {
				gotIDs[i] = inst.DBInstanceIdentifier
			}

			assert.Equal(t, tt.want, gotIDs)
		})
	}
}

func TestInstanceARNInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "arn_present", id: "my-instance"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":               []string{"CreateDBInstance"},
				"Version":              []string{"2014-10-31"},
				"DBInstanceIdentifier": []string{tt.id},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			body := resp.Body.String()
			assert.Contains(t, body, "DBInstanceArn")
			assert.Contains(t, body, "arn:aws:rds:")
		})
	}
}

func TestTagsOnCreate_Instance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		tagKey  string
		tagVal  string
		wantLen int
	}{
		{
			name:    "tags_stored",
			id:      "tagged-instance",
			tagKey:  "env",
			tagVal:  "prod",
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":               []string{"CreateDBInstance"},
				"Version":              []string{"2014-10-31"},
				"DBInstanceIdentifier": []string{tt.id},
				"Tags.Tag.1.Key":       []string{tt.tagKey},
				"Tags.Tag.1.Value":     []string{tt.tagVal},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			instances, err := h.Backend.DescribeDBInstances(context.Background(), tt.id, "")
			require.NoError(t, err)
			require.Len(t, instances, 1)

			assert.Len(t, instances[0].Tags, tt.wantLen)
			assert.Equal(t, tt.tagVal, instances[0].Tags[tt.tagKey])
		})
	}
}

func TestDescribeDBInstancesByCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *docdb.Handler)
		vals       url.Values
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "filter_by_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				for _, cid := range []string{"cluster-a", "cluster-b"} {
					doRequest(t, h, url.Values{
						"Action":              {"CreateDBCluster"},
						"Version":             {"2014-10-31"},
						"DBClusterIdentifier": {cid},
						"Engine":              {"docdb"},
					})
				}
				for _, id := range []string{"inst-a1", "inst-a2", "inst-b1"} {
					clusterID := "cluster-a"
					if id == "inst-b1" {
						clusterID = "cluster-b"
					}
					doRequest(t, h, url.Values{
						"Action":               {"CreateDBInstance"},
						"Version":              {"2014-10-31"},
						"DBInstanceIdentifier": {id},
						"DBClusterIdentifier":  {clusterID},
					})
				}
			},
			vals: url.Values{
				"Action":              {"DescribeDBInstances"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"cluster-a"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			body := rr.Body.String()

			type instance struct {
				XMLName xml.Name `xml:"DBInstance"`
			}
			type result struct {
				Instances []instance `xml:"DescribeDBInstancesResult>DBInstances>DBInstance"`
			}
			var res result
			_ = xml.Unmarshal([]byte(body), &res)
			assert.Len(t, res.Instances, tt.wantCount)
		})
	}
}

func TestCreateInstanceInheritsCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
	}{
		{
			name:         "instance_inherits_storage_encrypted",
			wantContains: "<StorageEncrypted>true</StorageEncrypted>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"enc-cluster"},
				"StorageEncrypted":    {"true"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"enc-inst"},
				"DBClusterIdentifier":  {"enc-cluster"},
				"DBInstanceClass":      {"db.r5.large"},
				"Engine":               {"docdb"},
			})
			assert.Equal(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestCreateInstance_CACertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		caCertID     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "ca_cert_set",
			caCertID:     "rds-ca-rsa2048-g1",
			wantContains: "rds-ca-rsa2048-g1",
			wantStatus:   200,
		},
		{
			name:         "no_ca_cert",
			caCertID:     "",
			wantContains: "CreateDBInstanceResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"ca-cert-inst"},
				"Engine":               {"docdb"},
			}
			if tt.caCertID != "" {
				vals.Set("CACertificateIdentifier", tt.caCertID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestCreateInstance_CopyTagsToSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		copyTagsToSnapshot string
		wantCopyTags       bool
		wantStatus         int
	}{
		{
			name:               "copy_tags_enabled",
			copyTagsToSnapshot: "true",
			wantCopyTags:       true,
			wantStatus:         200,
		},
		{
			name:               "copy_tags_disabled",
			copyTagsToSnapshot: "false",
			wantCopyTags:       false,
			wantStatus:         200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"copy-tags-inst"},
				"CopyTagsToSnapshot":   {tt.copyTagsToSnapshot},
				"Engine":               {"docdb"},
			})
			require.Equal(t, tt.wantStatus, rr.Code)

			instances, err := h.Backend.DescribeDBInstances(context.Background(), "copy-tags-inst", "")
			require.NoError(t, err)
			require.Len(t, instances, 1)
			assert.Equal(t, tt.wantCopyTags, instances[0].CopyTagsToSnapshot)
		})
	}
}

func TestModifyInstance_CACertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		caCertID     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "update_ca_cert",
			caCertID:     "rds-ca-rsa2048-g1",
			wantContains: "rds-ca-rsa2048-g1",
			wantStatus:   200,
		},
		{
			name:         "no_ca_cert_change",
			caCertID:     "",
			wantContains: "ModifyDBInstanceResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"modify-ca-inst"},
				"Engine":               {"docdb"},
			})
			vals := url.Values{
				"Action":               {"ModifyDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"modify-ca-inst"},
			}
			if tt.caCertID != "" {
				vals.Set("CACertificateIdentifier", tt.caCertID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}
