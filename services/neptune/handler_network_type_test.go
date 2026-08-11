package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DBCluster_NetworkType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createVals   url.Values
		modifyVals   url.Values
		wantCreate   string
		wantDescribe string
	}{
		{
			name: "network_type_supplied_on_create_echoed_on_describe",
			createVals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"nt-cluster"},
				"NetworkType":         {"DUAL"},
			},
			wantCreate:   "<NetworkType>DUAL</NetworkType>",
			wantDescribe: "<NetworkType>DUAL</NetworkType>",
		},
		{
			name: "network_type_defaults_to_ipv4_when_unset_on_create",
			createVals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"nt-cluster"},
			},
			wantCreate:   "<NetworkType>IPV4</NetworkType>",
			wantDescribe: "<NetworkType>IPV4</NetworkType>",
		},
		{
			name: "network_type_modified_and_reechoed",
			createVals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"nt-cluster"},
				"NetworkType":         {"IPV4"},
			},
			modifyVals: url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"nt-cluster"},
				"NetworkType":         {"DUAL"},
			},
			wantCreate:   "<NetworkType>IPV4</NetworkType>",
			wantDescribe: "<NetworkType>DUAL</NetworkType>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			createRR := doRequest(t, h, tt.createVals)
			require.Equal(t, http.StatusOK, createRR.Code)
			assert.Contains(t, createRR.Body.String(), tt.wantCreate)

			if tt.modifyVals != nil {
				modifyRR := doRequest(t, h, tt.modifyVals)
				require.Equal(t, http.StatusOK, modifyRR.Code)
			}

			describeRR := doRequest(t, h, url.Values{
				"Action":              {"DescribeDBClusters"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"nt-cluster"},
			})
			require.Equal(t, http.StatusOK, describeRR.Code)
			assert.Contains(t, describeRR.Body.String(), tt.wantDescribe)
		})
	}
}

func TestHandler_DBInstance_NetworkType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clusterVals  url.Values
		wantContains string
	}{
		{
			name: "instance_inherits_explicit_cluster_network_type",
			clusterVals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"nt-inst-cluster"},
				"NetworkType":         {"DUAL"},
			},
			wantContains: "<NetworkType>DUAL</NetworkType>",
		},
		{
			name: "instance_inherits_default_cluster_network_type",
			clusterVals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"nt-inst-cluster"},
			},
			wantContains: "<NetworkType>IPV4</NetworkType>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			clusterRR := doRequest(t, h, tt.clusterVals)
			require.Equal(t, http.StatusOK, clusterRR.Code)

			instanceVals := url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"nt-instance"},
				"DBClusterIdentifier":  {"nt-inst-cluster"},
				"DBInstanceClass":      {"db.r5.large"},
			}
			createRR := doRequest(t, h, instanceVals)
			require.Equal(t, http.StatusOK, createRR.Code)
			assert.Contains(t, createRR.Body.String(), tt.wantContains)

			describeRR := doRequest(t, h, url.Values{
				"Action":               {"DescribeDBInstances"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"nt-instance"},
			})
			require.Equal(t, http.StatusOK, describeRR.Code)
			assert.Contains(t, describeRR.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_SupportedNetworkTypes_AbsentFromWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals url.Values
		name string
	}{
		{
			name: "subnet_group_supported_network_types_absent",
			vals: url.Values{
				"Action":             {"CreateDBSubnetGroup"},
				"Version":            {"2014-10-31"},
				"DBSubnetGroupName":  {"nt-subgrp"},
				"SubnetIds.member.1": {"subnet-abc123"},
			},
		},
		{
			name: "orderable_options_supported_network_types_absent",
			vals: url.Values{
				"Action":  {"DescribeOrderableDBInstanceOptions"},
				"Version": {"2014-10-31"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			require.Equal(t, http.StatusOK, rr.Code)
			assert.NotContains(t, rr.Body.String(), "SupportedNetworkTypes")
		})
	}
}

func TestPersistenceRoundTrip_NetworkType(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := backend.CreateDBCluster(
		t.Context(), "nt-persist-cluster", "", 0,
		neptune.DBClusterCreateOptions{NetworkType: "DUAL"},
	)
	require.NoError(t, err)

	data := backend.Snapshot(t.Context())
	require.NotEmpty(t, data)

	restored := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), data))

	clusters, err := restored.DescribeDBClusters(t.Context(), "nt-persist-cluster", neptune.DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "DUAL", clusters[0].NetworkType)
}
