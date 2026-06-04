package rds_test

// handler_accuracy_ops3_test.go — RDS batch-2 ops AWS-accuracy audit (go-44bi)
//
// Covers two accuracy gaps:
//  1. SubnetIds parameter format: handlers now parse SubnetIds.SubnetIdentifier.N
//     (AWS query-protocol encoding) instead of SubnetIds.member.N.
//     Both CreateDBSubnetGroup and ModifyDBSubnetGroup are affected.
//  2. DBClusterParameterGroup XML field name: Create, Describe, and Copy responses
//     now emit DBClusterParameterGroupName (not DBParameterGroupName) and wrap list
//     members in DBClusterParameterGroup elements (not DBParameterGroup).

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// ================================================================
// Gap 1: SubnetIds.SubnetIdentifier.N parameter format
// ================================================================

func TestAccOps3_CreateDBSubnetGroup_SubnetIDs_AreCaptured(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	rec := postRDSForm(t, h, url.Values{
		"Action":                       {"CreateDBSubnetGroup"},
		"Version":                      {"2014-10-31"},
		"DBSubnetGroupName":            {"sg-capture"},
		"DBSubnetGroupDescription":     {"test"},
		"SubnetIds.SubnetIdentifier.1": {"subnet-aaa111"},
		"SubnetIds.SubnetIdentifier.2": {"subnet-bbb222"},
	}.Encode())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "subnet-aaa111", "first subnet ID must appear in response")
	assert.Contains(t, body, "subnet-bbb222", "second subnet ID must appear in response")
}

func TestAccOps3_ModifyDBSubnetGroup_SubnetIDs_AreCaptured(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	postRDSForm(t, h, url.Values{
		"Action":                       {"CreateDBSubnetGroup"},
		"Version":                      {"2014-10-31"},
		"DBSubnetGroupName":            {"sg-modify"},
		"DBSubnetGroupDescription":     {"original"},
		"SubnetIds.SubnetIdentifier.1": {"subnet-orig"},
	}.Encode())

	rec := postRDSForm(t, h, url.Values{
		"Action":                       {"ModifyDBSubnetGroup"},
		"Version":                      {"2014-10-31"},
		"DBSubnetGroupName":            {"sg-modify"},
		"DBSubnetGroupDescription":     {"updated"},
		"SubnetIds.SubnetIdentifier.1": {"subnet-new1"},
		"SubnetIds.SubnetIdentifier.2": {"subnet-new2"},
	}.Encode())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "subnet-new1", "first new subnet ID must appear in response")
	assert.Contains(t, body, "subnet-new2", "second new subnet ID must appear in response")
}

func TestAccOps3_SubnetGroup_MultipleSubnets_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		subnets []string
	}{
		{name: "single", subnets: []string{"subnet-111"}},
		{name: "three", subnets: []string{"subnet-aaa", "subnet-bbb", "subnet-ccc"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

			vals := url.Values{
				"Action":                   {"CreateDBSubnetGroup"},
				"Version":                  {"2014-10-31"},
				"DBSubnetGroupName":        {"sg-" + tc.name},
				"DBSubnetGroupDescription": {"test"},
			}
			for i, id := range tc.subnets {
				key := "SubnetIds.SubnetIdentifier." + func(n int) string {
					switch n {
					case 0:
						return "1"
					case 1:
						return "2"
					default:
						return "3"
					}
				}(i)
				vals[key] = []string{id}
			}

			rec := postRDSForm(t, h, vals.Encode())
			require.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()
			for _, subnet := range tc.subnets {
				assert.Contains(t, body, subnet)
			}
		})
	}
}

// ================================================================
// Gap 2: DBClusterParameterGroup XML field names
// ================================================================

func TestAccOps3_CreateDBClusterParameterGroup_UsesClusterFieldName(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	rec := postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-create"},
		"DBParameterGroupFamily":      {"aurora-postgresql14"},
		"Description":                 {"test cluster pg"},
	}.Encode())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DBClusterParameterGroupName",
		"response must use DBClusterParameterGroupName not DBParameterGroupName")
	assert.Contains(t, body, "cpg-create",
		"group name value must appear in response")
	assert.NotContains(t, body, "<DBParameterGroupName>",
		"response must not use DBParameterGroupName element for cluster PG")
}

func TestAccOps3_DescribeDBClusterParameterGroups_UsesClusterFieldName(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-describe"},
		"DBParameterGroupFamily":      {"aurora-mysql8.0"},
		"Description":                 {"describe test"},
	}.Encode())

	rec := postRDSForm(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameterGroups"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-describe"},
	}.Encode())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DBClusterParameterGroupName",
		"describe response must use DBClusterParameterGroupName element")
	assert.Contains(t, body, "cpg-describe",
		"group name value must appear in describe response")
	assert.Contains(t, body, "DBClusterParameterGroup>",
		"list must wrap members in DBClusterParameterGroup elements")
	assert.NotContains(t, body, "<DBParameterGroupName>",
		"describe response must not use DBParameterGroupName for cluster PG")
	assert.NotContains(t, body, "<DBParameterGroup>",
		"describe response list must not use DBParameterGroup element for cluster PG")
}

func TestAccOps3_CopyDBClusterParameterGroup_UsesClusterFieldName(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-src"},
		"DBParameterGroupFamily":      {"aurora-postgresql14"},
		"Description":                 {"source"},
	}.Encode())

	rec := postRDSForm(t, h, url.Values{
		"Action":  {"CopyDBClusterParameterGroup"},
		"Version": {"2014-10-31"},
		"SourceDBClusterParameterGroupIdentifier":  {"cpg-src"},
		"TargetDBClusterParameterGroupIdentifier":  {"cpg-dst"},
		"TargetDBClusterParameterGroupDescription": {"destination"},
	}.Encode())

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DBClusterParameterGroupName",
		"copy response must use DBClusterParameterGroupName element")
	assert.Contains(t, body, "cpg-dst",
		"destination group name must appear in copy response")
	assert.NotContains(t, body, "<DBParameterGroupName>",
		"copy response must not use DBParameterGroupName for cluster PG")
}

func TestAccOps3_DBClusterPG_NonClusterPG_FieldNamesDistinct(t *testing.T) {
	t.Parallel()

	h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

	postRDSForm(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"pg-instance"},
		"DBParameterGroupFamily": {"postgres14"},
		"Description":            {"instance pg"},
	}.Encode())

	recInstance := postRDSForm(t, h, url.Values{
		"Action":               {"DescribeDBParameterGroups"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-instance"},
	}.Encode())
	require.Equal(t, http.StatusOK, recInstance.Code)
	instanceBody := recInstance.Body.String()
	assert.Contains(t, instanceBody, "<DBParameterGroupName>",
		"instance PG describe must use DBParameterGroupName")
	assert.NotContains(t, instanceBody, "DBClusterParameterGroupName",
		"instance PG describe must not use cluster field name")

	postRDSForm(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-cluster"},
		"DBParameterGroupFamily":      {"aurora-postgresql14"},
		"Description":                 {"cluster pg"},
	}.Encode())

	recCluster := postRDSForm(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameterGroups"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-cluster"},
	}.Encode())
	require.Equal(t, http.StatusOK, recCluster.Code)
	clusterBody := recCluster.Body.String()
	assert.Contains(t, clusterBody, "DBClusterParameterGroupName",
		"cluster PG describe must use DBClusterParameterGroupName")
	assert.NotContains(t, clusterBody, "<DBParameterGroupName>",
		"cluster PG describe must not use instance field name")
}
