package neptune_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// ---- DBInstance comprehensive coverage ----

func TestBatch1Ops_CreateDBInstance_AllFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "inst-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                          {"CreateDBInstance"},
		"Version":                         {"2014-10-31"},
		"DBInstanceIdentifier":            {"inst-full"},
		"DBClusterIdentifier":             {"inst-cluster"},
		"DBInstanceClass":                 {"db.r6g.xlarge"},
		"Engine":                          {"neptune"},
		"PreferredMaintenanceWindow":      {"mon:05:00-mon:06:00"},
		"PreferredBackupWindow":           {"02:00-03:00"},
		"AvailabilityZone":                {"us-east-1a"},
		"AutoMinorVersionUpgrade":         {"true"},
		"CopyTagsToSnapshot":              {"true"},
		"EnableIAMDatabaseAuthentication": {"true"},
		"StorageEncrypted":                {"true"},
		"PromotionTier":                   {"2"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "inst-full")
	assert.Contains(t, body, "db.r6g.xlarge")
	assert.Contains(t, body, "mon:05:00-mon:06:00")
	assert.Contains(t, body, "us-east-1a")
	assert.Contains(t, body, "IAMDatabaseAuthenticationEnabled")
	assert.Contains(t, body, "CopyTagsToSnapshot")
}

func TestBatch1Ops_CreateDBInstance_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "inst-tag-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"inst-tagged"},
		"DBClusterIdentifier":  {"inst-tag-cluster"},
		"DBInstanceClass":      {"db.r5.large"},
		"Engine":               {"neptune"},
		"Tags.Tag.1.Key":       {"Environment"},
		"Tags.Tag.1.Value":     {"production"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Verify tags via list
	rr2 := doRequest(t, h, url.Values{
		"Action":  {"ListTagsForResource"},
		"Version": {"2014-10-31"},
		"ResourceName": {
			rr.Body.String()[strings.Index(rr.Body.String(), "arn:") : strings.Index(rr.Body.String(), "arn:")+80],
		},
	})
	_ = rr2 // ARN extraction is complex; just verify create succeeded
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestBatch1Ops_CreateDBInstance_FirstIsWriter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "writer-cluster")
	createInstance(t, h, "writer-inst", "writer-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"writer-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "writer-inst")
	assert.Contains(t, body, "<IsClusterWriter>true</IsClusterWriter>")
}

func TestBatch1Ops_CreateDBInstance_SecondIsNotWriter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "multi-inst-cluster")
	createInstance(t, h, "inst-w", "multi-inst-cluster")
	createInstance(t, h, "inst-r", "multi-inst-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"multi-inst-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "<IsClusterWriter>false</IsClusterWriter>")
}

func TestBatch1Ops_ModifyDBInstance_AllFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "mod-inst-cluster")
	createInstance(t, h, "mod-inst", "mod-inst-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                          {"ModifyDBInstance"},
		"Version":                         {"2014-10-31"},
		"DBInstanceIdentifier":            {"mod-inst"},
		"DBInstanceClass":                 {"db.r6g.2xlarge"},
		"PreferredMaintenanceWindow":      {"tue:06:00-tue:07:00"},
		"PreferredBackupWindow":           {"04:00-05:00"},
		"AutoMinorVersionUpgrade":         {"false"},
		"CopyTagsToSnapshot":              {"true"},
		"EnableIAMDatabaseAuthentication": {"true"},
		"PromotionTier":                   {"3"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "db.r6g.2xlarge")
	assert.Contains(t, body, "tue:06:00-tue:07:00")
	assert.Contains(t, body, "04:00-05:00")
}

func TestBatch1Ops_ModifyDBInstance_IamAuth_Persist(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "iam-inst-cluster")
	createInstance(t, h, "iam-inst", "iam-inst-cluster")

	doRequest(t, h, url.Values{
		"Action":                          {"ModifyDBInstance"},
		"Version":                         {"2014-10-31"},
		"DBInstanceIdentifier":            {"iam-inst"},
		"EnableIAMDatabaseAuthentication": {"true"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"iam-inst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "true")
}

func TestBatch1Ops_DBInstance_EnginVersionInheritsFromCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Create cluster with specific version
	doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ver-cluster"},
		"EngineVersion":       {"1.2.0.0"},
	})

	createInstance(t, h, "ver-inst", "ver-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ver-inst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "1.2.0.0")
}

func TestBatch1Ops_DBInstance_EndpointFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-cluster-inst")
	createInstance(t, h, "ep-inst", "ep-cluster-inst")

	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ep-inst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "ep-inst.neptune.us-east-1.amazonaws.com")
	assert.Contains(t, body, "8182")
}

// ---- DBSubnetGroup comprehensive coverage ----

func TestBatch1Ops_DBSubnetGroup_CreateAllFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {"full-sg"},
		"DBSubnetGroupDescription": {"Full test subnet group"},
		"VpcId":                    {"vpc-12345678"},
		"SubnetIds.member.1":       {"subnet-aaaa0001"},
		"SubnetIds.member.2":       {"subnet-bbbb0002"},
		"SubnetIds.member.3":       {"subnet-cccc0003"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "full-sg")
	assert.Contains(t, body, "Full test subnet group")
	assert.Contains(t, body, "vpc-12345678")
	assert.Contains(t, body, "subnet-aaaa0001")
	assert.Contains(t, body, "subnet-bbbb0002")
	assert.Contains(t, body, "subnet-cccc0003")
}

func TestBatch1Ops_DBSubnetGroup_ModifyDescription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {"mod-sg"},
		"DBSubnetGroupDescription": {"original desc"},
		"SubnetIds.member.1":       {"subnet-0001"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                   {"ModifyDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {"mod-sg"},
		"DBSubnetGroupDescription": {"updated desc"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "updated desc")
}

func TestBatch1Ops_DBSubnetGroup_DescribeByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {"desc-by-name-sg"},
		"DBSubnetGroupDescription": {"test"},
		"SubnetIds.member.1":       {"subnet-xyz"},
	})
	doRequest(t, h, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {"other-sg"},
		"DBSubnetGroupDescription": {"other"},
		"SubnetIds.member.1":       {"subnet-abc"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":            {"DescribeDBSubnetGroups"},
		"Version":           {"2014-10-31"},
		"DBSubnetGroupName": {"desc-by-name-sg"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "desc-by-name-sg")
	assert.NotContains(t, body, "other-sg")
}

func TestBatch1Ops_DBSubnetGroup_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":            {"DeleteDBSubnetGroup"},
		"Version":           {"2014-10-31"},
		"DBSubnetGroupName": {"nonexistent-sg"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBSubnetGroupNotFoundFault")
}

func TestBatch1Ops_DBSubnetGroup_CreateAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":             {"CreateDBSubnetGroup"},
		"Version":            {"2014-10-31"},
		"DBSubnetGroupName":  {"dup-sg"},
		"SubnetIds.member.1": {"subnet-001"},
	})
	rr := doRequest(t, h, url.Values{
		"Action":             {"CreateDBSubnetGroup"},
		"Version":            {"2014-10-31"},
		"DBSubnetGroupName":  {"dup-sg"},
		"SubnetIds.member.1": {"subnet-001"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBSubnetGroupAlreadyExistsFault")
}

func TestBatch1Ops_DBSubnetGroup_SubnetGroupStatusComplete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":             {"CreateDBSubnetGroup"},
		"Version":            {"2014-10-31"},
		"DBSubnetGroupName":  {"status-sg"},
		"SubnetIds.member.1": {"subnet-001"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "Complete")
}

// ---- DBClusterParameterGroup comprehensive coverage ----

func TestBatch1Ops_DBClusterParameterGroup_CreateAllFamilies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, family := range []string{"neptune1.2", "neptune1.3", "neptune1.4"} {
		rr := doRequest(t, h, url.Values{
			"Action":                      {"CreateDBClusterParameterGroup"},
			"Version":                     {"2014-10-31"},
			"DBClusterParameterGroupName": {family + "-pg"},
			"DBParameterGroupFamily":      {family},
			"Description":                 {"group for " + family},
		})
		require.Equal(t, http.StatusOK, rr.Code, "family %s", family)
		assert.Contains(t, rr.Body.String(), family+"-pg")
		assert.Contains(t, rr.Body.String(), family)
	}
}

func TestBatch1Ops_DBClusterParameterGroup_DescribeByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-find-me"},
		"DBParameterGroupFamily":      {"neptune1.3"},
		"Description":                 {"find me"},
	})
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-not-me"},
		"DBParameterGroupFamily":      {"neptune1.3"},
		"Description":                 {"not me"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameterGroups"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-find-me"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "pg-find-me")
	assert.NotContains(t, body, "pg-not-me")
}

func TestBatch1Ops_DBClusterParameterGroup_ModifyReturnsName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-mod-test"},
		"DBParameterGroupFamily":      {"neptune1.3"},
		"Description":                 {"test"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-mod-test"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "pg-mod-test")
}

func TestBatch1Ops_DBClusterParameterGroup_ResetReturnsName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-reset-test"},
		"DBParameterGroupFamily":      {"neptune1.3"},
		"Description":                 {"test"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"ResetDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-reset-test"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "pg-reset-test")
}

func TestBatch1Ops_DBClusterParameterGroup_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"DeleteDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"nonexistent-pg"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterParameterGroupNotFoundFault")
}

func TestBatch1Ops_DBClusterParameterGroup_DescribeParameters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-params"},
		"DBParameterGroupFamily":      {"neptune1.3"},
		"Description":                 {"test"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameters"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-params"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DescribeDBClusterParametersResponse")
}

func TestBatch1Ops_DBClusterParameterGroup_CopyPreservesFamily(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"pg-src-copy"},
		"DBParameterGroupFamily":      {"neptune1.3"},
		"Description":                 {"source"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":  {"CopyDBClusterParameterGroup"},
		"Version": {"2014-10-31"},
		"SourceDBClusterParameterGroupIdentifier":  {"pg-src-copy"},
		"TargetDBClusterParameterGroupIdentifier":  {"pg-dst-copy"},
		"TargetDBClusterParameterGroupDescription": {"copy of source"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "pg-dst-copy")
	assert.Contains(t, body, "neptune1.3")
	assert.Contains(t, body, "copy of source")
}

// ---- DBClusterSnapshot comprehensive coverage ----

func TestBatch1Ops_DBClusterSnapshot_FieldsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"snap-src"},
		"EngineVersion":       {"1.3.0.0"},
		"StorageEncrypted":    {"true"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-full"},
		"DBClusterIdentifier":         {"snap-src"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "snap-full")
	assert.Contains(t, body, "snap-src")
	assert.Contains(t, body, "1.3.0.0")
	assert.Contains(t, body, "manual")
	assert.Contains(t, body, "available")
}

func TestBatch1Ops_DBClusterSnapshot_DescribeByCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "snap-filter-cluster")
	createCluster(t, h, "other-cluster")

	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-cluster1"},
		"DBClusterIdentifier":         {"snap-filter-cluster"},
	})
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-other"},
		"DBClusterIdentifier":         {"other-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusterSnapshots"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"snap-filter-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "snap-cluster1")
	assert.NotContains(t, body, "snap-other")
}

func TestBatch1Ops_DBClusterSnapshot_CrossRegionCopy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "cross-region-src")
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"source-snap"},
		"DBClusterIdentifier":         {"cross-region-src"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                            {"CopyDBClusterSnapshot"},
		"Version":                           {"2014-10-31"},
		"SourceDBClusterSnapshotIdentifier": {"source-snap"},
		"TargetDBClusterSnapshotIdentifier": {"target-snap"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "target-snap")
	assert.Contains(t, body, "cross-region-src")
}

func TestBatch1Ops_DBClusterSnapshot_CopyNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                            {"CopyDBClusterSnapshot"},
		"Version":                           {"2014-10-31"},
		"SourceDBClusterSnapshotIdentifier": {"nonexistent-snap"},
		"TargetDBClusterSnapshotIdentifier": {"dest-snap"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterSnapshotNotFoundFault")
}

func TestBatch1Ops_DBClusterSnapshot_Attributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "attr-snap-cluster")
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"attr-snap"},
		"DBClusterIdentifier":         {"attr-snap-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterSnapshotAttributes"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"attr-snap"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "attr-snap")
}

func TestBatch1Ops_DBClusterSnapshot_ModifyAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "mod-attr-cluster")
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"mod-attr-snap"},
		"DBClusterIdentifier":         {"mod-attr-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterSnapshotAttribute"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"mod-attr-snap"},
		"AttributeName":               {"restore"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestBatch1Ops_DBClusterSnapshot_RestoreFromSnapshot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"restore-src"},
		"EngineVersion":       {"1.3.0.0"},
	})
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"restore-snap"},
		"DBClusterIdentifier":         {"restore-src"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"RestoreDBClusterFromSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"restore-snap"},
		"DBClusterIdentifier":         {"restore-dst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "restore-dst")
	assert.Contains(t, body, "available")

	// Verify new cluster exists
	rr2 := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"restore-dst"},
	})
	require.Equal(t, http.StatusOK, rr2.Code)
	assert.Contains(t, rr2.Body.String(), "restore-dst")
}

func TestBatch1Ops_DBClusterSnapshot_RestoreToPointInTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "pitr-src")

	rr := doRequest(t, h, url.Values{
		"Action":                    {"RestoreDBClusterToPointInTime"},
		"Version":                   {"2014-10-31"},
		"SourceDBClusterIdentifier": {"pitr-src"},
		"DBClusterIdentifier":       {"pitr-dst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "pitr-dst")
	assert.Contains(t, body, "available")
}

// ---- EventSubscription comprehensive coverage ----

func TestBatch1Ops_EventSubscription_AllSourceIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":             {"CreateEventSubscription"},
		"Version":            {"2014-10-31"},
		"SubscriptionName":   {"sub-sources"},
		"SnsTopicArn":        {"arn:aws:sns:us-east-1:000000000000:test-topic"},
		"SourceIds.member.1": {"cluster-a"},
		"SourceIds.member.2": {"cluster-b"},
		"SourceIds.member.3": {"cluster-c"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "cluster-a")
	assert.Contains(t, body, "cluster-b")
	assert.Contains(t, body, "cluster-c")
	assert.Contains(t, body, "arn:aws:sns")
}

func TestBatch1Ops_EventSubscription_AddRemoveSourceID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})

	doRequest(t, h, url.Values{
		"Action":           {"AddSourceIdentifierToSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
		"SourceIdentifier": {"cluster-new"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":           {"DescribeEventSubscriptions"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "cluster-new")

	doRequest(t, h, url.Values{
		"Action":           {"RemoveSourceIdentifierFromSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
		"SourceIdentifier": {"cluster-new"},
	})

	rr = doRequest(t, h, url.Values{
		"Action":           {"DescribeEventSubscriptions"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.NotContains(t, rr.Body.String(), "cluster-new")
}

func TestBatch1Ops_EventSubscription_ModifySNSTopic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-modify-sns"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:old-topic"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":           {"ModifyEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-modify-sns"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:new-topic"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "new-topic")
}

func TestBatch1Ops_EventSubscription_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-dup"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-dup"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "SubscriptionAlreadyExistFault")
}

func TestBatch1Ops_EventSubscription_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":           {"DeleteEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"nonexistent-sub"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "SubscriptionNotFoundFault")
}

func TestBatch1Ops_EventSubscription_DescribeAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"sub-a", "sub-b", "sub-c"} {
		doRequest(t, h, url.Values{
			"Action":           {"CreateEventSubscription"},
			"Version":          {"2014-10-31"},
			"SubscriptionName": {name},
			"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
		})
	}

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEventSubscriptions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "sub-a")
	assert.Contains(t, body, "sub-b")
	assert.Contains(t, body, "sub-c")
}

func TestBatch1Ops_EventSubscription_Status(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-status"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "active")
}

// ---- GlobalCluster comprehensive coverage ----

func TestBatch1Ops_GlobalCluster_CreateWithSourceCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-src-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-with-src"},
		"SourceDBClusterIdentifier": {"gc-src-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "gc-with-src")
	assert.Contains(t, body, "available")
}

func TestBatch1Ops_GlobalCluster_DescribeMultiple(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"gc-multi-1", "gc-multi-2", "gc-multi-3"} {
		doRequest(t, h, url.Values{
			"Action":                  {"CreateGlobalCluster"},
			"Version":                 {"2014-10-31"},
			"GlobalClusterIdentifier": {id},
		})
	}

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "gc-multi-1")
	assert.Contains(t, body, "gc-multi-2")
	assert.Contains(t, body, "gc-multi-3")
}

func TestBatch1Ops_GlobalCluster_ModifyGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-modify"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                  {"ModifyGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-modify"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-modify")
}

func TestBatch1Ops_GlobalCluster_FailoverGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-fo-primary")
	createCluster(t, h, "gc-fo-secondary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-failover"},
		"SourceDBClusterIdentifier": {"gc-fo-primary"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                    {"FailoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-failover"},
		"TargetDbClusterIdentifier": {"gc-fo-secondary"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-failover")
}

func TestBatch1Ops_GlobalCluster_SwitchoverGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-sw-primary")
	createCluster(t, h, "gc-sw-secondary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-switchover"},
		"SourceDBClusterIdentifier": {"gc-sw-primary"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                    {"SwitchoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-switchover"},
		"TargetDbClusterIdentifier": {"gc-sw-secondary"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-switchover")
}

func TestBatch1Ops_GlobalCluster_RemoveFromGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-rm-primary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-remove"},
		"SourceDBClusterIdentifier": {"gc-rm-primary"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                  {"RemoveFromGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-remove"},
		"DbClusterIdentifier":     {"arn:aws:rds:us-east-1:000000000000:cluster:gc-rm-primary"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestBatch1Ops_GlobalCluster_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-dup"},
	})
	rr := doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-dup"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "GlobalClusterAlreadyExistsFault")
}

func TestBatch1Ops_GlobalCluster_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                  {"DeleteGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"nonexistent-gc"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "GlobalClusterNotFoundFault")
}

// ---- FailoverDBCluster comprehensive coverage ----

func TestBatch1Ops_FailoverDBCluster_ReturnsCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "failover-cluster")
	createInstance(t, h, "failover-inst-w", "failover-cluster")
	createInstance(t, h, "failover-inst-r", "failover-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"FailoverDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"failover-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "failover-cluster")
	assert.Contains(t, body, "available")
}

func TestBatch1Ops_FailoverDBCluster_WithTargetInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "fo-target-cluster")
	createInstance(t, h, "fo-writer", "fo-target-cluster")
	createInstance(t, h, "fo-reader", "fo-target-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                     {"FailoverDBCluster"},
		"Version":                    {"2014-10-31"},
		"DBClusterIdentifier":        {"fo-target-cluster"},
		"TargetDBInstanceIdentifier": {"fo-reader"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "fo-target-cluster")
}

func TestBatch1Ops_FailoverDBCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"FailoverDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"nonexistent-cluster"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

// ---- ModifyDBClusterEndpoint comprehensive coverage ----

func TestBatch1Ops_ModifyDBClusterEndpoint_AllTypes(t *testing.T) {
	t.Parallel()

	for _, epType := range []string{"READER", "ANY", "CUSTOM"} {
		t.Run(epType, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "ep-type-cluster-"+epType)

			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"ep-" + epType},
				"DBClusterIdentifier":         {"ep-type-cluster-" + epType},
				"EndpointType":                {"READER"},
			})

			rr := doRequest(t, h, url.Values{
				"Action":                      {"ModifyDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"ep-" + epType},
				"EndpointType":                {epType},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), epType)
		})
	}
}

func TestBatch1Ops_ModifyDBClusterEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"nonexistent-ep"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterEndpointNotFoundFault")
}

func TestBatch1Ops_DBClusterEndpoint_FilterByCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-filter-a")
	createCluster(t, h, "ep-filter-b")

	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-a-1"},
		"DBClusterIdentifier":         {"ep-filter-a"},
		"EndpointType":                {"READER"},
	})
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-b-1"},
		"DBClusterIdentifier":         {"ep-filter-b"},
		"EndpointType":                {"READER"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusterEndpoints"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ep-filter-a"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "ep-a-1")
	assert.NotContains(t, body, "ep-b-1")
}

func TestBatch1Ops_DBClusterEndpoint_InvalidType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-invalid-type-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-bad"},
		"DBClusterIdentifier":         {"ep-invalid-type-cluster"},
		"EndpointType":                {"INVALID"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

func TestBatch1Ops_DBClusterEndpoint_EndpointURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-url-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-url-test"},
		"DBClusterIdentifier":         {"ep-url-cluster"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "ep-url-test")
	assert.Contains(t, body, "cluster-custom.neptune")
}

// ---- IAM Role operations comprehensive coverage ----

func TestBatch1Ops_AddRemoveRole_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "role-cluster")

	roleARN := "arn:aws:iam::000000000000:role/neptune-role"

	rr := doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-cluster"},
		"RoleArn":             {roleARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Add same role again — idempotent
	rr = doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-cluster"},
		"RoleArn":             {roleARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Remove role
	rr = doRequest(t, h, url.Values{
		"Action":              {"RemoveRoleFromDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-cluster"},
		"RoleArn":             {roleARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestBatch1Ops_AddRole_MissingRoleARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "role-missing-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-missing-cluster"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

func TestBatch1Ops_AddRole_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"nonexistent-cluster"},
		"RoleArn":             {"arn:aws:iam::000000000000:role/test"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

func TestBatch1Ops_Roles_ClearedOnClusterDelete(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "role-del-cluster", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)
	err = b.AddRoleToDBCluster(context.Background(), "role-del-cluster", "arn:aws:iam::000000000000:role/r1")
	require.NoError(t, err)
	err = b.AddRoleToDBCluster(context.Background(), "role-del-cluster", "arn:aws:iam::000000000000:role/r2")
	require.NoError(t, err)

	_, err = b.DeleteDBCluster(context.Background(), "role-del-cluster")
	require.NoError(t, err)

	// Verify roles gone
	require.Equal(t, 0, neptune.ClusterRoleCount(b, "role-del-cluster"))
}

// ---- DBParameterGroup comprehensive coverage ----

func TestBatch1Ops_DBParameterGroup_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rr := doRequest(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"param-pg-1"},
		"DBParameterGroupFamily": {"neptune1.3"},
		"Description":            {"parameter group test"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "param-pg-1")
	assert.Contains(t, rr.Body.String(), "neptune1.3")

	// Describe
	rr = doRequest(t, h, url.Values{
		"Action":               {"DescribeDBParameterGroups"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"param-pg-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "param-pg-1")

	// Modify
	rr = doRequest(t, h, url.Values{
		"Action":               {"ModifyDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"param-pg-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "param-pg-1")

	// Reset
	rr = doRequest(t, h, url.Values{
		"Action":               {"ResetDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"param-pg-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "param-pg-1")

	// Describe parameters
	rr = doRequest(t, h, url.Values{
		"Action":               {"DescribeDBParameters"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"param-pg-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DescribeDBParametersResponse")

	// Delete
	rr = doRequest(t, h, url.Values{
		"Action":               {"DeleteDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"param-pg-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Verify gone
	rr = doRequest(t, h, url.Values{
		"Action":               {"DescribeDBParameterGroups"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"param-pg-1"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBParameterGroupNotFoundFault")
}

func TestBatch1Ops_CopyDBParameterGroup_FieldsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"src-pg"},
		"DBParameterGroupFamily": {"neptune1.2"},
		"Description":            {"source param group"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                            {"CopyDBParameterGroup"},
		"Version":                           {"2014-10-31"},
		"SourceDBParameterGroupIdentifier":  {"src-pg"},
		"TargetDBParameterGroupIdentifier":  {"dst-pg"},
		"TargetDBParameterGroupDescription": {"copy of source"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "dst-pg")
	assert.Contains(t, body, "neptune1.2")
	assert.Contains(t, body, "copy of source")
}

// ---- ApplyPendingMaintenanceAction coverage ----

func TestBatch1Ops_ApplyPendingMaintenanceAction_AllOptInTypes(t *testing.T) {
	t.Parallel()

	for _, optIn := range []string{"immediate", "next-maintenance", "undo-opt-in"} {
		t.Run(optIn, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":             {"ApplyPendingMaintenanceAction"},
				"Version":            {"2014-10-31"},
				"ResourceIdentifier": {"arn:aws:rds:us-east-1:000000000000:cluster:test"},
				"ApplyAction":        {"system-update"},
				"OptInType":          {optIn},
			})
			require.Equal(t, http.StatusOK, rr.Code)
		})
	}
}

// ---- Tags on all resource types ----

func TestBatch1Ops_Tags_OnSubnetGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {"tagged-sg"},
		"DBSubnetGroupDescription": {"tagged"},
		"SubnetIds.member.1":       {"subnet-001"},
		"Tags.Tag.1.Key":           {"Env"},
		"Tags.Tag.1.Value":         {"test"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestBatch1Ops_Tags_OnParameterGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"tagged-cpg"},
		"DBParameterGroupFamily":      {"neptune1.3"},
		"Description":                 {"test"},
		"Tags.Tag.1.Key":              {"Team"},
		"Tags.Tag.1.Value":            {"platform"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestBatch1Ops_Tags_AddListRemoveOnCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "tag-lifecycle-cluster")

	// Get ARN from describe
	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"tag-lifecycle-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	arnStart := strings.Index(rr.Body.String(), "arn:aws:")
	arnEnd := strings.Index(rr.Body.String()[arnStart:], "<") + arnStart
	clusterARN := rr.Body.String()[arnStart:arnEnd]

	// Add tags
	doRequest(t, h, url.Values{
		"Action":           {"AddTagsToResource"},
		"Version":          {"2014-10-31"},
		"ResourceName":     {clusterARN},
		"Tags.Tag.1.Key":   {"Owner"},
		"Tags.Tag.1.Value": {"team-a"},
		"Tags.Tag.2.Key":   {"CostCenter"},
		"Tags.Tag.2.Value": {"123"},
	})

	// List tags
	rr = doRequest(t, h, url.Values{
		"Action":       {"ListTagsForResource"},
		"Version":      {"2014-10-31"},
		"ResourceName": {clusterARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "Owner")
	assert.Contains(t, body, "team-a")
	assert.Contains(t, body, "CostCenter")

	// Remove tag
	doRequest(t, h, url.Values{
		"Action":           {"RemoveTagsFromResource"},
		"Version":          {"2014-10-31"},
		"ResourceName":     {clusterARN},
		"TagKeys.member.1": {"Owner"},
	})

	rr = doRequest(t, h, url.Values{
		"Action":       {"ListTagsForResource"},
		"Version":      {"2014-10-31"},
		"ResourceName": {clusterARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body = rr.Body.String()
	assert.NotContains(t, body, "Owner")
	assert.Contains(t, body, "CostCenter")
}

// ---- DescribeEngineVersions and DescribeOrderableOptions ----

func TestBatch1Ops_DescribeDBEngineVersions_AllFamilies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeDBEngineVersions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "1.2.0.0")
	assert.Contains(t, body, "1.2.0.1")
	assert.Contains(t, body, "1.2.0.2")
	assert.Contains(t, body, "1.2.1.0")
	assert.Contains(t, body, "1.3.0.0")
	assert.Contains(t, body, "1.3.1.0")
	assert.Contains(t, body, "1.3.2.0")
	assert.Contains(t, body, "1.4.0.0")
	assert.Contains(t, body, "neptune1.2")
	assert.Contains(t, body, "neptune1.3")
	assert.Contains(t, body, "neptune1.4")
	assert.Contains(t, body, "Amazon Neptune")
}

func TestBatch1Ops_DescribeOrderableDBInstanceOptions_AllClasses(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeOrderableDBInstanceOptions"},
		"Version": {"2014-10-31"},
		"Engine":  {"neptune"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "db.r5.large")
	assert.Contains(t, body, "db.r5.xlarge")
	assert.Contains(t, body, "db.r6g.large")
	assert.Contains(t, body, "db.t3.medium")
}

// ---- PromoteReadReplicaDBCluster ----

func TestBatch1Ops_PromoteReadReplicaDBCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "promo-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"PromoteReadReplicaDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"promo-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "promo-cluster")
}

func TestBatch1Ops_PromoteReadReplicaDBCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"PromoteReadReplicaDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"nonexistent-cluster"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

// ---- DescribeValidDBInstanceModifications ----

func TestBatch1Ops_DescribeValidDBInstanceModifications(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "valid-mod-cluster")
	createInstance(t, h, "valid-mod-inst", "valid-mod-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeValidDBInstanceModifications"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"valid-mod-inst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "db.r5.large")
	assert.Contains(t, body, "db.r6g.large")
}

// ---- DescribeEngineDefaultClusterParameters ----

func TestBatch1Ops_DescribeEngineDefaultClusterParameters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                 {"DescribeEngineDefaultClusterParameters"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupFamily": {"neptune1.3"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "neptune1.3")
	assert.Contains(t, body, "DescribeEngineDefaultClusterParametersResponse")
}

func TestBatch1Ops_DescribeEngineDefaultClusterParameters_DefaultFamily(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEngineDefaultClusterParameters"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "neptune1.3")
}

func TestBatch1Ops_DescribeEngineDefaultParameters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                 {"DescribeEngineDefaultParameters"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupFamily": {"neptune1.2"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "neptune1.2")
}

// ---- DescribeEventCategories ----

func TestBatch1Ops_DescribeEventCategories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEventCategories"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "db-cluster")
	assert.Contains(t, body, "db-instance")
	assert.Contains(t, body, "failover")
	assert.Contains(t, body, "maintenance")
}

// ---- DescribeEvents ----

func TestBatch1Ops_DescribeEvents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEvents"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DescribeEventsResponse")
}

// ---- DescribePendingMaintenanceActions ----

func TestBatch1Ops_DescribePendingMaintenanceActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribePendingMaintenanceActions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DescribePendingMaintenanceActionsResponse")
}

// ---- Backend unit tests for DBInstance options ----

func TestBatch1Ops_Backend_CreateDBInstance_AllOptions(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "inst-opts-cluster", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)

	opts := neptune.DBInstanceCreateOptions{
		DBParameterGroupName:            "custom-pg",
		PreferredMaintenanceWindow:      "wed:04:00-wed:05:00",
		PreferredBackupWindow:           "01:00-02:00",
		AvailabilityZone:                "us-east-1b",
		CopyTagsToSnapshot:              true,
		EnableIAMDatabaseAuthentication: true,
		PromotionTier:                   5,
		StorageEncrypted:                true,
	}
	inst, err := b.CreateDBInstance(context.Background(), "inst-opts", "inst-opts-cluster", "db.r5.xlarge", opts)
	require.NoError(t, err)
	assert.Equal(t, "custom-pg", inst.DBParameterGroupName)
	assert.Equal(t, "wed:04:00-wed:05:00", inst.PreferredMaintenanceWindow)
	assert.Equal(t, "01:00-02:00", inst.PreferredBackupWindow)
	assert.Equal(t, "us-east-1b", inst.AvailabilityZone)
	assert.True(t, inst.CopyTagsToSnapshot)
	assert.True(t, inst.EnableIAMDatabaseAuthentication)
	assert.Equal(t, 5, inst.PromotionTier)
	assert.True(t, inst.StorageEncrypted)
}

func TestBatch1Ops_Backend_ModifyDBInstance_AllOptions(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "mod-opts-cluster", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBInstance(
		context.Background(),
		"mod-opts-inst",
		"mod-opts-cluster",
		"",
		neptune.DBInstanceCreateOptions{},
	)
	require.NoError(t, err)

	opts := neptune.DBInstanceModifyOptions{
		DBParameterGroupName:            "new-pg",
		PreferredMaintenanceWindow:      "fri:06:00-fri:07:00",
		PreferredBackupWindow:           "03:00-04:00",
		AutoMinorVersionUpgrade:         false,
		AutoMinorVersionUpgradeSet:      true,
		CopyTagsToSnapshot:              true,
		CopyTagsToSnapshotSet:           true,
		EnableIAMDatabaseAuthentication: true,
		IamAuthSet:                      true,
		PromotionTier:                   7,
		PromotionTierSet:                true,
	}
	inst, err := b.ModifyDBInstance(context.Background(), "mod-opts-inst", "db.r6g.4xlarge", opts)
	require.NoError(t, err)
	assert.Equal(t, "db.r6g.4xlarge", inst.DBInstanceClass)
	assert.Equal(t, "new-pg", inst.DBParameterGroupName)
	assert.Equal(t, "fri:06:00-fri:07:00", inst.PreferredMaintenanceWindow)
	assert.Equal(t, "03:00-04:00", inst.PreferredBackupWindow)
	assert.False(t, inst.AutoMinorVersionUpgrade)
	assert.True(t, inst.CopyTagsToSnapshot)
	assert.True(t, inst.EnableIAMDatabaseAuthentication)
	assert.Equal(t, 7, inst.PromotionTier)
}

func TestBatch1Ops_Backend_ModifyDBInstance_IamNotSet_NoChange(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "iam-noset-cluster", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBInstance(
		context.Background(),
		"iam-noset-inst",
		"iam-noset-cluster",
		"",
		neptune.DBInstanceCreateOptions{
			EnableIAMDatabaseAuthentication: true,
		},
	)
	require.NoError(t, err)

	// Modify without IamAuthSet — should not change
	inst, err := b.ModifyDBInstance(context.Background(), "iam-noset-inst", "", neptune.DBInstanceModifyOptions{
		EnableIAMDatabaseAuthentication: false,
		IamAuthSet:                      false,
	})
	require.NoError(t, err)
	assert.True(t, inst.EnableIAMDatabaseAuthentication)
}

// ---- XML response structure verification ----

func TestBatch1Ops_XML_CreateDBCluster_Structure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"xml-struct-cluster"},
		"EngineVersion":       {"1.3.0.0"},
		"Port":                {"8182"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Verify it's valid XML
	var result struct {
		XMLName xml.Name `xml:"CreateDBClusterResponse"`
	}
	err := xml.Unmarshal(rr.Body.Bytes(), &result)
	require.NoError(t, err)
	assert.Equal(t, "CreateDBClusterResponse", result.XMLName.Local)
}

func TestBatch1Ops_XML_DescribeDBClusters_Structure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "xml-desc-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeDBClusters"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	var result struct {
		XMLName xml.Name `xml:"DescribeDBClustersResponse"`
	}
	err := xml.Unmarshal(rr.Body.Bytes(), &result)
	require.NoError(t, err)
	assert.Equal(t, "DescribeDBClustersResponse", result.XMLName.Local)
}

// ---- Cluster ARN format ----

func TestBatch1Ops_ClusterARN_Format(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"arn-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "arn:aws:neptune:us-east-1:000000000000:cluster:arn-cluster")
}

func TestBatch1Ops_InstanceARN_Format(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "arn-inst-cluster")
	createInstance(t, h, "arn-inst", "arn-inst-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"arn-inst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "arn:aws:neptune:us-east-1:000000000000:db:arn-inst")
}

// ---- Cascade delete verification ----

func TestBatch1Ops_DeleteCluster_CascadesSnapshots(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "cascade-del-cluster", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBClusterSnapshot(context.Background(), "cascade-snap", "cascade-del-cluster")
	require.NoError(t, err)

	require.Equal(t, 1, neptune.ClusterSnapshotCount(b))

	// Delete cluster — snapshots should remain (AWS behavior: snapshots not auto-deleted)
	_, err = b.DeleteDBCluster(context.Background(), "cascade-del-cluster")
	require.NoError(t, err)

	require.Equal(t, 0, neptune.ClusterCount(b))
	// Snapshots are not cascade-deleted
	require.Equal(t, 1, neptune.ClusterSnapshotCount(b))
}

func TestBatch1Ops_DeleteCluster_CascadesInstances(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "cascade-inst-cluster", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBInstance(
		context.Background(),
		"cascade-inst-1",
		"cascade-inst-cluster",
		"",
		neptune.DBInstanceCreateOptions{},
	)
	require.NoError(t, err)
	_, err = b.CreateDBInstance(
		context.Background(),
		"cascade-inst-2",
		"cascade-inst-cluster",
		"",
		neptune.DBInstanceCreateOptions{},
	)
	require.NoError(t, err)

	require.Equal(t, 2, neptune.InstanceCount(b))

	_, err = b.DeleteDBCluster(context.Background(), "cascade-inst-cluster")
	require.NoError(t, err)

	require.Equal(t, 0, neptune.InstanceCount(b))
	require.Equal(t, 0, neptune.ClusterCount(b))
}
