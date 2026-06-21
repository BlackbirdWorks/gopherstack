package rds_test

import (
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// ---- helpers ----

func newBatch3Backend() *rds.InMemoryBackend {
	return rds.NewInMemoryBackend("123456789012", "us-east-1")
}

func newBatch3Handler() *rds.Handler {
	return rds.NewHandler(newBatch3Backend())
}

// ---- DescribeCustomDBEngineVersions ----

func TestDescribeCustomDBEngineVersions_Empty(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	versions := b.DescribeCustomDBEngineVersions("", "")
	assert.Empty(t, versions)
}

func TestDescribeCustomDBEngineVersions_AfterCreate(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateCustomDBEngineVersion("oracle-ee", "19.0.0.0.ru-2024-04.rur-2024-04.r1", "test cev")
	require.NoError(t, err)

	_, err = b.CreateCustomDBEngineVersion("oracle-ee", "19.0.0.0.ru-2023-10.rur-2023-10.r1", "older cev")
	require.NoError(t, err)

	all := b.DescribeCustomDBEngineVersions("", "")
	require.Len(t, all, 2)

	filtered := b.DescribeCustomDBEngineVersions("oracle-ee", "19.0.0.0.ru-2024-04.rur-2024-04.r1")
	require.Len(t, filtered, 1)
	assert.Equal(t, "19.0.0.0.ru-2024-04.rur-2024-04.r1", filtered[0].EngineVersion)
}

func TestDescribeCustomDBEngineVersions_EngineFilter(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateCustomDBEngineVersion("oracle-ee", "19.v1", "oracle")
	require.NoError(t, err)

	_, err = b.CreateCustomDBEngineVersion("oracle-se2", "19.v2", "oracle se2")
	require.NoError(t, err)

	filtered := b.DescribeCustomDBEngineVersions("oracle-ee", "")
	require.Len(t, filtered, 1)
	assert.Equal(t, "oracle-ee", filtered[0].Engine)
}

func TestDescribeCustomDBEngineVersions_SortedResults(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateCustomDBEngineVersion("oracle-ee", "19.v2", "v2")
	require.NoError(t, err)

	_, err = b.CreateCustomDBEngineVersion("oracle-ee", "19.v1", "v1")
	require.NoError(t, err)

	all := b.DescribeCustomDBEngineVersions("oracle-ee", "")
	require.Len(t, all, 2)
	assert.Equal(t, "19.v1", all[0].EngineVersion, "results should be sorted by version")
	assert.Equal(t, "19.v2", all[1].EngineVersion)
}

func TestDescribeCustomDBEngineVersions_ViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateCustomDBEngineVersion("oracle-ee", "19.test.v1", "test cev")
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=DescribeCustomDBEngineVersions&Version=2014-10-31&Engine=oracle-ee")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "oracle-ee")
	assert.Contains(t, rec.Body.String(), "19.test.v1")
	assert.Contains(t, rec.Body.String(), "DescribeCustomDBEngineVersionsResponse")
}

func TestDescribeCustomDBEngineVersions_InSupportedOps(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()
	ops := h.GetSupportedOperations()

	assert.True(
		t,
		slices.Contains(ops, "DescribeCustomDBEngineVersions"),
		"DescribeCustomDBEngineVersions should be in supported operations",
	)
}

// ---- DBCluster new fields ----

func TestDBCluster_ReaderEndpointGenerated(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("my-cluster", "aurora-mysql", "admin", "mydb", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	assert.NotEmpty(t, c.ReaderEndpoint, "ReaderEndpoint should be set on cluster creation")
	assert.Contains(t, c.ReaderEndpoint, "my-cluster")
	assert.Contains(t, c.ReaderEndpoint, "-ro")
}

func TestDBCluster_NetworkTypeIPV4Default(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("net-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	assert.Equal(t, "IPV4", c.NetworkType)
}

func TestDBCluster_NetworkTypeDual(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("dual-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{NetworkType: "DUAL"})
	require.NoError(t, err)

	assert.Equal(t, "DUAL", c.NetworkType)
}

func TestDBCluster_StorageTypeAuroraIOOptimized(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("iopt-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{StorageType: "aurora-iopt1"})
	require.NoError(t, err)

	assert.Equal(t, "aurora-iopt1", c.StorageType)
}

func TestDBCluster_EngineLifecycleSupport(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("els-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{
			EngineLifecycleSupport: "open-source-rds-extended-support",
		})
	require.NoError(t, err)

	assert.Equal(t, "open-source-rds-extended-support", c.EngineLifecycleSupport)
}

func TestDBCluster_OptimizedWritesEnabled(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("ow-cluster", "aurora-mysql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{OptimizedWrites: true})
	require.NoError(t, err)

	assert.True(t, c.OptimizedWrites)
}

func TestDBCluster_ModifyStorageTypeAndNetworkType(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBCluster("mod-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	modified, err := b.ModifyDBCluster("mod-cluster", "", rds.DBClusterOptions{
		StorageType: "aurora-iopt1",
		NetworkType: "DUAL",
	})
	require.NoError(t, err)

	assert.Equal(t, "aurora-iopt1", modified.StorageType)
	assert.Equal(t, "DUAL", modified.NetworkType)
}

func TestDBCluster_NewFieldsViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateDBCluster&Version=2014-10-31"+
			"&DBClusterIdentifier=handler-cluster&Engine=aurora-mysql&MasterUsername=admin"+
			"&StorageType=aurora-iopt1&NetworkType=DUAL"+
			"&EngineLifecycleSupport=open-source-rds-extended-support"+
			"&EnableOptimizedWrites=true")
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "aurora-iopt1", "StorageType should be in response")
	assert.Contains(t, respStr, "DUAL", "NetworkType should be in response")
	assert.Contains(t, respStr, "open-source-rds-extended-support")
	assert.Contains(t, respStr, "ReaderEndpoint")
}

// ---- DBInstance new fields ----

func TestDBInstance_OptimizedWritesAndStorageOptimized(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	inst, err := b.CreateDBInstance("ow-inst", "postgres", "db.r6g.large", "", "admin", "", 100,
		rds.DBInstanceOptions{
			StorageOptimized: true,
			OptimizedWrites:  true,
		})
	require.NoError(t, err)

	assert.True(t, inst.StorageOptimized)
	assert.True(t, inst.OptimizedWrites)
}

func TestDBInstance_EngineLifecycleSupport(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	inst, err := b.CreateDBInstance("els-inst", "postgres", "db.t3.micro", "", "admin", "", 20,
		rds.DBInstanceOptions{
			EngineLifecycleSupport: "open-source-rds-extended-support-disabled",
		})
	require.NoError(t, err)

	assert.Equal(t, "open-source-rds-extended-support-disabled", inst.EngineLifecycleSupport)
}

func TestDBInstance_ModifyOptimizedWrites(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBInstance("mod-ow", "postgres", "db.t3.micro", "", "admin", "", 20,
		rds.DBInstanceOptions{})
	require.NoError(t, err)

	modified, err := b.ModifyDBInstance("mod-ow", "", 0,
		rds.DBInstanceOptions{
			OptimizedWrites:        true,
			EngineLifecycleSupport: "open-source-rds-extended-support",
		})
	require.NoError(t, err)

	assert.True(t, modified.OptimizedWrites)
	assert.Equal(t, "open-source-rds-extended-support", modified.EngineLifecycleSupport)
}

func TestDBInstance_NewFieldsViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateDBInstance&Version=2014-10-31"+
			"&DBInstanceIdentifier=handler-inst&DBInstanceClass=db.r6g.large"+
			"&Engine=postgres&MasterUsername=admin&AllocatedStorage=100"+
			"&EnableOptimizedWrites=true&StorageOptimized=true"+
			"&EngineLifecycleSupport=open-source-rds-extended-support-disabled")
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "OptimizedWritesEnabled")
	assert.Contains(t, respStr, "StorageOptimized")
	assert.Contains(t, respStr, "open-source-rds-extended-support-disabled")
}

// ---- Blue/Green Deployment Target field ----

func TestBlueGreenDeployment_TargetGenerated(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	dep, err := b.CreateBlueGreenDeployment("my-bgd", "arn:aws:rds:us-east-1:123:cluster:source")
	require.NoError(t, err)

	assert.NotEmpty(t, dep.Target, "Target should be set for blue/green deployment")
	assert.Contains(t, dep.Target, "source-green", "Target should be derived from source")
}

func TestBlueGreenDeployment_TargetViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateBlueGreenDeployment&Version=2014-10-31"+
			"&BlueGreenDeploymentName=bgd-test"+
			"&Source=arn:aws:rds:us-east-1:123:cluster:my-cluster")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "<Target>", "Target field should be in XML response")
	assert.Contains(t, respStr, "green")
}

// ---- DBShardGroup Endpoint field ----

func TestDBShardGroup_EndpointGenerated(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	sg, err := b.CreateDBShardGroup("my-sg", "my-cluster", 128, 0.5, 1, false)
	require.NoError(t, err)

	assert.NotEmpty(t, sg.Endpoint, "Endpoint should be generated for shard group")
	assert.Contains(t, sg.Endpoint, "my-sg", "Endpoint should contain shard group ID")
	assert.Contains(t, sg.Endpoint, "rds.amazonaws.com")
}

func TestDBShardGroup_EndpointViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=sg-handler&DBClusterIdentifier=cl-handler"+
			"&MaxACU=64&MinACU=2")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "<Endpoint>", "Endpoint should be in XML response")
	assert.Contains(t, respStr, "64", "MaxACU should be in XML response")
}

func TestDBShardGroup_DescribeIncludesAllFields(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBShardGroup("sg-desc", "cl-desc", 100, 1.0, 2, true)
	require.NoError(t, err)

	groups, err := b.DescribeDBShardGroups("")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	sg := groups[0]
	assert.InDelta(t, float64(100), sg.MaxACU, 0.001)
	assert.InDelta(t, float64(1.0), sg.MinACU, 0.001)
	assert.Equal(t, 2, sg.ComputeRedundancy)
	assert.True(t, sg.PubliclyAccessible)
	assert.NotEmpty(t, sg.Endpoint)
}

// ---- Integration DataFilter and Description ----

func TestIntegration_DataFilterPersisted(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	intg, err := b.CreateIntegration(
		"my-intg",
		"arn:aws:rds:us-east-1:123:cluster:source",
		"arn:aws:redshift:us-east-1:123:namespace:target",
		"",
		"include(tableName=orders)",
		"my integration description",
	)
	require.NoError(t, err)

	assert.Equal(t, "include(tableName=orders)", intg.DataFilter)
	assert.Equal(t, "my integration description", intg.IntegrationDescription)
}

func TestIntegration_ModifyDataFilter(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateIntegration("intg-mod", "src", "tgt", "", "", "original desc")
	require.NoError(t, err)

	modified, err := b.ModifyIntegration("intg-mod", "include(tableName=users)", "updated desc")
	require.NoError(t, err)

	assert.Equal(t, "include(tableName=users)", modified.DataFilter)
	assert.Equal(t, "updated desc", modified.IntegrationDescription)
}

func TestIntegration_DataFilterViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateIntegration&Version=2014-10-31"+
			"&IntegrationName=df-intg"+
			"&SourceArn=arn:aws:rds:us-east-1:123:cluster:src"+
			"&TargetArn=arn:aws:redshift:us-east-1:123:namespace:dst"+
			"&DataFilter=include%28tableName%3Dorders%2Ccustomers%29"+
			"&Description=zero-etl+pipeline")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "orders")
	assert.Contains(t, respStr, "zero-etl pipeline")
}

func TestIntegration_ModifyViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateIntegration("mod-intg", "src", "tgt", "", "", "")
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=ModifyIntegration&Version=2014-10-31"+
			"&IntegrationIdentifier=mod-intg"+
			"&DataFilter=include%28tableName%3Devents%29"+
			"&Description=updated+description")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "events")
	assert.Contains(t, respStr, "updated description")
}

// ---- Recommendation status management ----

func TestDBRecommendation_PauseActive(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-1",
		TypeID:           "type-idx-missing",
		Severity:         "medium",
		Status:           "active",
		Description:      "Add index to improve query performance",
	})

	active := b.DescribeDBRecommendations("", "active")
	require.Len(t, active, 1)

	modified, err := b.ModifyDBRecommendation("rec-1", "paused")
	require.NoError(t, err)
	assert.Equal(t, "paused", modified.Status)

	active = b.DescribeDBRecommendations("", "active")
	assert.Empty(t, active, "paused recommendation should not appear in active filter")

	paused := b.DescribeDBRecommendations("", "paused")
	require.Len(t, paused, 1)
	assert.Equal(t, "rec-1", paused[0].RecommendationID)
}

func TestDBRecommendation_InactiveStatus(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-inactive",
		Status:           "active",
		Description:      "test recommendation",
	})

	_, err := b.ModifyDBRecommendation("rec-inactive", "inactive")
	require.NoError(t, err)

	all := b.DescribeDBRecommendations("", "")
	require.Len(t, all, 1)
	assert.Equal(t, "inactive", all[0].Status)
}

func TestDBRecommendation_MultipleStatusTransitions(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	b.AddDBRecommendation(rds.DBRecommendation{RecommendationID: "r1", Status: "active"})
	b.AddDBRecommendation(rds.DBRecommendation{RecommendationID: "r2", Status: "active"})
	b.AddDBRecommendation(rds.DBRecommendation{RecommendationID: "r3", Status: "active"})

	_, err := b.ModifyDBRecommendation("r1", "paused")
	require.NoError(t, err)

	_, err = b.ModifyDBRecommendation("r2", "inactive")
	require.NoError(t, err)

	assert.Len(t, b.DescribeDBRecommendations("", "active"), 1)
	assert.Len(t, b.DescribeDBRecommendations("", "paused"), 1)
	assert.Len(t, b.DescribeDBRecommendations("", "inactive"), 1)
}

// ---- Performance Insights real data ----

func TestPerformanceInsights_ReturnsDataPoints(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	now := time.Now().UTC()
	start := now.Add(-time.Hour)

	points := b.GetPerformanceInsightsData("db-instance-1", "db.load.avg", start, now, 60)

	assert.NotEmpty(t, points, "should return data points for a 1-hour window at 60s intervals")
	assert.Greater(t, len(points), 50, "should have at least 50 data points for a 1-hour window")
}

func TestPerformanceInsights_Deterministic(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	start := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 15, 11, 0, 0, 0, time.UTC)

	points1 := b.GetPerformanceInsightsData("db-1", "db.load.avg", start, end, 60)
	points2 := b.GetPerformanceInsightsData("db-1", "db.load.avg", start, end, 60)

	require.Len(t, points2, len(points1))
	for i := range points1 {
		assert.Equal(t, points1[i].Timestamp, points2[i].Timestamp)
		assert.InDelta(t, points1[i].Value, points2[i].Value, 0)
	}
}

func TestPerformanceInsights_DifferentResourcesDifferentData(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	start := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 15, 10, 10, 0, 0, time.UTC)

	points1 := b.GetPerformanceInsightsData("db-resource-A", "db.load.avg", start, end, 60)
	points2 := b.GetPerformanceInsightsData("db-resource-B", "db.load.avg", start, end, 60)

	require.Len(t, points2, len(points1), "same time window should yield same number of points")

	differentCount := 0
	for i := range points1 {
		if points1[i].Value != points2[i].Value {
			differentCount++
		}
	}

	assert.Positive(t, differentCount, "different resources should produce different values")
}

func TestPerformanceInsights_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()
	start := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 15, 10, 10, 0, 0, time.UTC)

	rec := postRDSForm(t, h,
		"Action=GetPerformanceInsightsMetrics&Version=2014-10-31"+
			"&DBResourceIdentifier=my-db-instance"+
			"&MetricQueries.member.1.Metric=db.load.avg"+
			"&StartTime="+start.Format(time.RFC3339)+
			"&EndTime="+end.Format(time.RFC3339)+
			"&PeriodInSeconds=60")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "GetPerformanceInsightsMetricsResponse")
	assert.Contains(t, respStr, "db.load.avg")
	assert.Contains(t, respStr, "AlignedStartTime")
	assert.Contains(t, respStr, "AlignedEndTime")
	assert.Contains(t, respStr, "DataPoint")
}

// ---- Validate helper functions ----

func TestValidateEngineLifecycleSupport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		val     string
		wantErr bool
	}{
		{"empty", "", false},
		{"open-source", "open-source-rds-extended-support", false},
		{"disabled", "open-source-rds-extended-support-disabled", false},
		{"invalid", "unsupported-value", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := rds.ValidateEngineLifecycleSupport(tt.val)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateStorageTypeForCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		val     string
		wantErr bool
	}{
		{"empty", "", false},
		{"aurora", "aurora", false},
		{"aurora-iopt1", "aurora-iopt1", false},
		{"invalid-io1", "io1", true},
		{"invalid-gp3", "gp3", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := rds.ValidateStorageTypeForCluster(tt.val)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// ---- Handler XML for new fields ----

func TestDescribeDBClusters_ReaderEndpointInResponse(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateDBCluster("re-cluster", "aurora-mysql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=DescribeDBClusters&Version=2014-10-31&DBClusterIdentifier=re-cluster")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "ReaderEndpoint")
	assert.Contains(t, respStr, "re-cluster")
	assert.Contains(t, respStr, "-ro")
}

func TestDBCluster_ReaderEndpointPersistedAndDescribed(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	created, err := b.CreateDBCluster("persist-re", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, created.ReaderEndpoint)

	clusters, err := b.DescribeDBClusters("persist-re")
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	assert.Equal(t, created.ReaderEndpoint, clusters[0].ReaderEndpoint)
}

// ---- Concurrent safety ----

func TestDescribeCustomDBEngineVersions_ConcurrentSafe(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	const goroutines = 10
	done := make(chan struct{}, goroutines)

	for i := range goroutines {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			ver := "19.v" + string(rune('a'+n))
			_, err := b.CreateCustomDBEngineVersion("oracle-ee", ver, "concurrent")
			if err != nil {
				return
			}
			b.DescribeCustomDBEngineVersions("oracle-ee", "")
		}(i)
	}

	for range goroutines {
		<-done
	}

	versions := b.DescribeCustomDBEngineVersions("", "")
	assert.NotEmpty(t, versions)
}

func TestBatch3_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()
	assert.Equal(t, 165, rds.HandlerOpsLen(h))
}

// ---- Persistence tests for new fields ----

func TestBatch3_Persistence_ClusterNewFields(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBCluster("cls-snap", "aurora-mysql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{
			StorageType:            "aurora-iopt1",
			NetworkType:            "DUAL",
			EngineLifecycleSupport: "open-source-rds-extended-support",
			OptimizedWrites:        true,
		})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	clusters, err := b2.DescribeDBClusters("cls-snap")
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	c := clusters[0]
	assert.Equal(t, "aurora-iopt1", c.StorageType, "StorageType should survive round-trip")
	assert.Equal(t, "DUAL", c.NetworkType, "NetworkType should survive round-trip")
	assert.Equal(t, "open-source-rds-extended-support", c.EngineLifecycleSupport)
	assert.True(t, c.OptimizedWrites)
	assert.NotEmpty(t, c.ReaderEndpoint, "ReaderEndpoint should survive round-trip")
}

func TestBatch3_Persistence_InstanceNewFields(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBInstance("inst-snap", "postgres", "db.r6g.large", "", "admin", "", 100,
		rds.DBInstanceOptions{
			StorageOptimized:       true,
			OptimizedWrites:        true,
			EngineLifecycleSupport: "open-source-rds-extended-support-disabled",
		})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	instances, err := b2.DescribeDBInstances("inst-snap")
	require.NoError(t, err)
	require.Len(t, instances, 1)

	inst := instances[0]
	assert.True(t, inst.StorageOptimized, "StorageOptimized should survive round-trip")
	assert.True(t, inst.OptimizedWrites, "OptimizedWrites should survive round-trip")
	assert.Equal(t, "open-source-rds-extended-support-disabled", inst.EngineLifecycleSupport)
}

func TestBatch3_Persistence_BlueGreenTarget(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateBlueGreenDeployment("bgd-snap", "arn:aws:rds:us-east-1:123:cluster:src")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	deployments, err := b2.DescribeBlueGreenDeployments("")
	require.NoError(t, err)
	require.Len(t, deployments, 1)

	assert.NotEmpty(t, deployments[0].Target, "Target should survive round-trip")
	assert.Contains(t, deployments[0].Target, "green")
}

func TestBatch3_Persistence_ShardGroupEndpoint(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBShardGroup("sg-snap", "cl-snap", 64, 1, 1, false)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	groups, err := b2.DescribeDBShardGroups("")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	assert.NotEmpty(t, groups[0].Endpoint, "Endpoint should survive round-trip")
}

func TestBatch3_Persistence_IntegrationDataFilter(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateIntegration("intg-snap", "src", "tgt", "", "include(orders)", "my description")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	integrations, err := b2.DescribeIntegrations("")
	require.NoError(t, err)
	require.Len(t, integrations, 1)

	assert.Equal(t, "include(orders)", integrations[0].DataFilter, "DataFilter should survive round-trip")
	assert.Equal(t, "my description", integrations[0].IntegrationDescription)
}

// ---- Multi-AZ cluster RWG tests ----

func TestDBCluster_MultiAZEndpoints(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("rwg-cluster", "aurora-postgresql", "admin", "prod", "", 0, nil,
		rds.DBClusterOptions{MultiAZ: true})
	require.NoError(t, err)

	assert.NotEmpty(t, c.Endpoint, "writer endpoint should be set")
	assert.NotEmpty(t, c.ReaderEndpoint, "reader endpoint should be set for Multi-AZ")
	assert.True(t, c.MultiAZ)

	assert.NotEqual(t, c.Endpoint, c.ReaderEndpoint,
		"writer and reader endpoints should be different")
}

func TestDBCluster_ServerlessV2WithIOOptimized(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	serverlessCfg := &rds.ServerlessV2ScalingConfiguration{
		MinCapacity: 0.5,
		MaxCapacity: 128.0,
	}

	c, err := b.CreateDBCluster("sl2-iopt", "aurora-postgresql", "admin", "", "", 0, serverlessCfg,
		rds.DBClusterOptions{
			StorageType:            "aurora-iopt1",
			EngineLifecycleSupport: "open-source-rds-extended-support-disabled",
		})
	require.NoError(t, err)

	assert.NotNil(t, c.ServerlessV2ScalingConfig)
	assert.Equal(t, "aurora-iopt1", c.StorageType)
	assert.Equal(t, "open-source-rds-extended-support-disabled", c.EngineLifecycleSupport)
}

// ---- Automated backup response structure ----

func TestAutomatedBackup_DescribeClusterViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateDBCluster("bkp-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{})
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=DescribeDBClusterAutomatedBackups&Version=2014-10-31&DBClusterIdentifier=bkp-cluster")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeDBClusterAutomatedBackupsResponse")
}

func TestAutomatedBackup_DescribeInstanceViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=DescribeDBInstanceAutomatedBackups&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeDBInstanceAutomatedBackupsResponse")
}

// ---- ModifyDBCluster EngineLifecycleSupport via handler ----

func TestDBCluster_ModifyEngineLifecycleSupportViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateDBCluster("els-mod", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=ModifyDBCluster&Version=2014-10-31&DBClusterIdentifier=els-mod"+
			"&EngineLifecycleSupport=open-source-rds-extended-support")
	require.Equal(t, http.StatusOK, rec.Code)

	clusters, err := b.DescribeDBClusters("els-mod")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "open-source-rds-extended-support", clusters[0].EngineLifecycleSupport)
}

// ---- CEV describe pagination ----

func TestDescribeCustomDBEngineVersions_Pagination(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	for i := range 5 {
		_, err := b.CreateCustomDBEngineVersion(
			"oracle-ee",
			"19.v"+string(rune('a'+i)),
			"version "+string(rune('a'+i)),
		)
		require.NoError(t, err)
	}

	rec := postRDSForm(t, h,
		"Action=DescribeCustomDBEngineVersions&Version=2014-10-31&Engine=oracle-ee&MaxRecords=2")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "Marker", "paginated response should have a Marker")
}

// ---- Performance Insights with custom period ----

func TestPerformanceInsights_CustomPeriod(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	start := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 15, 10, 10, 0, 0, time.UTC)

	points300s := b.GetPerformanceInsightsData("res", "db.load.avg", start, end, 300)
	points60s := b.GetPerformanceInsightsData("res", "db.load.avg", start, end, 60)

	assert.Greater(t, len(points60s), len(points300s),
		"shorter period should produce more data points")
}

// ---- Recommendation seeding and all status lifecycle ----

func TestDBRecommendation_AddAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	assert.Equal(t, 0, rds.RecommendationCount(b))

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-add-1",
		TypeID:           "type-index",
		Severity:         "high",
		Status:           "active",
		Description:      "Missing index on user_id",
		ResourceARN:      "arn:aws:rds:us-east-1:123:db:mydb",
	})

	assert.Equal(t, 1, rds.RecommendationCount(b))

	recs := b.DescribeDBRecommendations("rec-add-1", "")
	require.Len(t, recs, 1)
	assert.Equal(t, "high", recs[0].Severity)
	assert.Equal(t, "Missing index on user_id", recs[0].Description)
}

func TestDBRecommendation_ModifyNotFound(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.ModifyDBRecommendation("nonexistent", "active")
	require.Error(t, err)
	require.ErrorIs(t, err, rds.ErrInvalidParameter)
}

// ---- Integration ARN format ----

func TestIntegration_ARNContainsRegionAndAccount(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("111122223333", "eu-west-1")

	intg, err := b.CreateIntegration("my-intg", "src", "tgt", "", "", "")
	require.NoError(t, err)

	assert.Contains(t, intg.IntegrationArn, "eu-west-1")
	assert.Contains(t, intg.IntegrationArn, "111122223333")
	assert.Contains(t, intg.IntegrationArn, "my-intg")
}

// ---- DBShardGroup full lifecycle via handler ----

func TestDBShardGroup_FullLifecycleViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=lifecycle-sg&DBClusterIdentifier=lifecycle-cl"+
			"&MaxACU=256&MinACU=4&ComputeRedundancy=2&PubliclyAccessible=true")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lifecycle-sg")
	assert.Contains(t, rec.Body.String(), "<Endpoint>")

	rec = postRDSForm(t, h,
		"Action=ModifyDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=lifecycle-sg&MaxACU=512")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "512")

	rec = postRDSForm(t, h,
		"Action=DescribeDBShardGroups&Version=2014-10-31&DBShardGroupIdentifier=lifecycle-sg")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<MaxACU>512</MaxACU>")
	assert.Contains(t, rec.Body.String(), "<MinACU>4</MinACU>")

	rec = postRDSForm(t, h,
		"Action=RebootDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=lifecycle-sg")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h,
		"Action=DeleteDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=lifecycle-sg")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, rds.ShardGroupCount(newBatch3Backend()))
}

// ---- DBRecommendation via handler ----

func TestDBRecommendation_ModifyViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-handler",
		Status:           "active",
		Description:      "test",
	})

	rec := postRDSForm(t, h,
		"Action=ModifyDBRecommendation&Version=2014-10-31"+
			"&RecommendationId=rec-handler&Status=paused")
	require.Equal(t, http.StatusOK, rec.Code)

	recs := b.DescribeDBRecommendations("rec-handler", "")
	require.Len(t, recs, 1)
	assert.Equal(t, "paused", recs[0].Status)
}

func TestDBRecommendation_DescribeViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-desc-handler",
		Status:           "active",
		Severity:         "medium",
		Description:      "Optimize checkpoint interval",
	})

	rec := postRDSForm(t, h,
		"Action=DescribeDBRecommendations&Version=2014-10-31&RecommendationId=rec-desc-handler")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "rec-desc-handler")
	assert.Contains(t, respStr, "Optimize checkpoint interval")
}

// ---- New cluster fields ModifyDBCluster persistence ----

func TestDBCluster_ModifyOptimizedWritesViaBackend(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBCluster("ow-mod", "aurora-mysql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	clusters, err := b.DescribeDBClusters("ow-mod")
	require.NoError(t, err)
	assert.False(t, clusters[0].OptimizedWrites, "OptimizedWrites should be false by default")

	_, err = b.ModifyDBCluster("ow-mod", "", rds.DBClusterOptions{OptimizedWrites: true})
	require.NoError(t, err)

	clusters, err = b.DescribeDBClusters("ow-mod")
	require.NoError(t, err)
	assert.True(t, clusters[0].OptimizedWrites, "OptimizedWrites should be set after modify")
}

// ---- SDK completeness still passes ----

func TestBatch3_SDKCompleteness(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	h := rds.NewHandler(b)

	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "DescribeCustomDBEngineVersions")
	assert.Contains(t, ops, "GetPerformanceInsightsMetrics")
	assert.Contains(t, ops, "CreateBlueGreenDeployment")
	assert.Contains(t, ops, "CreateDBShardGroup")
	assert.Contains(t, ops, "CreateIntegration")
}
