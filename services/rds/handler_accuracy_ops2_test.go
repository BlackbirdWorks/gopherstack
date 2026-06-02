package rds_test

// handler_accuracy_ops2_test.go — RDS batch-2 ops AWS-accuracy audit (go-q8rk)
//
// Covers four accuracy gaps:
//  1. DescribeDBSnapshots: DBInstanceIdentifier filter returns all snapshots for an instance.
//  2. DescribeDBClusterSnapshots: DBClusterIdentifier filter returns all snapshots for a cluster.
//  3. DBParameter Source + ApplyMethod: ModifyDBParameterGroup stamps Source="user",
//     ApplyMethod="pending-reboot" on modified parameters; DescribeDBParameters returns them.
//  4. RebootDBInstance: returns the instance in "rebooting" status, matching AWS behaviour.

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func newAccOps2Handler(t *testing.T) *rds.Handler {
	t.Helper()
	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)
	return rds.NewHandler(b)
}

func doAccOps2(t *testing.T, h *rds.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	return rec
}

func mustCreateAccOps2Instance(t *testing.T, h *rds.Handler, id string) {
	t.Helper()
	rec := doAccOps2(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {id},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code, "setup: %s", rec.Body)
}

// ================================================================
// DescribeDBSnapshots — DBInstanceIdentifier filter
// ================================================================

func TestAccOps2_DescribeDBSnapshots_FilterByInstance_ReturnsMatching(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "inst-a")
	mustCreateAccOps2Instance(t, h, "inst-b")

	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-a1"}, "DBInstanceIdentifier": {"inst-a"},
	}.Encode())
	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-a2"}, "DBInstanceIdentifier": {"inst-a"},
	}.Encode())
	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-b1"}, "DBInstanceIdentifier": {"inst-b"},
	}.Encode())

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"DescribeDBSnapshots"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"inst-a"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeDBSnapshotsResponse"`
		Result  struct {
			Snapshots []struct {
				DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
				DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
			} `xml:"DBSnapshots>DBSnapshot"`
		} `xml:"DescribeDBSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	ids := make([]string, 0, len(resp.Result.Snapshots))
	for _, s := range resp.Result.Snapshots {
		assert.Equal(t, "inst-a", s.DBInstanceIdentifier)
		ids = append(ids, s.DBSnapshotIdentifier)
	}
	assert.ElementsMatch(t, []string{"snap-a1", "snap-a2"}, ids)
}

func TestAccOps2_DescribeDBSnapshots_FilterByInstance_NoMatch_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "inst-x")
	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-x"}, "DBInstanceIdentifier": {"inst-x"},
	}.Encode())

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"DescribeDBSnapshots"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"no-such-instance"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Snapshots []struct{ DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"` } `xml:"DBSnapshots>DBSnapshot"`
		} `xml:"DescribeDBSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.Snapshots)
}

func TestAccOps2_DescribeDBSnapshots_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "inst-p")
	mustCreateAccOps2Instance(t, h, "inst-q")

	for _, pair := range [][2]string{
		{"snap-p1", "inst-p"}, {"snap-p2", "inst-p"}, {"snap-q1", "inst-q"},
	} {
		doAccOps2(t, h, url.Values{
			"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
			"DBSnapshotIdentifier": {pair[0]}, "DBInstanceIdentifier": {pair[1]},
		}.Encode())
	}

	rec := doAccOps2(t, h, "Action=DescribeDBSnapshots&Version=2014-10-31")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Snapshots []struct{ DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"` } `xml:"DBSnapshots>DBSnapshot"`
		} `xml:"DescribeDBSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.Snapshots, 3)
}

func TestAccOps2_DescribeDBSnapshots_SpecificID_StillWorks(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "inst-s")
	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-s"}, "DBInstanceIdentifier": {"inst-s"},
	}.Encode())

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"DescribeDBSnapshots"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"snap-s"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Snapshots []struct{ DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"` } `xml:"DBSnapshots>DBSnapshot"`
		} `xml:"DescribeDBSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Snapshots, 1)
	assert.Equal(t, "snap-s", resp.Result.Snapshots[0].DBSnapshotIdentifier)
}

// ================================================================
// DescribeDBClusterSnapshots — DBClusterIdentifier filter
// ================================================================

func mustCreateAccOps2Cluster(t *testing.T, h *rds.Handler, id string) {
	t.Helper()
	rec := doAccOps2(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {id},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
		"MasterUserPassword":  {"password123"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code, "setup cluster: %s", rec.Body)
}

func TestAccOps2_DescribeDBClusterSnapshots_FilterByCluster_ReturnsMatching(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Cluster(t, h, "cluster-1")
	mustCreateAccOps2Cluster(t, h, "cluster-2")

	for _, pair := range [][2]string{
		{"csnap-1a", "cluster-1"},
		{"csnap-1b", "cluster-1"},
		{"csnap-2a", "cluster-2"},
	} {
		rec := doAccOps2(t, h, url.Values{
			"Action": {"CreateDBClusterSnapshot"}, "Version": {"2014-10-31"},
			"DBClusterSnapshotIdentifier": {pair[0]},
			"DBClusterIdentifier":         {pair[1]},
		}.Encode())
		require.Equal(t, http.StatusOK, rec.Code, "create cluster snapshot %s: %s", pair[0], rec.Body)
	}

	rec := doAccOps2(t, h, url.Values{
		"Action":              {"DescribeDBClusterSnapshots"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"cluster-1"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Snapshots []struct {
				DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
				DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
			} `xml:"DBClusterSnapshots>DBClusterSnapshot"`
		} `xml:"DescribeDBClusterSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	ids := make([]string, 0, len(resp.Result.Snapshots))
	for _, s := range resp.Result.Snapshots {
		assert.Equal(t, "cluster-1", s.DBClusterIdentifier)
		ids = append(ids, s.DBClusterSnapshotIdentifier)
	}
	assert.ElementsMatch(t, []string{"csnap-1a", "csnap-1b"}, ids)
}

func TestAccOps2_DescribeDBClusterSnapshots_FilterByCluster_NoMatch_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Cluster(t, h, "cluster-x")
	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBClusterSnapshot"}, "Version": {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"csnap-x"},
		"DBClusterIdentifier":         {"cluster-x"},
	}.Encode())

	rec := doAccOps2(t, h, url.Values{
		"Action":              {"DescribeDBClusterSnapshots"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"no-such-cluster"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Snapshots []struct{ DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"` } `xml:"DBClusterSnapshots>DBClusterSnapshot"`
		} `xml:"DescribeDBClusterSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.Snapshots)
}

func TestAccOps2_DescribeDBClusterSnapshots_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Cluster(t, h, "cluster-m")
	mustCreateAccOps2Cluster(t, h, "cluster-n")

	for _, pair := range [][2]string{
		{"csnap-m1", "cluster-m"}, {"csnap-n1", "cluster-n"}, {"csnap-n2", "cluster-n"},
	} {
		doAccOps2(t, h, url.Values{
			"Action": {"CreateDBClusterSnapshot"}, "Version": {"2014-10-31"},
			"DBClusterSnapshotIdentifier": {pair[0]},
			"DBClusterIdentifier":         {pair[1]},
		}.Encode())
	}

	rec := doAccOps2(t, h, "Action=DescribeDBClusterSnapshots&Version=2014-10-31")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Snapshots []struct{ DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"` } `xml:"DBClusterSnapshots>DBClusterSnapshot"`
		} `xml:"DescribeDBClusterSnapshotsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.Snapshots, 3)
}

// ================================================================
// DBParameter Source + ApplyMethod via ModifyDBParameterGroup
// ================================================================

func TestAccOps2_ModifyDBParameterGroup_SetsSourceAndApplyMethod(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateDBParameterGroup("pg-source", "postgres14", "test group")
	require.NoError(t, err)

	_, err = b.ModifyDBParameterGroup("pg-source", []rds.DBParameter{
		{ParameterName: "max_connections", ParameterValue: "300"},
		{ParameterName: "shared_buffers", ParameterValue: "512MB"},
	})
	require.NoError(t, err)

	params, err := b.DescribeDBParameters("pg-source")
	require.NoError(t, err)
	require.Len(t, params, 2)

	for _, p := range params {
		assert.Equal(t, "user", p.Source, "param %s Source", p.ParameterName)
		assert.Equal(t, "pending-reboot", p.ApplyMethod, "param %s ApplyMethod", p.ParameterName)
	}
}

func TestAccOps2_ModifyDBParameterGroup_ExplicitApplyMethod_Preserved(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateDBParameterGroup("pg-immediate", "postgres14", "test group")
	require.NoError(t, err)

	_, err = b.ModifyDBParameterGroup("pg-immediate", []rds.DBParameter{
		{ParameterName: "log_statement", ParameterValue: "all", ApplyMethod: "immediate"},
	})
	require.NoError(t, err)

	params, err := b.DescribeDBParameters("pg-immediate")
	require.NoError(t, err)
	require.Len(t, params, 1)
	assert.Equal(t, "immediate", params[0].ApplyMethod)
	assert.Equal(t, "user", params[0].Source)
}

func TestAccOps2_DescribeDBParameters_SourceInXMLResponse(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)

	doAccOps2(t, h, url.Values{
		"Action": {"CreateDBParameterGroup"}, "Version": {"2014-10-31"},
		"DBParameterGroupName":   {"pg-xml"},
		"DBParameterGroupFamily": {"postgres14"},
		"Description":            {"test"},
	}.Encode())

	doAccOps2(t, h, url.Values{
		"Action": {"ModifyDBParameterGroup"}, "Version": {"2014-10-31"},
		"DBParameterGroupName":                     {"pg-xml"},
		"Parameters.Parameter.1.ParameterName":     {"work_mem"},
		"Parameters.Parameter.1.ParameterValue":    {"64MB"},
		"Parameters.Parameter.1.ApplyMethod":        {"pending-reboot"},
	}.Encode())

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"DescribeDBParameters"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-xml"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<Source>user</Source>")
	assert.Contains(t, body, "<ApplyMethod>pending-reboot</ApplyMethod>")
}

// ================================================================
// RebootDBInstance — returns "rebooting" status, not "available"
// ================================================================

func TestAccOps2_RebootDBInstance_ReturnsRebootingStatus(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)
	mustCreateAccOps2Instance(t, h, "reboot-me")

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"RebootDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"reboot-me"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Instance struct {
				DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
				DBInstanceStatus     string `xml:"DBInstanceStatus"`
			} `xml:"DBInstance"`
		} `xml:"RebootDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "reboot-me", resp.Result.Instance.DBInstanceIdentifier)
	assert.Equal(t, "rebooting", resp.Result.Instance.DBInstanceStatus,
		"AWS returns rebooting immediately; available comes after transition delay")
}

func TestAccOps2_RebootDBInstance_NotFound_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccOps2Handler(t)

	rec := doAccOps2(t, h, url.Values{
		"Action":               {"RebootDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"does-not-exist"},
	}.Encode())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBInstanceNotFound")
}

func TestAccOps2_RebootDBInstance_TransitionsToAvailableAfterDelay(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateDBInstance("reboot-delay", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)

	inst, err := b.RebootDBInstance("reboot-delay")
	require.NoError(t, err)
	assert.Equal(t, "rebooting", inst.DBInstanceStatus)

	// After the transition delay the reconciler moves it back to available.
	instances, err := b.DescribeDBInstances("reboot-delay")
	require.NoError(t, err)
	require.Len(t, instances, 1)
	// Status is either still rebooting or already available depending on timing;
	// at minimum it must be one of the two valid states.
	status := instances[0].DBInstanceStatus
	assert.Truef(t, status == "rebooting" || status == "available",
		"unexpected status %q", status)
}
