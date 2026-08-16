package rds_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestCreateDBShardGroup_WireShapeIsFlat verifies CreateDBShardGroupOutput's members sit
// directly under <CreateDBShardGroupResult>, matching the real (flat) aws-sdk-go-v2 shape —
// there is no nested <DBShardGroup> wrapper element (unlike e.g. CreateDBInstanceOutput,
// which does nest under <DBInstance>). A client using the real SDK deserializer would get
// back an empty struct if gopherstack wrapped these fields one level too deep, since the
// deserializer only looks for named fields as direct children of the Result element.
func TestCreateDBShardGroup_WireShapeIsFlat(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                 {"CreateDBShardGroup"},
		"Version":                {"2014-10-31"},
		"DBShardGroupIdentifier": {"wire-sg"},
		"DBClusterIdentifier":    {"wire-cluster"},
		"MaxACU":                 {"128"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			DBShardGroupIdentifier string `xml:"DBShardGroupIdentifier"`
			DBClusterIdentifier    string `xml:"DBClusterIdentifier"`
		} `xml:"CreateDBShardGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "wire-sg", resp.Result.DBShardGroupIdentifier)
	assert.Equal(t, "wire-cluster", resp.Result.DBClusterIdentifier)
}

func TestCreateDBShardGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		id        string
		clusterID string
		wantErr   bool
	}{
		{name: "success", id: "sg-1", clusterID: "cluster-1"},
		{
			name:      "empty id",
			id:        "",
			clusterID: "cluster-1",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty cluster",
			id:        "sg-2",
			clusterID: "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "duplicate",
			id:        "sg-dup",
			clusterID: "cluster-1",
			wantErr:   true,
			wantErrIs: rds.ErrDBShardGroupAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)

			if tt.name == "duplicate" {
				_, err := b.CreateDBShardGroup(tt.id, tt.clusterID, 128, 0, 0, false)
				require.NoError(t, err)
			}

			sg, err := b.CreateDBShardGroup(tt.id, tt.clusterID, 128, 0, 0, false)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.id, sg.DBShardGroupIdentifier)
			assert.Equal(t, tt.clusterID, sg.DBClusterIdentifier)
			assert.Equal(t, "available", sg.Status)
		})
	}
}

func TestDeleteDBShardGroup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateDBShardGroup("sg-del", "cluster-1", 64, 0, 0, false)
		require.NoError(t, err)

		sg, err := b.DeleteDBShardGroup("sg-del")
		require.NoError(t, err)
		assert.Equal(t, "sg-del", sg.DBShardGroupIdentifier)
		assert.Equal(t, "deleting", sg.Status)

		// Confirm removed
		groups, err := b.DescribeDBShardGroups("sg-del")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
		assert.Empty(t, groups)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DeleteDBShardGroup("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
	})
}

func TestDescribeDBShardGroups(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		groups, err := b.DescribeDBShardGroups("")
		require.NoError(t, err)
		assert.Empty(t, groups)
	})

	t.Run("lists all", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		for _, id := range []string{"sg-z", "sg-a", "sg-m"} {
			_, err := b.CreateDBShardGroup(id, "cluster-1", 64, 0, 0, false)
			require.NoError(t, err)
		}
		groups, err := b.DescribeDBShardGroups("")
		require.NoError(t, err)
		require.Len(t, groups, 3)
		// verify sorted
		assert.Equal(t, "sg-a", groups[0].DBShardGroupIdentifier)
		assert.Equal(t, "sg-m", groups[1].DBShardGroupIdentifier)
		assert.Equal(t, "sg-z", groups[2].DBShardGroupIdentifier)
	})

	t.Run("filtered", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateDBShardGroup("sg-x", "cluster-1", 64, 0, 0, false)
		require.NoError(t, err)
		_, err = b.CreateDBShardGroup("sg-y", "cluster-2", 128, 0, 0, false)
		require.NoError(t, err)

		groups, err := b.DescribeDBShardGroups("sg-x")
		require.NoError(t, err)
		require.Len(t, groups, 1)
		assert.Equal(t, "sg-x", groups[0].DBShardGroupIdentifier)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DescribeDBShardGroups("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
	})
}

func TestModifyDBShardGroup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateDBShardGroup("sg-mod", "cluster-1", 64, 0, 0, false)
		require.NoError(t, err)

		sg, err := b.ModifyDBShardGroup("sg-mod", 256, 1)
		require.NoError(t, err)
		assert.InEpsilon(t, 256.0, sg.MaxACU, 0.001)
		assert.Equal(t, 1, sg.ComputeRedundancy)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.ModifyDBShardGroup("missing", 128, 0)
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
	})
}

func TestRebootDBShardGroup(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateDBShardGroup("sg-reboot", "cluster-1", 64, 0, 0, false)
		require.NoError(t, err)

		sg, err := b.RebootDBShardGroup("sg-reboot")
		require.NoError(t, err)
		assert.Equal(t, "rebooting", sg.Status)

		// After reboot the stored state should be available
		groups, err := b.DescribeDBShardGroups("sg-reboot")
		require.NoError(t, err)
		assert.Equal(t, "available", groups[0].Status)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.RebootDBShardGroup("missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrDBShardGroupNotFound)
	})
}

func TestHandler_DBShardGroupCRUD(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Create
	rec := postRDSForm(t, h,
		"Action=CreateDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=sg-1&DBClusterIdentifier=cluster-1&MaxACU=128")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "sg-1")

	// Describe all
	rec = postRDSForm(t, h, "Action=DescribeDBShardGroups&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "sg-1")

	// Describe filtered
	rec = postRDSForm(
		t,
		h,
		"Action=DescribeDBShardGroups&Version=2014-10-31&DBShardGroupIdentifier=sg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "sg-1")

	// Modify
	rec = postRDSForm(
		t,
		h,
		"Action=ModifyDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=sg-1&MaxACU=256",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Reboot
	rec = postRDSForm(
		t,
		h,
		"Action=RebootDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=sg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRDSForm(
		t,
		h,
		"Action=DeleteDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=sg-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm gone
	rec = postRDSForm(
		t,
		h,
		"Action=DescribeDBShardGroups&Version=2014-10-31&DBShardGroupIdentifier=sg-1",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DBShardGroup_DuplicateError(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	rec := postRDSForm(t, h,
		"Action=CreateDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=sg-dup&DBClusterIdentifier=cluster-1&MaxACU=64")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h,
		"Action=CreateDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=sg-dup&DBClusterIdentifier=cluster-1&MaxACU=64")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DBShardGroupAlreadyExists")
}

func TestDBShardGroup_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)

	for i := range 5 {
		_, err := b.CreateDBShardGroup(fmt.Sprintf("sg-%d", i), "cluster-1", 64, 0, 0, false)
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 30)

	for range 10 {
		wg.Go(func() {
			if _, err := b.DescribeDBShardGroups(""); err != nil {
				errs <- err
			}
		})
	}

	for i := range 10 {
		wg.Go(func() {
			if _, err := b.CreateDBShardGroup(
				fmt.Sprintf("sg-concurrent-%d", i), "cluster-1", 32, 0, 0, false,
			); err != nil {
				errs <- err
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func TestHandler_DescribeDBShardGroups_Pagination(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Create 5 shard groups
	for i := range 5 {
		rec := postRDSForm(t, h, fmt.Sprintf(
			"Action=CreateDBShardGroup&Version=2014-10-31"+
				"&DBShardGroupIdentifier=sg-%02d&DBClusterIdentifier=cluster-1&MaxACU=64", i,
		))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Paginate with MaxRecords=2
	rec := postRDSForm(t, h, "Action=DescribeDBShardGroups&Version=2014-10-31&MaxRecords=2")
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Marker")
}

func TestCreateDBShardGroup_Fields(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)

	sg, err := b.CreateDBShardGroup("sg-fields", "cluster-1", 128.5, 16.0, 2, true)
	require.NoError(t, err)
	assert.InEpsilon(t, 128.5, sg.MaxACU, 0.001)
	assert.InEpsilon(t, 16.0, sg.MinACU, 0.001)
	assert.Equal(t, 2, sg.ComputeRedundancy)
	assert.True(t, sg.PubliclyAccessible)
	assert.NotEmpty(t, sg.DBShardGroupArn, "DBShardGroupArn should be populated")
	assert.Contains(t, sg.DBShardGroupArn, "sg-fields")
	assert.NotEmpty(t, sg.DBShardGroupResourceID, "DBShardGroupResourceID should be populated")
}

func TestPersistence_ShardGroupsAndIntegrations(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.CreateDBShardGroup("shard-1", "cluster-1", 64.0, 2.0, 1, false)
	require.NoError(t, err)
	_, err = b.CreateDBShardGroup("shard-2", "cluster-1", 128.0, 4.0, 0, true)
	require.NoError(t, err)

	_, err = b.CreateIntegration(
		"intg-1",
		"arn:aws:rds:us-east-1:000:db:src",
		"arn:aws:redshift:us-east-1:000:ns:dst",
		"",
		"",
		"",
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	shards, err := b2.DescribeDBShardGroups("")
	require.NoError(t, err)
	assert.Len(t, shards, 2)

	integrations, err := b2.DescribeIntegrations("")
	require.NoError(t, err)
	assert.Len(t, integrations, 1)
	assert.Equal(t, "intg-1", integrations[0].IntegrationName)
}

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
	assert.NotEmpty(t, sg.DBShardGroupArn)
	assert.NotEmpty(t, sg.DBShardGroupResourceID)
}

// TestDBShardGroup_WireFieldsPresentOnAllOps asserts that
// DBShardGroupArn/DBShardGroupResourceId/PubliclyAccessible -- fields
// present on types.DBShardGroup and on every one of
// Create/Delete/Modify/RebootDBShardGroupOutput in the real SDK, but
// previously modeled only in this emulator's internal DBShardGroup struct
// and never serialized onto ANY of these four operations' XML responses --
// actually appear in the raw wire body, not just the backend struct. This is
// the "disguised stub" bug class from .claude/memories/parity-principles.md:
// a real-looking, correctly-populated Go value that a real SDK client would
// never see because the XML struct never carried it.
func TestDBShardGroup_WireFieldsPresentOnAllOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			// The resource is seeded (as part of test setup, below) with
			// the same CreateDBShardGroup request this case re-asserts on,
			// so Create's own wire fields are checked via seedRec rather
			// than a second (would-be AlreadyExistsFault) call here.
			name:   "modify",
			action: "ModifyDBShardGroup",
			body:   "Action=ModifyDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=wire-sg&MaxACU=128",
		},
		{
			name:   "reboot",
			action: "RebootDBShardGroup",
			body:   "Action=RebootDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=wire-sg",
		},
		{
			name:   "delete",
			action: "DeleteDBShardGroup",
			body:   "Action=DeleteDBShardGroup&Version=2014-10-31&DBShardGroupIdentifier=wire-sg",
		},
	}

	h := newRDSHandler()
	// Seed the shard group once, outside the table loop, then exercise every
	// remaining op against the SAME resource in sequence -- Modify/Reboot/
	// Delete all need it to already exist, so this family can't run each
	// case against an independent fresh backend the way the rest of this
	// file's table tests do.
	seedRec := postRDSForm(t, h,
		"Action=CreateDBShardGroup&Version=2014-10-31"+
			"&DBShardGroupIdentifier=wire-sg&DBClusterIdentifier=wire-cluster"+
			"&MaxACU=64&PubliclyAccessible=true")
	require.Equal(t, http.StatusOK, seedRec.Code)
	assertShardGroupWireFieldsPresent(t, "CreateDBShardGroup", seedRec.Body.String())

	for _, tt := range tests {
		rec := postRDSForm(t, h, tt.body)
		require.Equal(t, http.StatusOK, rec.Code, "action=%s body=%s", tt.action, rec.Body.String())
		assertShardGroupWireFieldsPresent(t, tt.action, rec.Body.String())
	}
}

func assertShardGroupWireFieldsPresent(t *testing.T, action, respBody string) {
	t.Helper()

	assert.Contains(t, respBody, "<DBShardGroupArn>", "action=%s missing DBShardGroupArn", action)
	assert.Contains(t, respBody, "<DBShardGroupResourceId>", "action=%s missing DBShardGroupResourceId", action)
	assert.Contains(t, respBody, "<PubliclyAccessible>true</PubliclyAccessible>",
		"action=%s missing PubliclyAccessible", action)
}

func TestPersistence_ShardGroupEndpoint(t *testing.T) {
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
