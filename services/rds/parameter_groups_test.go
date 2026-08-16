package rds_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestHandler_DescribeDBParameterGroups_Pagination verifies that
// DescribeDBParameterGroups honours MaxRecords and returns a Marker that pages through
// the full set exactly once with no overlap.
func TestHandler_DescribeDBParameterGroups_Pagination(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	const total = 5
	for i := range total {
		rec := postRDSForm(t, h, fmt.Sprintf(
			"Action=CreateDBParameterGroup&Version=2014-10-31"+
				"&DBParameterGroupName=pg-%02d&DBParameterGroupFamily=postgres15"+
				"&Description=test", i,
		))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// First page: MaxRecords=2 must return a Marker.
	rec := postRDSForm(t, h, "Action=DescribeDBParameterGroups&Version=2014-10-31&MaxRecords=2")
	require.Equal(t, http.StatusOK, rec.Code)

	type pgResp struct {
		Marker string `xml:"DescribeDBParameterGroupsResult>Marker"`
		Groups []struct {
			Name string `xml:"DBParameterGroupName"`
		} `xml:"DescribeDBParameterGroupsResult>DBParameterGroups>DBParameterGroup"`
	}

	seen := map[string]int{}
	marker := ""
	pages := 0

	for {
		q := "Action=DescribeDBParameterGroups&Version=2014-10-31&MaxRecords=2"
		if marker != "" {
			q += "&Marker=" + marker
		}

		pageRec := postRDSForm(t, h, q)
		require.Equal(t, http.StatusOK, pageRec.Code)

		var resp pgResp
		require.NoError(t, xml.Unmarshal(pageRec.Body.Bytes(), &resp))
		require.LessOrEqual(t, len(resp.Groups), 2)

		for _, g := range resp.Groups {
			seen[g.Name]++
		}

		pages++
		if resp.Marker == "" {
			break
		}
		marker = resp.Marker
		require.Less(t, pages, 10, "pagination did not terminate")
	}

	// 5 user groups + the default parameter group are returned exactly once each.
	require.GreaterOrEqual(t, len(seen), total)
	for name, count := range seen {
		assert.Equalf(t, 1, count, "group %s appeared on more than one page", name)
	}
	assert.Greater(t, pages, 1, "expected multiple pages")
}

func TestDBParameterGroup_ResetAll(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBParameterGroup("pg1", "postgres14", "test group")
	require.NoError(t, err)

	params := []rds.DBParameter{
		{ParameterName: "max_connections", ParameterValue: "200"},
		{ParameterName: "shared_buffers", ParameterValue: "256MB"},
	}
	_, err = b.ModifyDBParameterGroup("pg1", params)
	require.NoError(t, err)

	got, err := b.DescribeDBParameters("pg1")
	require.NoError(t, err)
	require.Len(t, got, 2)

	_, err = b.ResetDBParameterGroup("pg1", true, nil)
	require.NoError(t, err)

	gotAfter, err := b.DescribeDBParameters("pg1")
	require.NoError(t, err)
	require.Len(t, gotAfter, 2)
	for _, p := range gotAfter {
		assert.Empty(
			t,
			p.ParameterValue,
			"reset should clear parameter value for %s",
			p.ParameterName,
		)
	}
}

func TestDBParameterGroup_ResetSpecific(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBParameterGroup("pg2", "postgres14", "test group")
	require.NoError(t, err)

	params := []rds.DBParameter{
		{ParameterName: "max_connections", ParameterValue: "200"},
		{ParameterName: "shared_buffers", ParameterValue: "256MB"},
	}
	_, err = b.ModifyDBParameterGroup("pg2", params)
	require.NoError(t, err)

	_, err = b.ResetDBParameterGroup("pg2", false, []string{"max_connections"})
	require.NoError(t, err)

	got, err := b.DescribeDBParameters("pg2")
	require.NoError(t, err)
	require.Len(t, got, 2)
	paramMap := make(map[string]string)
	for _, p := range got {
		paramMap[p.ParameterName] = p.ParameterValue
	}
	assert.Empty(t, paramMap["max_connections"], "reset should clear max_connections value")
	assert.Equal(t, "256MB", paramMap["shared_buffers"], "shared_buffers should be unchanged")
}

func TestDBParameterGroup_NotFoundErrors(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.DescribeDBParameterGroups("noexist")
	require.ErrorIs(t, err, rds.ErrParameterGroupNotFound)

	err = b.DeleteDBParameterGroup("noexist")
	require.ErrorIs(t, err, rds.ErrParameterGroupNotFound)

	_, err = b.ModifyDBParameterGroup("noexist", nil)
	require.ErrorIs(t, err, rds.ErrParameterGroupNotFound)

	_, err = b.DescribeDBParameters("noexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrParameterGroupNotFound)
}

// TestDBParameterGroup_Duplicate covers the exact-duplicate-identifier case,
// plus (DBParameterGroupName is a case-insensitive AWS identifier, matching
// real AWS, which lower-cases identifiers internally) the case-varying
// duplicate collisions and the corresponding case-insensitive
// Describe/Delete lookups.
func TestDBParameterGroup_Duplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		setupID   string
		actionID  string
		action    string
		wantErr   bool
	}{
		{
			name:      "exact_duplicate_collides",
			setupID:   "dup-pg",
			actionID:  "dup-pg",
			action:    "create",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupAlreadyExists,
		},
		{
			name:      "lowercased_duplicate_collides",
			setupID:   "MyParamGroup",
			actionID:  "myparamgroup",
			action:    "create",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupAlreadyExists,
		},
		{
			name:      "uppercased_duplicate_collides",
			setupID:   "lower-pg",
			actionID:  "LOWER-PG",
			action:    "create",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupAlreadyExists,
		},
		{
			name:     "distinct_id_does_not_collide",
			setupID:  "some-pg",
			actionID: "some-other-pg",
			action:   "create",
		},
		{
			name:     "describe_finds_resource_under_different_case",
			setupID:  "FindMe-PG",
			actionID: "findme-pg",
			action:   "describe",
		},
		{
			name:     "delete_removes_resource_under_different_case",
			setupID:  "DeleteMe-PG",
			actionID: "deleteme-pg",
			action:   "delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBatch2Backend()
			_, err := b.CreateDBParameterGroup(tt.setupID, "mysql8.0", "first")
			require.NoError(t, err)

			switch tt.action {
			case "create":
				_, err = b.CreateDBParameterGroup(tt.actionID, "mysql8.0", "second")
			case "describe":
				var pgs []rds.DBParameterGroup
				pgs, err = b.DescribeDBParameterGroups(tt.actionID)
				if err == nil {
					require.Len(t, pgs, 1)
					// The wire response must keep echoing the ORIGINAL
					// caller-supplied casing from creation time.
					assert.Equal(t, tt.setupID, pgs[0].DBParameterGroupName)
				}
			case "delete":
				err = b.DeleteDBParameterGroup(tt.actionID)
				if err == nil {
					_, describeErr := b.DescribeDBParameterGroups(tt.setupID)
					require.ErrorIs(t, describeErr, rds.ErrParameterGroupNotFound)
				}
			}

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDBParameterGroup_CopyPreservesParams(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBParameterGroup("src-pg", "postgres14", "src")
	require.NoError(t, err)

	params := []rds.DBParameter{
		{ParameterName: "work_mem", ParameterValue: "64MB"},
	}
	_, err = b.ModifyDBParameterGroup("src-pg", params)
	require.NoError(t, err)

	_, err = b.CopyDBParameterGroup("src-pg", "dst-pg", "copy of src")
	require.NoError(t, err)

	dstParams, err := b.DescribeDBParameters("dst-pg")
	require.NoError(t, err)
	require.Len(t, dstParams, 1)
	assert.Equal(t, "work_mem", dstParams[0].ParameterName)
	assert.Equal(t, "64MB", dstParams[0].ParameterValue)
}

func TestDBParameterGroup_HTTP_CRUD(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"http-pg"},
		"DBParameterGroupFamily": {"postgres14"},
		"Description":            {"http test"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-pg")

	rec = postRDSForm(t, h, url.Values{
		"Action":               {"DescribeDBParameterGroups"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"http-pg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-pg")

	rec = postRDSForm(t, h, url.Values{
		"Action":                                {"ModifyDBParameterGroup"},
		"Version":                               {"2014-10-31"},
		"DBParameterGroupName":                  {"http-pg"},
		"Parameters.Parameter.1.ParameterName":  {"max_connections"},
		"Parameters.Parameter.1.ParameterValue": {"300"},
		"Parameters.Parameter.1.ApplyMethod":    {"pending-reboot"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":               {"DescribeDBParameters"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"http-pg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "max_connections")

	rec = postRDSForm(t, h, url.Values{
		"Action":               {"ResetDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"http-pg"},
		"ResetAllParameters":   {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":               {"DeleteDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"http-pg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestConcurrent_DBParameterGroup(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	for i := range 5 {
		_, err := b.CreateDBParameterGroup(fmt.Sprintf("pg%d", i), "postgres14", "test")
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("pg%d", n)
			_, _ = b.ModifyDBParameterGroup(name, []rds.DBParameter{
				{ParameterName: "max_connections", ParameterValue: strconv.Itoa(100 + n)},
			})
			_, _ = b.DescribeDBParameters(name)
		}(i)
	}
	wg.Wait()
}

func TestModifyDBParameterGroup_SetsSourceAndApplyMethod(t *testing.T) {
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

func TestModifyDBParameterGroup_ExplicitApplyMethod_Preserved(t *testing.T) {
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

func TestDescribeDBParameters_SourceInXMLResponse(t *testing.T) {
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
		"DBParameterGroupName":                  {"pg-xml"},
		"Parameters.Parameter.1.ParameterName":  {"work_mem"},
		"Parameters.Parameter.1.ParameterValue": {"64MB"},
		"Parameters.Parameter.1.ApplyMethod":    {"pending-reboot"},
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

// TestRDSBackend_CopyDBParameterGroup tests CopyDBParameterGroup.
func TestRDSBackend_CopyDBParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		source      string
		target      string
		description string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBParameterGroup("src-pg", "postgres14", "Source group")
			},
			source:      "src-pg",
			target:      "dst-pg",
			description: "Copied group",
		},
		{
			name:      "source_not_found",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "no-such-pg",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupNotFound,
		},
		{
			name: "target_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBParameterGroup("src-pg", "postgres14", "Source")
				_, _ = b.CreateDBParameterGroup("dst-pg", "postgres14", "Dest")
			},
			source:    "src-pg",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrParameterGroupAlreadyExists,
		},
		{
			name:      "empty_source",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "",
			target:    "dst-pg",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			pg, err := b.CopyDBParameterGroup(tt.source, tt.target, tt.description)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.target, pg.DBParameterGroupName)
		})
	}
}

func TestDescribeEngineDefaultParameters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		family string
	}{
		{name: "mysql family", family: "mysql8.0"},
		{name: "empty family", family: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeEngineDefaultParameters(tt.family)
			assert.NotNil(t, got)

			h := rds.NewHandler(b)
			rec := postRDSForm(t, h, "Action=DescribeEngineDefaultParameters&Version=2014-10-31"+
				"&DBParameterGroupFamily="+url.QueryEscape(tt.family))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp struct {
				Result struct {
					DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
				} `xml:"DescribeEngineDefaultParametersResult>EngineDefaults"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.family, resp.Result.DBParameterGroupFamily,
				"DescribeEngineDefaultParameters must echo the requested family")
		})
	}
}

// TestDescribeDBParameterGroupsSorted verifies sorted order.
func TestDescribeDBParameterGroupsSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	for _, name := range []string{"pg-z", "pg-a", "pg-m"} {
		_, err := b.CreateDBParameterGroup(name, "postgres15", "desc")
		require.NoError(t, err)
	}
	got, err := b.DescribeDBParameterGroups("")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(got), 3)
	names := make([]string, len(got))
	for i, pg := range got {
		names[i] = pg.DBParameterGroupName
	}
	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i], "parameter groups should be sorted alphabetically")
	}
}
