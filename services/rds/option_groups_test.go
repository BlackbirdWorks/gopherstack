package rds_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateDBSnapshotOptionGroupName verifies OptionGroupName is copied to snapshot.
func TestCreateDBSnapshotOptionGroupName(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Create option group.
	doAccuracyRDS(t, h, url.Values{
		"Action":                 {"CreateOptionGroup"},
		"Version":                {"2014-10-31"},
		"OptionGroupName":        {"myog"},
		"EngineName":             {"postgres"},
		"MajorEngineVersion":     {"14"},
		"OptionGroupDescription": {"test"},
	})

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"og-inst"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"OptionGroupName":      {"myog"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"og-snap"},
		"DBInstanceIdentifier": {"og-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBSnapshot struct {
				OptionGroupName string `xml:"OptionGroupName"`
			} `xml:"DBSnapshot"`
		} `xml:"CreateDBSnapshotResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "myog", resp.Result.DBSnapshot.OptionGroupName)
}

func TestBatch2_OptionGroup_ModifyAddRemove(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateOptionGroup("og1", "mysql", "8.0", "test og")
	require.NoError(t, err)

	toAdd := []rds.OptionGroupOption{
		{OptionName: "MEMCACHED"},
		{OptionName: "MARIADB_AUDIT_PLUGIN"},
	}
	og, err := b.ModifyOptionGroup("og1", toAdd, nil)
	require.NoError(t, err)
	assert.Len(t, og.Options, 2)

	og, err = b.ModifyOptionGroup("og1", nil, []string{"MEMCACHED"})
	require.NoError(t, err)
	assert.Len(t, og.Options, 1)
	assert.Equal(t, "MARIADB_AUDIT_PLUGIN", og.Options[0].OptionName)
}

func TestBatch2_OptionGroup_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateOptionGroup("dup-og", "mysql", "8.0", "first")
	require.NoError(t, err)

	_, err = b.CreateOptionGroup("dup-og", "mysql", "8.0", "second")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrOptionGroupAlreadyExists)
}

func TestBatch2_OptionGroup_DeleteNotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	err := b.DeleteOptionGroup("noexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrOptionGroupNotFound)
}

func TestBatch2_OptionGroup_CopyPreservesOptions(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateOptionGroup("src-og", "mysql", "8.0", "source")
	require.NoError(t, err)

	_, err = b.ModifyOptionGroup("src-og", []rds.OptionGroupOption{{OptionName: "SSL"}}, nil)
	require.NoError(t, err)

	_, err = b.CopyOptionGroup("src-og", "dst-og", "destination")
	require.NoError(t, err)

	groups, err := b.DescribeOptionGroups("dst-og")
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Len(t, groups[0].Options, 1)
	assert.Equal(t, "SSL", groups[0].Options[0].OptionName)
}

func TestBatch2_OptionGroup_Concurrent(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateOptionGroup("conc-og", "mysql", "8.0", "concurrent test")
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, _ = b.ModifyOptionGroup("conc-og", []rds.OptionGroupOption{
				{OptionName: fmt.Sprintf("OPT%d", n)},
			}, nil)
		}(i)
	}
	wg.Wait()

	groups, err := b.DescribeOptionGroups("conc-og")
	require.NoError(t, err)
	assert.NotEmpty(t, groups[0].Options)
}

func TestBatch2_OptionGroup_HTTP_CRUD(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                 {"CreateOptionGroup"},
		"Version":                {"2014-10-31"},
		"OptionGroupName":        {"http-og"},
		"EngineName":             {"mysql"},
		"MajorEngineVersion":     {"8.0"},
		"OptionGroupDescription": {"http test og"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":          {"ModifyOptionGroup"},
		"Version":         {"2014-10-31"},
		"OptionGroupName": {"http-og"},
		"OptionsToInclude.OptionConfiguration.1.OptionName": {"SSL"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":          {"DescribeOptionGroups"},
		"Version":         {"2014-10-31"},
		"OptionGroupName": {"http-og"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-og")

	rec = postRDSForm(t, h, url.Values{
		"Action":          {"DeleteOptionGroup"},
		"Version":         {"2014-10-31"},
		"OptionGroupName": {"http-og"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRDS_OptionGroups(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	// CreateOptionGroup
	rec := postRDSForm(t, h, url.Values{
		"Action":                 []string{"CreateOptionGroup"},
		"OptionGroupName":        []string{"my-option-group"},
		"EngineName":             []string{"mysql"},
		"MajorEngineVersion":     []string{"8.0"},
		"OptionGroupDescription": []string{"My option group"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DescribeOptionGroups
	rec = postRDSForm(t, h, url.Values{
		"Action":          []string{"DescribeOptionGroups"},
		"OptionGroupName": []string{"my-option-group"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DeleteOptionGroup
	rec = postRDSForm(t, h, url.Values{
		"Action":          []string{"DeleteOptionGroup"},
		"OptionGroupName": []string{"my-option-group"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}

// TestRDSBackend_CopyOptionGroup tests CopyOptionGroup.
func TestRDSBackend_CopyOptionGroup(t *testing.T) {
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
				_, _ = b.CreateOptionGroup("src-og", "mysql", "8.0", "Source group")
			},
			source:      "src-og",
			target:      "dst-og",
			description: "Copied group",
		},
		{
			name: "inherits_description",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateOptionGroup("src-og", "mysql", "8.0", "Original desc")
			},
			source: "src-og",
			target: "dst-og",
		},
		{
			name:      "source_not_found",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "no-such-og",
			target:    "dst-og",
			wantErr:   true,
			wantErrIs: rds.ErrOptionGroupNotFound,
		},
		{
			name: "target_already_exists",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateOptionGroup("src-og", "mysql", "8.0", "Source")
				_, _ = b.CreateOptionGroup("dst-og", "mysql", "8.0", "Dest")
			},
			source:    "src-og",
			target:    "dst-og",
			wantErr:   true,
			wantErrIs: rds.ErrOptionGroupAlreadyExists,
		},
		{
			name:      "empty_source",
			setup:     func(_ *rds.InMemoryBackend) {},
			source:    "",
			target:    "dst-og",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			og, err := b.CopyOptionGroup(tt.source, tt.target, tt.description)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.target, og.OptionGroupName)
		})
	}
}

// TestDescribeOptionGroupsSorted verifies sorted order.
func TestDescribeOptionGroupsSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	for _, name := range []string{"og-z", "og-a", "og-m"} {
		_, err := b.CreateOptionGroup(name, "mysql", "8.0", "desc")
		require.NoError(t, err)
	}
	got, err := b.DescribeOptionGroups("")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(got), 3)
	names := make([]string, len(got))
	for i, og := range got {
		names[i] = og.OptionGroupName
	}
	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i], "option groups should be sorted alphabetically")
	}
}
