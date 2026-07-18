package rds_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBSecurityGroup_CRUD(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	sg, err := b.CreateDBSecurityGroup("my-sg", "legacy security group")
	require.NoError(t, err)
	assert.Equal(t, "my-sg", sg.DBSecurityGroupName)

	groups, err := b.DescribeDBSecurityGroups("my-sg")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	err = b.DeleteDBSecurityGroup("my-sg")
	require.NoError(t, err)

	_, err = b.DescribeDBSecurityGroups("my-sg")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrDBSecurityGroupNotFound)
}

func TestDBSecurityGroup_AuthorizeRevoke(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBSecurityGroup("auth-sg", "test")
	require.NoError(t, err)

	sg, err := b.AuthorizeDBSecurityGroupIngress("auth-sg", "10.0.0.0/8")
	require.NoError(t, err)
	require.Len(t, sg.IPRanges, 1)
	assert.Equal(t, "10.0.0.0/8", sg.IPRanges[0].CIDRIP)

	sg, err = b.RevokeDBSecurityGroupIngress("auth-sg", "10.0.0.0/8")
	require.NoError(t, err)
	assert.Empty(t, sg.IPRanges)
}

func TestDBSecurityGroup_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateDBSecurityGroup("dup-sg", "first")
	require.NoError(t, err)

	_, err = b.CreateDBSecurityGroup("dup-sg", "second")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrDBSecurityGroupAlreadyExists)
}

func TestDBSecurityGroup_NotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	// AuthorizeDBSecurityGroupIngress auto-creates the group (EC2-Classic behaviour)
	sg, err := b.AuthorizeDBSecurityGroupIngress("auto-created-sg", "0.0.0.0/0")
	require.NoError(t, err)
	assert.Equal(t, "auto-created-sg", sg.DBSecurityGroupName)

	err = b.DeleteDBSecurityGroup("noexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrDBSecurityGroupNotFound)
}

func TestDBSecurityGroup_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                     {"CreateDBSecurityGroup"},
		"Version":                    {"2014-10-31"},
		"DBSecurityGroupName":        {"http-legacy-sg"},
		"DBSecurityGroupDescription": {"legacy sg for test"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"DescribeDBSecurityGroups"},
		"Version":             {"2014-10-31"},
		"DBSecurityGroupName": {"http-legacy-sg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-legacy-sg")

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"AuthorizeDBSecurityGroupIngress"},
		"Version":             {"2014-10-31"},
		"DBSecurityGroupName": {"http-legacy-sg"},
		"CIDRIP":              {"192.168.0.0/16"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"RevokeDBSecurityGroupIngress"},
		"Version":             {"2014-10-31"},
		"DBSecurityGroupName": {"http-legacy-sg"},
		"CIDRIP":              {"192.168.0.0/16"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":              {"DeleteDBSecurityGroup"},
		"Version":             {"2014-10-31"},
		"DBSecurityGroupName": {"http-legacy-sg"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRDSBackend_AuthorizeDBSecurityGroupIngress tests AuthorizeDBSecurityGroupIngress.
func TestRDSBackend_AuthorizeDBSecurityGroupIngress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *rds.InMemoryBackend)
		name      string
		groupName string
		cidrIP    string
		wantErrIs error
		wantCIDRs []string
		wantErr   bool
	}{
		{
			name:      "creates_new_group",
			setup:     func(_ *rds.InMemoryBackend) {},
			groupName: "my-sg",
			cidrIP:    "10.0.0.0/8",
			wantCIDRs: []string{"10.0.0.0/8"},
		},
		{
			name: "adds_to_existing",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.AuthorizeDBSecurityGroupIngress("my-sg", "10.0.0.0/8")
			},
			groupName: "my-sg",
			cidrIP:    "192.168.0.0/16",
			wantCIDRs: []string{"10.0.0.0/8", "192.168.0.0/16"},
		},
		{
			name: "idempotent_duplicate",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.AuthorizeDBSecurityGroupIngress("my-sg", "10.0.0.0/8")
			},
			groupName: "my-sg",
			cidrIP:    "10.0.0.0/8",
			wantCIDRs: []string{"10.0.0.0/8"},
		},
		{
			name:      "empty_group_name",
			setup:     func(_ *rds.InMemoryBackend) {},
			groupName: "",
			cidrIP:    "10.0.0.0/8",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty_cidr",
			setup:     func(_ *rds.InMemoryBackend) {},
			groupName: "my-sg",
			cidrIP:    "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			sg, err := b.AuthorizeDBSecurityGroupIngress(tt.groupName, tt.cidrIP)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupName, sg.DBSecurityGroupName)

			cidrs := make([]string, 0, len(sg.IPRanges))
			for _, r := range sg.IPRanges {
				cidrs = append(cidrs, r.CIDRIP)
			}

			assert.Equal(t, tt.wantCIDRs, cidrs)
		})
	}
}

func TestDescribeDBSecurityGroups(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		filter    string
		wantCount int
		wantErr   bool
	}{
		{name: "all", filter: "", wantCount: 2},
		{name: "by name", filter: "sg-1", wantCount: 1},
		{name: "not found", filter: "missing", wantErr: true, wantErrIs: rds.ErrDBSecurityGroupNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			b.AddSecurityGroupInternal("sg-1", "Security group 1")
			b.AddSecurityGroupInternal("sg-2", "Security group 2")
			got, err := b.DescribeDBSecurityGroups(tt.filter)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestDeleteDBSecurityGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		sgName    string
		wantErr   bool
	}{
		{name: "success", sgName: "sg-1"},
		{name: "not found", sgName: "missing", wantErr: true, wantErrIs: rds.ErrDBSecurityGroupNotFound},
		{name: "empty name", sgName: "", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "success" {
				b.AddSecurityGroupInternal(tt.sgName, "desc")
			}
			err := b.DeleteDBSecurityGroup(tt.sgName)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRevokeDBSecurityGroupIngress(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		groupName string
		cidrIP    string
		wantErr   bool
	}{
		{name: "success", groupName: "sg-1", cidrIP: "10.0.0.0/8"},
		{
			name:      "group not found",
			groupName: "missing",
			cidrIP:    "10.0.0.0/8",
			wantErr:   true,
			wantErrIs: rds.ErrDBSecurityGroupNotFound,
		},
		{
			name:      "CIDR not found",
			groupName: "sg-1",
			cidrIP:    "192.168.0.0/16",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name != "group not found" {
				b.AddSecurityGroupInternal("sg-1", "desc")
				_, err := b.AuthorizeDBSecurityGroupIngress("sg-1", "10.0.0.0/8")
				require.NoError(t, err)
			}
			got, err := b.RevokeDBSecurityGroupIngress(tt.groupName, tt.cidrIP)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.NotNil(t, got)
		})
	}
}

func TestCreateDBSecurityGroupR2(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs   error
		name        string
		groupName   string
		description string
		wantErr     bool
	}{
		{
			name:        "success",
			groupName:   "my-sg",
			description: "test group",
		},
		{
			name:      "empty name",
			groupName: "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "already exists",
			groupName: "dup-sg",
			wantErr:   true,
			wantErrIs: rds.ErrDBSecurityGroupAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "already exists" {
				_, err := b.CreateDBSecurityGroup(tt.groupName, tt.description)
				require.NoError(t, err)
			}
			got, err := b.CreateDBSecurityGroup(tt.groupName, tt.description)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.groupName, got.DBSecurityGroupName)
			assert.Equal(t, tt.description, got.DBSecurityGroupDescription)
		})
	}
}
