package dax_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- Parameter Groups ----

func TestCreateParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check       func(t *testing.T, pg *dax.ParameterGroup)
		name        string
		pgName      string
		description string
		wantErr     bool
	}{
		{
			name:        "success",
			pgName:      "my-pg",
			description: "test group",
			check: func(t *testing.T, pg *dax.ParameterGroup) {
				t.Helper()
				assert.Equal(t, "my-pg", pg.ParameterGroupName)
				assert.Equal(t, "test group", pg.Description)
				assert.Equal(t, "300000", pg.Parameters["query-ttl-millis"])
				assert.Equal(t, "300000", pg.Parameters["record-ttl-millis"])
			},
		},
		{
			name:    "empty name",
			pgName:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			pg, err := b.CreateParameterGroup(tt.pgName, tt.description)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, pg)
			}
		})
	}
}

func TestCreateParameterGroup_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateParameterGroup("pg", "")
	require.NoError(t, err)
	_, err = b.CreateParameterGroup("pg", "")
	require.Error(t, err)
}

// ---- ParameterGroupName format validation ----

func TestCreateParameterGroupNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pgName  string
		wantErr bool
	}{
		{name: "valid", pgName: "my-pg", wantErr: false},
		{name: "starts with digit", pgName: "1pg", wantErr: true},
		{name: "ends with hyphen", pgName: "pg-", wantErr: true},
		{name: "consecutive hyphens", pgName: "my--pg", wantErr: true},
		{name: "underscore invalid", pgName: "my_pg", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, err := b.CreateParameterGroup(tt.pgName, "")

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, dax.ErrInvalidParameterValue)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDescribeParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(b *dax.InMemoryBackend)
		names     []string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "default group exists",
			setup:     func(_ *dax.InMemoryBackend) {},
			wantCount: 1,
		},
		{
			name: "with custom group",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("custom", "")
			},
			wantCount: 2,
		},
		{
			name: "filter by name found",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("target", "")
			},
			names:     []string{"target"},
			wantCount: 1,
		},
		{
			name:    "filter by name not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			names:   []string{"nonexistent"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			groups, _, err := b.DescribeParameterGroups(tt.names, 0, "")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantCount)
		})
	}
}

// ---- DescribeParameterGroups pagination ----

func TestDescribeParameterGroupsPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	// Create additional groups beyond the default.
	for i := range 5 {
		name := []byte{'a' + byte(i)}
		_, err := b.CreateParameterGroup(string(name)+"-pg", "")
		require.NoError(t, err)
	}

	// First page of 2.
	page1, tok1, err := b.DescribeParameterGroups(nil, 2, "")
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, tok1)

	// Second page.
	page2, tok2, err := b.DescribeParameterGroups(nil, 2, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, tok2)

	// Ensure no duplicates across pages.
	seen := make(map[string]bool)
	for _, pg := range append(page1, page2...) {
		assert.False(t, seen[pg.ParameterGroupName], "duplicate %s", pg.ParameterGroupName)
		seen[pg.ParameterGroupName] = true
	}
}

func TestUpdateParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *dax.InMemoryBackend)
		check   func(t *testing.T, pg *dax.ParameterGroup)
		name    string
		input   dax.UpdateParameterGroupInput
		wantErr bool
	}{
		{
			name: "update query-ttl",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("my-pg", "")
			},
			input: dax.UpdateParameterGroupInput{
				ParameterGroupName: "my-pg",
				ParameterNameValues: []dax.ParameterNameValue{
					{ParameterName: "query-ttl-millis", ParameterValue: "60000"},
				},
			},
			check: func(t *testing.T, pg *dax.ParameterGroup) {
				t.Helper()
				assert.Equal(t, "60000", pg.Parameters["query-ttl-millis"])
			},
		},
		{
			name:  "unknown parameter name",
			setup: func(b *dax.InMemoryBackend) { _, _ = b.CreateParameterGroup("pg", "") },
			input: dax.UpdateParameterGroupInput{
				ParameterGroupName:  "pg",
				ParameterNameValues: []dax.ParameterNameValue{{ParameterName: "unknown-param", ParameterValue: "1"}},
			},
			wantErr: true,
		},
		{
			name:    "group not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			input:   dax.UpdateParameterGroupInput{ParameterGroupName: "no-such"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			pg, err := b.UpdateParameterGroup(tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, pg)
			}
		})
	}
}

// ---- UpdateParameterGroup: value must be non-negative integer ----

func TestUpdateParameterGroupValueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "valid zero", value: "0", wantErr: false},
		{name: "valid positive", value: "60000", wantErr: false},
		{name: "valid max", value: "2147483647", wantErr: false},
		{name: "negative rejected", value: "-1", wantErr: true},
		{name: "float rejected", value: "1.5", wantErr: true},
		{name: "non-numeric rejected", value: "fast", wantErr: true},
		{name: "empty rejected", value: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, err := b.CreateParameterGroup("test-pg", "")
			require.NoError(t, err)

			_, err = b.UpdateParameterGroup(dax.UpdateParameterGroupInput{
				ParameterGroupName: "test-pg",
				ParameterNameValues: []dax.ParameterNameValue{
					{ParameterName: "query-ttl-millis", ParameterValue: tt.value},
				},
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, dax.ErrInvalidParameterValue)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- UpdateParameterGroup: marks dependent clusters pending-reboot ----

func TestUpdateParameterGroupMarksPendingReboot(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.CreateParameterGroup("custom-pg", "test group")
	require.NoError(t, err)

	_, err = b.CreateCluster(dax.CreateClusterInput{
		ClusterName:        "pg-cluster",
		NodeType:           "dax.r5.large",
		IamRoleArn:         "arn:aws:iam::123456789012:role/DAXRole",
		ReplicationFactor:  2,
		ParameterGroupName: "custom-pg",
	})
	require.NoError(t, err)

	// Cluster not using this PG should not be affected.
	_, err = b.CreateCluster(validCreateInput("other-cluster"))
	require.NoError(t, err)

	_, err = b.UpdateParameterGroup(dax.UpdateParameterGroupInput{
		ParameterGroupName: "custom-pg",
		ParameterNameValues: []dax.ParameterNameValue{
			{ParameterName: "query-ttl-millis", ParameterValue: "600000"},
		},
	})
	require.NoError(t, err)

	clusters, _, err := b.DescribeClusters([]string{"pg-cluster"}, 0, "")
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	pgStatus := clusters[0].ParameterGroup
	assert.Equal(t, "pending-reboot", pgStatus.ParameterApplyStatus)
	assert.Len(t, pgStatus.NodeIDsToReboot, 2, "both nodes should be listed for reboot")

	// Other cluster should remain in-sync.
	other, _, err := b.DescribeClusters([]string{"other-cluster"}, 0, "")
	require.NoError(t, err)
	require.Len(t, other, 1)
	assert.Equal(t, "in-sync", other[0].ParameterGroup.ParameterApplyStatus)
}

func TestDeleteParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(b *dax.InMemoryBackend)
		pgName  string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("pg-del", "")
			},
			pgName: "pg-del",
		},
		{
			name:    "not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			pgName:  "no-such",
			wantErr: true,
		},
		{
			name: "in use",
			setup: func(b *dax.InMemoryBackend) {
				input := validCreateInput("cluster-with-pg")
				input.ParameterGroupName = dax.DefaultParameterGroupName
				_, _ = b.CreateCluster(input)
			},
			pgName:  dax.DefaultParameterGroupName,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			err := b.DeleteParameterGroup(tt.pgName)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- ParameterGroupInUseFault ----

func TestDeleteParameterGroupInUseFault(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.CreateCluster(validCreateInput("uses-default"))
	require.NoError(t, err)

	err = b.DeleteParameterGroup(dax.DefaultParameterGroupName)
	require.Error(t, err)
	assert.ErrorIs(t, err, dax.ErrParameterGroupInUse)
}

func TestDescribeParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pgName     string
		wantParams []string
		wantErr    bool
	}{
		{
			name:       "default group has params",
			pgName:     dax.DefaultParameterGroupName,
			wantParams: []string{"query-ttl-millis", "record-ttl-millis"},
		},
		{
			name:    "group not found",
			pgName:  "no-such",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			params, _, err := b.DescribeParameters(tt.pgName, 0, "", "")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			names := make([]string, 0, len(params))
			for _, p := range params {
				names = append(names, p.ParameterName)
			}

			for _, expected := range tt.wantParams {
				assert.Contains(t, names, expected)
			}
		})
	}
}

// ---- DescribeParameters Source filter ----

// TestDescribeParametersSourceFilter verifies the request's Source field
// (types.DescribeParametersInput.Source in the real SDK) narrows results to
// "user"-modified or "system"-default parameters.
func TestDescribeParametersSourceFilter(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.CreateParameterGroup("filter-pg", "")
	require.NoError(t, err)

	// Override one of the two default parameters so it becomes "user" sourced;
	// the other stays at its default value and is reported as "system".
	_, err = b.UpdateParameterGroup(dax.UpdateParameterGroupInput{
		ParameterGroupName: "filter-pg",
		ParameterNameValues: []dax.ParameterNameValue{
			{ParameterName: "query-ttl-millis", ParameterValue: "60000"},
		},
	})
	require.NoError(t, err)

	userParams, _, err := b.DescribeParameters("filter-pg", 0, "", "user")
	require.NoError(t, err)
	require.Len(t, userParams, 1)
	assert.Equal(t, "query-ttl-millis", userParams[0].ParameterName)
	assert.Equal(t, "user", userParams[0].Source)

	systemParams, _, err := b.DescribeParameters("filter-pg", 0, "", "system")
	require.NoError(t, err)
	require.Len(t, systemParams, 1)
	assert.Equal(t, "record-ttl-millis", systemParams[0].ParameterName)
	assert.Equal(t, "system", systemParams[0].Source)

	allParams, _, err := b.DescribeParameters("filter-pg", 0, "", "")
	require.NoError(t, err)
	assert.Len(t, allParams, 2, "empty Source filter must return every parameter")
}

// ---- DescribeParameters pagination ----

func TestDescribeParametersPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	// Paginate a single-item page (there are exactly 2 default params).
	page1, tok1, err := b.DescribeParameters(dax.DefaultParameterGroupName, 1, "", "")
	require.NoError(t, err)
	assert.Len(t, page1, 1)
	assert.NotEmpty(t, tok1)

	page2, tok2, err := b.DescribeParameters(dax.DefaultParameterGroupName, 1, tok1, "")
	require.NoError(t, err)
	assert.Len(t, page2, 1)
	assert.Empty(t, tok2, "second page should be the last")

	// Names must be distinct.
	assert.NotEqual(t, page1[0].ParameterName, page2[0].ParameterName)
}

func TestDescribeDefaultParameters(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	params, _, err := b.DescribeDefaultParameters(0, "")
	require.NoError(t, err)
	assert.Len(t, params, 2)

	names := make([]string, 0, len(params))
	for _, p := range params {
		names = append(names, p.ParameterName)
	}

	assert.Contains(t, names, "query-ttl-millis")
	assert.Contains(t, names, "record-ttl-millis")
}

// ---- DescribeDefaultParameters pagination ----

func TestDescribeDefaultParametersPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	page1, tok1, err := b.DescribeDefaultParameters(1, "")
	require.NoError(t, err)
	assert.Len(t, page1, 1)
	assert.NotEmpty(t, tok1)

	page2, tok2, err := b.DescribeDefaultParameters(1, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 1)
	assert.Empty(t, tok2)

	assert.NotEqual(t, page1[0].ParameterName, page2[0].ParameterName)
}

// ---- Parameter AllowedValues and ParameterType fields ----

func TestParameterResponseFields(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	params, _, err := b.DescribeDefaultParameters(0, "")
	require.NoError(t, err)
	require.NotEmpty(t, params)

	for _, p := range params {
		assert.NotEmpty(t, p.AllowedValues, "param %s should have AllowedValues", p.ParameterName)
		assert.Equal(t, dax.ParameterTypeDefault, p.ParameterType,
			"param %s should have ParameterType", p.ParameterName)
		assert.Equal(t, "integer", p.DataType, "param %s DataType should be integer", p.ParameterName)
		assert.Equal(t, "TRUE", p.IsModifiable, "param %s IsModifiable should be TRUE", p.ParameterName)
		assert.Equal(t, "REQUIRES_REBOOT", p.ChangeType, "param %s ChangeType", p.ParameterName)
	}
}

func TestResetParameterGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(b *dax.InMemoryBackend)
		check          func(t *testing.T, pg *dax.ParameterGroup)
		name           string
		pgName         string
		parameterNames []string
		wantErr        bool
	}{
		{
			name: "reset all to defaults",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("pg", "")
				_, _ = b.UpdateParameterGroup(dax.UpdateParameterGroupInput{
					ParameterGroupName: "pg",
					ParameterNameValues: []dax.ParameterNameValue{
						{ParameterName: "query-ttl-millis", ParameterValue: "99999"},
					},
				})
			},
			pgName: "pg",
			check: func(t *testing.T, pg *dax.ParameterGroup) {
				t.Helper()
				assert.Equal(t, "300000", pg.Parameters["query-ttl-millis"])
			},
		},
		{
			name: "reset specific parameter",
			setup: func(b *dax.InMemoryBackend) {
				_, _ = b.CreateParameterGroup("pg2", "")
				_, _ = b.UpdateParameterGroup(dax.UpdateParameterGroupInput{
					ParameterGroupName: "pg2",
					ParameterNameValues: []dax.ParameterNameValue{
						{ParameterName: "query-ttl-millis", ParameterValue: "99999"},
					},
				})
			},
			pgName:         "pg2",
			parameterNames: []string{"query-ttl-millis"},
			check: func(t *testing.T, pg *dax.ParameterGroup) {
				t.Helper()
				assert.Equal(t, "300000", pg.Parameters["query-ttl-millis"])
			},
		},
		{
			name:    "not found",
			setup:   func(_ *dax.InMemoryBackend) {},
			pgName:  "no-such",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			tt.setup(b)

			pg, err := b.ResetParameterGroup(tt.pgName, tt.parameterNames)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, pg)
			}
		})
	}
}
