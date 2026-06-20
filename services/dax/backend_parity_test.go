package dax_test

import (
	"maps"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- ClusterName format validation ----

func TestValidateClusterName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		input       string
		wantErr     bool
	}{
		{name: "valid simple", input: "mycluster", wantErr: false},
		{name: "valid with hyphen", input: "my-cluster", wantErr: false},
		{name: "valid single letter", input: "a", wantErr: false},
		{name: "valid max length", input: strings.Repeat("a", 20), wantErr: false},
		{name: "empty", input: "", wantErr: true, errSentinel: dax.ErrInvalidParameterValue},
		{name: "too long", input: strings.Repeat("a", 21), wantErr: true, errSentinel: dax.ErrInvalidParameterValue},
		{name: "starts with digit", input: "1cluster", wantErr: true, errSentinel: dax.ErrInvalidParameterValue},
		{name: "starts with hyphen", input: "-cluster", wantErr: true, errSentinel: dax.ErrInvalidParameterValue},
		{name: "ends with hyphen", input: "cluster-", wantErr: true, errSentinel: dax.ErrInvalidParameterValue},
		{name: "consecutive hyphens", input: "my--cluster", wantErr: true, errSentinel: dax.ErrInvalidParameterValue},
		{
			name:        "invalid char underscore",
			input:       "my_cluster",
			wantErr:     true,
			errSentinel: dax.ErrInvalidParameterValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, err := b.CreateCluster(dax.CreateClusterInput{
				ClusterName:       tt.input,
				NodeType:          "dax.r5.large",
				IamRoleArn:        "arn:aws:iam::123456789012:role/DAXRole",
				ReplicationFactor: 1,
			})

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- ReplicationFactor boundary validation ----

func TestCreateClusterReplicationFactorBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel       error
		name              string
		replicationFactor int
		wantErr           bool
	}{
		{
			name:              "zero is rejected",
			replicationFactor: 0,
			wantErr:           true,
			errSentinel:       dax.ErrInvalidParameterCombination,
		},
		{
			name:              "negative is rejected",
			replicationFactor: -1,
			wantErr:           true,
			errSentinel:       dax.ErrInvalidParameterCombination,
		},
		{name: "one is valid", replicationFactor: 1, wantErr: false},
		{name: "ten is valid max", replicationFactor: 10, wantErr: false},
		{
			name:              "eleven exceeds max",
			replicationFactor: 11,
			wantErr:           true,
			errSentinel:       dax.ErrInvalidParameterCombination,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, err := b.CreateCluster(dax.CreateClusterInput{
				ClusterName:       "valid-name",
				NodeType:          "dax.r5.large",
				IamRoleArn:        "arn:aws:iam::123456789012:role/DAXRole",
				ReplicationFactor: tt.replicationFactor,
			})

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- SubnetGroupInUseFault ----

func TestDeleteSubnetGroupInUseFault(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	_, err := b.CreateCluster(validCreateInput("uses-default"))
	require.NoError(t, err)

	err = b.DeleteSubnetGroup(dax.DefaultSubnetGroupName)
	require.Error(t, err)
	assert.ErrorIs(t, err, dax.ErrSubnetGroupInUse)
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

// ---- CreateSubnetGroup: requires at least one subnet ----

func TestCreateSubnetGroupRequiresSubnet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		subnetIDs []string
		wantErr   bool
	}{
		{name: "nil subnets rejected", subnetIDs: nil, wantErr: true},
		{name: "empty subnets rejected", subnetIDs: []string{}, wantErr: true},
		{name: "one subnet accepted", subnetIDs: []string{"subnet-abc12345"}, wantErr: false},
		{name: "multiple subnets accepted", subnetIDs: []string{"subnet-aaa", "subnet-bbb"}, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, err := b.CreateSubnetGroup("mysg", "", tt.subnetIDs)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, dax.ErrInvalidParameterValue)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---- CreateSubnetGroup: VpcID is populated ----

func TestCreateSubnetGroupVpcIDPopulated(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	sg, err := b.CreateSubnetGroup("test-sg", "", []string{"subnet-abc12345"})
	require.NoError(t, err)
	assert.NotEmpty(t, sg.VpcID, "VpcID should be populated")
	assert.True(t, strings.HasPrefix(sg.VpcID, "vpc-"), "VpcID should start with vpc-")
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

// ---- SubnetGroupName format validation ----

func TestCreateSubnetGroupNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sgName  string
		wantErr bool
	}{
		{name: "valid", sgName: "my-sg", wantErr: false},
		{name: "starts with digit", sgName: "1sg", wantErr: true},
		{name: "ends with hyphen", sgName: "sg-", wantErr: true},
		{name: "consecutive hyphens", sgName: "my--sg", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, err := b.CreateSubnetGroup(tt.sgName, "", []string{"subnet-1"})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, dax.ErrInvalidParameterValue)
			} else {
				require.NoError(t, err)
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

// ---- DescribeSubnetGroups pagination ----

func TestDescribeSubnetGroupsPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	// Create additional groups beyond the default.
	for i := range 5 {
		name := []byte{'a' + byte(i)}
		_, err := b.CreateSubnetGroup(string(name)+"-sg", "", []string{"subnet-1"})
		require.NoError(t, err)
	}

	// First page of 2.
	page1, tok1, err := b.DescribeSubnetGroups(nil, 2, "")
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, tok1)

	// Second page.
	page2, tok2, err := b.DescribeSubnetGroups(nil, 2, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, tok2)

	// Ensure no duplicates across pages.
	seen := make(map[string]bool)
	for _, sg := range append(page1, page2...) {
		assert.False(t, seen[sg.SubnetGroupName], "duplicate %s", sg.SubnetGroupName)
		seen[sg.SubnetGroupName] = true
	}
}

// ---- DescribeParameters pagination ----

func TestDescribeParametersPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	// Paginate a single-item page (there are exactly 2 default params).
	page1, tok1, err := b.DescribeParameters(dax.DefaultParameterGroupName, 1, "")
	require.NoError(t, err)
	assert.Len(t, page1, 1)
	assert.NotEmpty(t, tok1)

	page2, tok2, err := b.DescribeParameters(dax.DefaultParameterGroupName, 1, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 1)
	assert.Empty(t, tok2, "second page should be the last")

	// Names must be distinct.
	assert.NotEqual(t, page1[0].ParameterName, page2[0].ParameterName)
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
		assert.Equal(t, "requires-reboot", p.ChangeType, "param %s ChangeType", p.ParameterName)
	}
}

// ---- ListTags pagination ----

func TestListTagsPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()

	// Create cluster with many tags.
	input := validCreateInput("tagged-cluster")
	input.Tags = make(map[string]string, 15)
	for i := range 15 {
		input.Tags[string([]byte{'a' + byte(i)})+"-key"] = "val"
	}

	_, err := b.CreateCluster(input)
	require.NoError(t, err)

	clusterARN := "arn:aws:dax:us-east-1:123456789012:cache/tagged-cluster"

	// First page (page size = 10).
	page1, tok1, err := b.ListTags(clusterARN, "")
	require.NoError(t, err)
	assert.Len(t, page1, 10)
	assert.NotEmpty(t, tok1)

	// Second page.
	page2, tok2, err := b.ListTags(clusterARN, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 5)
	assert.Empty(t, tok2)

	// All tags accounted for, no duplicates.
	all := make(map[string]string)
	maps.Copy(all, page1)

	for k, v := range page2 {
		assert.NotContains(t, all, k, "duplicate key %s", k)
		all[k] = v
	}

	assert.Len(t, all, 15)
}

// ---- DecreaseReplicationFactor: NodeIDsToRemove count validation ----

func TestDecreaseReplicationFactorNodeIDsCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		nodeIDs     []string
		newFactor   int
		wantErr     bool
	}{
		{
			name:      "no node IDs uses tail removal",
			nodeIDs:   nil,
			newFactor: 1,
			wantErr:   false,
		},
		{
			name:        "wrong count rejected",
			nodeIDs:     []string{"valid-name-0000"},
			newFactor:   1,
			wantErr:     true,
			errSentinel: dax.ErrInvalidParameterCombination,
		},
		{
			name:      "correct count accepted",
			nodeIDs:   []string{"valid-name-0001", "valid-name-0002"},
			newFactor: 1,
			wantErr:   false,
		},
		{
			name:        "nonexistent node ID rejected",
			nodeIDs:     []string{"nonexistent-9999", "nonexistent-8888"},
			newFactor:   1,
			wantErr:     true,
			errSentinel: dax.ErrNodeNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()
			_, err := b.CreateCluster(dax.CreateClusterInput{
				ClusterName:       "valid-name",
				NodeType:          "dax.r5.large",
				IamRoleArn:        "arn:aws:iam::123456789012:role/DAXRole",
				ReplicationFactor: 3,
			})
			require.NoError(t, err)

			_, err = b.DecreaseReplicationFactor(dax.DecreaseReplicationFactorInput{
				ClusterName:          "valid-name",
				NewReplicationFactor: tt.newFactor,
				NodeIDsToRemove:      tt.nodeIDs,
			})

			if tt.wantErr {
				require.Error(t, err)
				if tt.errSentinel != nil {
					require.ErrorIs(t, err, tt.errSentinel)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
