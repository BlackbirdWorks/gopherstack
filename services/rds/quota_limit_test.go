package rds_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

func quotaFunc(code string, limit float64, known bool) quotalimit.Func {
	return func(_, _, svc, got string) (float64, bool) {
		return limit, known && svc == "rds" && got == code
	}
}

func TestQuotaLimiterCreates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kind    string
		code    string
		limit   float64
		creates int
		known   bool
		wantErr bool
	}{
		{name: "instances within limit", kind: "instance", code: "L-7B6409FD", limit: 2, known: true, creates: 2},
		{
			name: "instances over limit", kind: "instance", code: "L-7B6409FD", limit: 2, known: true,
			creates: 3, wantErr: true,
		},
		{name: "instances unknown quota", kind: "instance", code: "L-7B6409FD", limit: 1, creates: 3},
		{name: "clusters within limit", kind: "cluster", code: "L-952B80B8", limit: 2, known: true, creates: 2},
		{
			name: "clusters over limit", kind: "cluster", code: "L-952B80B8", limit: 2, known: true,
			creates: 3, wantErr: true,
		},
		{name: "clusters unknown quota", kind: "cluster", code: "L-952B80B8", limit: 1, creates: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch3Handler(t)
			h.SetQuotaLimiter(quotaFunc(tt.code, tt.limit, tt.known))

			var err error

			for i := range tt.creates {
				id := "q" + strconv.Itoa(i)
				if tt.kind == "instance" {
					_, err = h.Backend.CreateDBInstance(id, "postgres", "db.t3.micro", "", "admin", "", 20,
						rds.DBInstanceOptions{})
				} else {
					_, err = h.Backend.CreateDBCluster(id, "aurora-postgresql", "admin", "", "", 5432, nil,
						rds.DBClusterOptions{})
				}
			}

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			if tt.kind == "instance" {
				require.ErrorIs(t, err, rds.ErrInstanceQuotaExceeded)
				assert.Equal(t, 2, h.InstanceCount("us-east-1"))

				return
			}

			require.ErrorIs(t, err, rds.ErrClusterQuotaExceeded)
			assert.Equal(t, 2, h.ClusterCount("us-east-1"))
		})
	}
}

func TestQuotaLimiterSharedWithRegionBackends(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)
	h.SetQuotaLimiter(quotaFunc("L-7B6409FD", 1, true))

	west := h.BackendFor("us-west-2")
	_, err := west.CreateDBInstance("w1", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)

	_, err = west.CreateDBInstance("w2", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.ErrorIs(t, err, rds.ErrInstanceQuotaExceeded)

	assert.Equal(t, 1, h.InstanceCount("us-west-2"))
	assert.Equal(t, 0, h.InstanceCount("us-east-1"))
	assert.Equal(t, 0, h.InstanceCount("eu-central-1"))
}
