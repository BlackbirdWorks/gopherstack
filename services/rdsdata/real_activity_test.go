package rdsdata //nolint:testpackage // white-box test of the engine's resolver calls.

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingResolver struct {
	calls *atomic.Int32
}

func (c countingResolver) DataAPITarget(string) (RealTarget, error) {
	c.calls.Add(1)

	return RealTarget{Kind: kindMySQL, Addr: "db:3306", DBName: "app"}, nil
}

// Statements inside a held transaction must reach the cluster resolver so the cluster counts them as activity.
func TestRealEngineTransactionStatementsReportActivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		stmts int
	}{
		{name: "one_statement", stmts: 1},
		{name: "three_statements", stmts: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newItemsHarness(t)
			calls := &atomic.Int32{}
			h.b.real.resolver = countingResolver{calls: calls}

			id, err := h.b.BeginTransaction(h.ctx(t), testClusterARN)
			require.NoError(t, err)

			afterBegin := calls.Load()

			for range tt.stmts {
				_, err = h.exec(t, "SELECT count(*) FROM items", id)
				require.NoError(t, err)
			}

			assert.Equal(t, int32(tt.stmts), calls.Load()-afterBegin)
		})
	}
}
