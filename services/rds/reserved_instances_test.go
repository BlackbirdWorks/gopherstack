package rds_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPurchaseReservedDBInstancesOffering(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		offeringID      string
		reservedID      string
		dbInstanceCount int
		wantErr         bool
	}{
		{
			name:            "success with known offering",
			offeringID:      "01f5e8a3-2f47-4f47-8a7f-1234567890ab",
			reservedID:      "my-reserved-1",
			dbInstanceCount: 1,
		},
		{
			name:            "success with unknown offering",
			offeringID:      "unknown-offering-id",
			reservedID:      "my-reserved-2",
			dbInstanceCount: 2,
		},
		{
			name:            "zero count",
			offeringID:      "some-offering",
			reservedID:      "my-reserved-3",
			dbInstanceCount: 0,
			wantErr:         true,
			wantErrIs:       rds.ErrInvalidParameter,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.PurchaseReservedDBInstancesOffering(tt.offeringID, tt.reservedID, tt.dbInstanceCount)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.offeringID, got.ReservedDBInstancesOfferingID)
			assert.Equal(t, tt.dbInstanceCount, got.DBInstanceCount)
			assert.Equal(t, "active", got.State)
		})
	}
}

func TestDescribeReservedDBInstances(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		filterID    string
		filterClass string
		wantCount   int
	}{
		{name: "all empty", filterID: "", filterClass: "", wantCount: 1},
		{name: "match by ID", filterID: "my-ri", filterClass: "", wantCount: 1},
		{name: "no match by ID", filterID: "missing", filterClass: "", wantCount: 0},
		{name: "match by class", filterID: "", filterClass: "db.t3.micro", wantCount: 1},
		{name: "no match by class", filterID: "", filterClass: "db.r5.large", wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			_, err := b.PurchaseReservedDBInstancesOffering("01f5e8a3-2f47-4f47-8a7f-1234567890ab", "my-ri", 1)
			require.NoError(t, err)
			got := b.DescribeReservedDBInstances(tt.filterID, tt.filterClass)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestDescribeReservedDBInstancesOfferings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		offeringID  string
		classFilter string
		wantMin     int
	}{
		{name: "all offerings", offeringID: "", classFilter: "", wantMin: 5},
		{name: "filter by ID", offeringID: "01f5e8a3-2f47-4f47-8a7f-1234567890ab", classFilter: "", wantMin: 1},
		{name: "filter by class", offeringID: "", classFilter: "db.m5.large", wantMin: 2},
		{name: "no match", offeringID: "nonexistent", classFilter: "", wantMin: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeReservedDBInstancesOfferings(tt.offeringID, tt.classFilter)
			assert.GreaterOrEqual(t, len(got), tt.wantMin)
		})
	}
}
