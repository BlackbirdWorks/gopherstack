package dynamodb

import (
	"errors"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

var (
	errOneWitnessUpdate       = errors.New("only one witness can be created or deleted per UpdateTable operation")
	errWitnessNeedsStrong     = errors.New("a witness requires a table with MultiRegionConsistency STRONG")
	errWitnessOnCreateOnly    = errors.New("a witness must be added when creating the MRSC global table")
	errOneWitnessPerTable     = errors.New("an MRSC global table supports one witness")
	errWitnessNotFound        = errors.New("the witness Region does not exist on this global table")
	errWitnessDeleteNeedsDrop = errors.New("a witness must be deleted together with a replica")
	errWitnessRegionRequired  = errors.New("RegionName is required for GlobalTableWitnessUpdates")
)

// applyWitnessUpdates applies UpdateTable.GlobalTableWitnessUpdates per the SDK and the MRSC developer
// guide: one witness change per call, added only while creating a STRONG global table, deleted only
// together with a replica.
func applyWitnessUpdates(table *Table, input *dynamodb.UpdateTableInput) error {
	updates := input.GlobalTableWitnessUpdates
	if len(updates) == 0 {
		return nil
	}

	if len(updates) > 1 {
		return errOneWitnessUpdate
	}

	u := updates[0]

	switch {
	case u.Create != nil:
		return createWitness(table, aws.ToString(u.Create.RegionName), input)
	case u.Delete != nil:
		return deleteWitness(table, aws.ToString(u.Delete.RegionName), input.ReplicaUpdates)
	}

	return nil
}

func createWitness(table *Table, region string, input *dynamodb.UpdateTableInput) error {
	if region == "" {
		return errWitnessRegionRequired
	}

	if table.MultiRegionConsistency != string(types.MultiRegionConsistencyStrong) {
		return errWitnessNeedsStrong
	}

	creating := slices.ContainsFunc(input.ReplicaUpdates, func(r types.ReplicationGroupUpdate) bool {
		return r.Create != nil
	})
	if !creating {
		return errWitnessOnCreateOnly
	}

	if len(table.GlobalTableWitnesses) > 0 {
		return errOneWitnessPerTable
	}

	table.GlobalTableWitnesses = []models.GlobalTableWitness{
		{RegionName: region, WitnessStatus: string(types.WitnessStatusActive)},
	}

	return nil
}

func deleteWitness(table *Table, region string, replicaUpdates []types.ReplicationGroupUpdate) error {
	if region == "" {
		return errWitnessRegionRequired
	}

	idx := slices.IndexFunc(table.GlobalTableWitnesses, func(w models.GlobalTableWitness) bool {
		return w.RegionName == region
	})
	if idx < 0 {
		return errWitnessNotFound
	}

	if !slices.ContainsFunc(replicaUpdates, func(r types.ReplicationGroupUpdate) bool { return r.Delete != nil }) {
		return errWitnessDeleteNeedsDrop
	}

	table.GlobalTableWitnesses = slices.Delete(table.GlobalTableWitnesses, idx, idx+1)

	return nil
}
