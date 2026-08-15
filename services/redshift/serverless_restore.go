package redshift

import (
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// RestoreFromSnapshot, ConvertRecoveryPointToSnapshot
//
// Neither belongs to the RecoveryPoint/TableRestoreStatus entangled group
// built in a prior pass: RestoreFromSnapshot restores a namespace from a
// Snapshot with no RecoveryPoint involved, and ConvertRecoveryPointToSnapshot
// is not required by any restore op to function.
// ---------------------------------------------------------------------------

// RestoreFromSnapshotParams holds RestoreFromSnapshotInput's fields.
//
// MaintainIntegration is accepted for wire compatibility but intentionally
// inert: real AWS uses it to gate whether data-sharing/zero-ETL/S3-event
// integrations survive the restore, but this backend does not model
// integration state on namespaces at all (nothing to maintain or drop), so
// there is nothing honest to gate.
type RestoreFromSnapshotParams struct {
	MaintainIntegration         *bool
	NamespaceName               string
	WorkgroupName               string
	SnapshotName                string
	SnapshotArn                 string
	AdminPasswordSecretKmsKeyID string
	ManageAdminPassword         bool
}

// RestoreFromSnapshotSL restores namespaceName's data from a snapshot using
// workgroupName's compute. namespaceName/workgroupName are both required on
// the real RestoreFromSnapshotRequest and, by the same required-field shape
// and "name of the namespace to restore ... to/into" wording convention
// RestoreFromRecoveryPointRequest also uses (confirmed against both shapes
// in service-2.json), both are treated as pre-existing resources here too --
// the same design RestoreFromRecoveryPointSL already established. Real AWS
// restores a namespace's storage layer in place; this backend does not
// simulate real data content, so once the lookup/FK checks pass the existing
// Namespace is returned unchanged, same as RestoreFromRecoveryPointSL.
func (b *InMemoryBackend) RestoreFromSnapshotSL(p RestoreFromSnapshotParams) (*Namespace, string, error) {
	_ = p.MaintainIntegration // no integration state modeled on namespaces, see RestoreFromSnapshotParams doc comment

	b.mu.Lock("RestoreFromSnapshotSL")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces.Get(p.NamespaceName)
	if !ok {
		return nil, "", fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, p.NamespaceName)
	}

	wg, ok := b.slWorkgroups.Get(p.WorkgroupName)
	if !ok {
		return nil, "", fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, p.WorkgroupName)
	}

	if wg.NamespaceName != p.NamespaceName {
		return nil, "", fmt.Errorf(
			"%w: workgroup %q does not belong to namespace %q",
			ErrServerlessValidation, p.WorkgroupName, p.NamespaceName,
		)
	}

	lookup := p.SnapshotName
	if lookup == "" {
		lookup = p.SnapshotArn
	}

	name := lookup
	if idx := strings.LastIndex(lookup, "/"); strings.Contains(lookup, ":snapshot/") && idx >= 0 {
		name = lookup[idx+1:]
	}

	snap, ok := b.slSnapshots.Get(name)
	if !ok {
		return nil, "", fmt.Errorf("%w: snapshot %q not found", ErrServerlessSnapshotNotFound, lookup)
	}

	if p.ManageAdminPassword {
		if ns.AdminPasswordSecretArn == "" {
			ns.AdminPasswordSecretArn = arn.Build(
				"secretsmanager", b.region, b.accountID,
				"secret:redshift!"+p.NamespaceName+"-"+randomHex(slSecretHexBytes),
			)
		}

		if p.AdminPasswordSecretKmsKeyID != "" {
			ns.AdminPasswordSecretKmsKeyID = p.AdminPasswordSecretKmsKeyID
		}
	}

	return cloneNamespace(ns), snap.SnapshotName, nil
}

// ConvertRecoveryPointToSnapshotSL converts a recovery point into a durable
// snapshot. recoveryPointId/snapshotName are both required on the real
// ConvertRecoveryPointToSnapshotRequest (confirmed against service-2.json).
func (b *InMemoryBackend) ConvertRecoveryPointToSnapshotSL(
	recoveryPointID, snapshotName string, retentionPeriod int, tags map[string]string,
) (*ServerlessSnapshot, error) {
	b.mu.Lock("ConvertRecoveryPointToSnapshotSL")
	defer b.mu.Unlock()

	rp, ok := b.slRecoveryPoints.Get(recoveryPointID)
	if !ok {
		return nil, fmt.Errorf("%w: recovery point %q not found", ErrRecoveryPointNotFound, recoveryPointID)
	}

	if _, exists := b.slSnapshots.Get(snapshotName); exists {
		return nil, fmt.Errorf("%w: snapshot %q already exists", ErrServerlessConflict, snapshotName)
	}

	var adminUsername string
	if ns, nsOK := b.slNamespaces.Get(rp.NamespaceName); nsOK {
		adminUsername = ns.AdminUsername
	}

	snapArn := arn.Build("redshift-serverless", b.region, b.accountID, "snapshot/"+snapshotName)
	snap := &ServerlessSnapshot{
		SnapshotCreateTime:      time.Now(),
		SnapshotArn:             snapArn,
		SnapshotName:            snapshotName,
		NamespaceName:           rp.NamespaceName,
		NamespaceArn:            rp.NamespaceArn,
		Status:                  slStatusAvailable,
		AdminUsername:           adminUsername,
		SnapshotRetentionPeriod: retentionPeriod,
	}
	b.slSnapshots.Put(snap)
	b.slSnapshotIdx.insert(snapshotName)
	b.putServerlessTagsLocked(snapArn, tags)

	return cloneServerlessSnapshot(snap), nil
}
