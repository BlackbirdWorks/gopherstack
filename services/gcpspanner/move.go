package gcpspanner

import (
	"context"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
)

// instanceHasBackups asks the data plane whether the instance holds backups; without one there are none.
func (b *InMemoryBackend) instanceHasBackups(ctx context.Context, name string) (bool, error) {
	cc := b.emu.current()
	if cc == nil {
		return false, nil
	}

	resp, err := databasepb.NewDatabaseAdminClient(cc).
		ListBackups(ctx, &databasepb.ListBackupsRequest{Parent: name, PageSize: 1})
	if err != nil {
		return false, err //nolint:wrapcheck // emulator status passes through
	}

	return len(resp.GetBackups()) > 0, nil
}

// moveBlocker names why an instance cannot move, or "" when it can. The caller holds b.mu.
func moveBlocker(rec *instRec) string {
	switch {
	case rec.pb.GetState() != instancepb.Instance_READY:
		return "has an ongoing update"
	case rec.pb.GetInstanceType() == instancepb.Instance_FREE_INSTANCE:
		return "is a free trial instance"
	}

	for _, d := range rec.dbs {
		if d.pb.GetEncryptionConfig() != nil {
			return "contains CMEK-enabled databases"
		}
	}

	return ""
}

// MoveInstance moves an instance to another configuration.
func (b *InMemoryBackend) MoveInstance(
	ctx context.Context,
	in *instancepb.MoveInstanceRequest,
) (*longrunningpb.Operation, error) {
	if _, _, err := parseInstance(in.GetName()); err != nil {
		return nil, err
	}

	target, ok := b.lookupConfigName(in.GetTargetConfig())
	if !ok {
		return nil, errNotFound("Instance config not found: %s", in.GetTargetConfig())
	}

	if target.GetState() != instancepb.InstanceConfig_READY {
		return nil, errPrecondition("Instance config %s is not ready.", in.GetTargetConfig())
	}

	if _, err := b.GetInstance(in.GetName()); err != nil {
		return nil, err
	}

	hasBackups, err := b.instanceHasBackups(ctx, in.GetName())
	if err != nil {
		return nil, err
	}

	if hasBackups {
		return nil, errPrecondition("Instance %s has backups and cannot be moved.", in.GetName())
	}

	start := b.ts()

	b.mu.Lock("MoveInstance")
	err = b.moveLocked(in.GetName(), target)
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	meta := &instancepb.MoveInstanceMetadata{TargetConfig: in.GetTargetConfig(), Progress: progress(start, b.ts())}

	return b.finished(opName(in.GetName(), ""), meta, &instancepb.MoveInstanceResponse{})
}

func (b *InMemoryBackend) moveLocked(name string, target *instancepb.InstanceConfig) error {
	if err := b.configGoneLocked(target.GetName()); err != nil {
		return err
	}

	rec, err := b.instanceLocked(name)
	if err != nil {
		return err
	}

	if why := moveBlocker(rec); why != "" {
		return errPrecondition("Instance %s %s and cannot be moved.", name, why)
	}

	if rec.pb.GetConfig() == target.GetName() {
		return errInvalid("Instance %s already uses %s.", name, target.GetName())
	}

	next := cloneInstance(rec.pb)
	next.Config = target.GetName()
	next.UpdateTime = b.ts()

	if next.GetAutoscalingConfig() != nil {
		if err = validateAutoscaling(next.GetAutoscalingConfig(), target); err != nil {
			return err
		}
	}

	replicaCapacity(next, target)
	rec.pb = next

	return nil
}
