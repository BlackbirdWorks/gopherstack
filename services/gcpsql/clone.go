package gcpsql

import (
	"context"
)

// CloneInput is the cloneContext of instances.clone.
type CloneInput struct {
	DestinationInstanceName string   `json:"destinationInstanceName"`
	DestinationProject      string   `json:"destinationProject"`
	PointInTime             string   `json:"pointInTime"`
	PitrTimestampMs         string   `json:"pitrTimestampMs"`
	DatabaseNames           []string `json:"databaseNames"`
}

// CloneInstance creates a new instance with the settings, accounts and data of the source.
func (b *InMemoryBackend) CloneInstance(project, instance string, in CloneInput) (*Operation, error) {
	at, pitr, err := recoveryTarget(in)
	if err != nil {
		return nil, err
	}

	if in.DestinationProject != "" && in.DestinationProject != project {
		return nil, errInvalid("Cloning into another project is not supported by the emulator.")
	}

	if !instanceNameRe.MatchString(in.DestinationInstanceName) || len(in.DestinationInstanceName) > maxNameLen {
		return nil, errInvalid("Invalid instance name: %q.", in.DestinationInstanceName)
	}

	b.mu.Lock("CloneInstance")
	defer b.mu.Unlock()

	src, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	if pitr {
		if err = b.checkRecoveryLocked(src, at); err != nil {
			return nil, err
		}
	}

	if _, dup := b.instances[instKey(project, in.DestinationInstanceName)]; dup {
		return nil, errConflict("instanceAlreadyExists", "The Cloud SQL instance already exists.")
	}

	dst, err := b.cloneRecLocked(src, in)
	if err != nil {
		return nil, err
	}

	b.instances[instKey(project, in.DestinationInstanceName)] = dst
	op := b.newOpLocked(project, instance, "CLONE", opMeta{})
	b.enqueueLocked(dst, task{op: op.Name, fn: func(ctx context.Context) error {
		if pitr {
			return b.populateRecovery(ctx, src, dst, at)
		}

		return b.populateClone(ctx, src, dst, in.DatabaseNames)
	}})

	cp := *op

	return &cp, nil
}

func (b *InMemoryBackend) cloneRecLocked(src *instRec, in CloneInput) (*instRec, error) {
	req := InstanceInput{
		Name: in.DestinationInstanceName, Region: src.Inst.Region, DatabaseVersion: src.Inst.DatabaseVersion,
		Settings: asMap(deepCopy(src.Inst.Settings)), RootPassword: src.RootPassword,
		MaintenanceVersion: src.Inst.MaintenanceVersion,
	}
	req.Settings["settingsVersion"] = "1"

	dst, err := b.newInstance(src.Inst.Project, &req)
	if err != nil {
		return nil, err
	}

	for name, d := range src.Databases {
		if d.System || (len(in.DatabaseNames) > 0 && !contains(in.DatabaseNames, name)) {
			continue
		}

		dst.Databases[name] = newDatabase(dst, name, d.Charset, d.Collation, false)
	}

	for key, u := range src.Users {
		cp := newUser(dst, u.User.Name, u.User.Host, u.User.Type)
		cp.User.PasswordPolicy, cp.User.DatabaseRoles, cp.Password = u.User.PasswordPolicy, u.User.DatabaseRoles, u.Password
		dst.Users[key] = cp
	}

	return dst, nil
}

func (b *InMemoryBackend) populateClone(ctx context.Context, src, dst *instRec, only []string) error {
	if err := b.bringUp(ctx, dst); err != nil {
		return err
	}

	srcEng, dstEng := b.engineOf(src), b.engineOf(dst)
	if srcEng == nil || dstEng == nil {
		return nil
	}

	dbs := b.exportableDBs(src, only)
	if len(dbs) == 0 {
		return nil
	}

	dump, err := srcEng.Dump(ctx, dbs, DumpOptions{Markers: true})
	if err != nil {
		return err
	}

	if err = dstEng.Restore(ctx, "", dump); err != nil {
		return err
	}

	return b.reconcile(ctx, dst, dstEng)
}
