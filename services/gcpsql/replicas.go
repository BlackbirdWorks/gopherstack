package gcpsql

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	instanceTypePrimary = "CLOUD_SQL_INSTANCE"
	instanceTypeReplica = "READ_REPLICA_INSTANCE"
	primaryPollEvery    = 200 * time.Millisecond
	serverIDMin         = 2
	serverIDSpan        = 1<<31 - serverIDMin
)

func randomServerID() uint32 {
	var buf [4]byte

	_, _ = rand.Read(buf[:])

	return binary.BigEndian.Uint32(buf[:])%serverIDSpan + serverIDMin
}

// masterName strips the optional "project:" prefix of masterInstanceName.
func masterName(project, v string) (string, error) {
	p, name, found := strings.Cut(v, ":")
	if !found {
		return v, nil
	}

	if p != project {
		return "", errInvalid("Read replicas in another project are not supported by the emulator.")
	}

	return name, nil
}

func (b *InMemoryBackend) isReplica(rec *instRec) bool { return rec.Inst.MasterInstanceName != "" }

func binlogOn(settings map[string]any) bool {
	v, _ := asMap(settings["backupConfiguration"])["binaryLogEnabled"].(bool)

	return v
}

// prepareReplicaLocked validates a replica request against its primary and fills what it inherits.
func (b *InMemoryBackend) prepareReplicaLocked(project string, in *InstanceInput) (*instRec, error) {
	if in.MasterInstanceName == "" {
		return nil, errInvalid("masterInstanceName is required for a read replica.")
	}

	if in.InstanceType != "" && in.InstanceType != instanceTypeReplica {
		return nil, errInvalid(
			"instanceType %q does not match a replica of %q.",
			in.InstanceType,
			in.MasterInstanceName,
		)
	}

	name, err := masterName(project, in.MasterInstanceName)
	if err != nil {
		return nil, err
	}

	prim, err := b.lookupLocked(project, name)
	if err != nil {
		return nil, err
	}

	if prim.Inst.State != stateRunnable {
		return nil, errNotRunnable()
	}

	if b.isReplica(prim) {
		return nil, errInvalid("Replicas of replicas are not supported by the emulator.")
	}

	if err = inheritFromPrimary(in, prim); err != nil {
		return nil, err
	}

	in.MasterInstanceName = name

	return prim, nil
}

// inheritFromPrimary fills the fields a replica takes from its primary and rejects mismatches.
func inheritFromPrimary(in *InstanceInput, prim *instRec) error {
	if in.DatabaseVersion == "" {
		in.DatabaseVersion = prim.Inst.DatabaseVersion
	}

	if in.Region == "" {
		in.Region = prim.Inst.Region
	}

	if in.DatabaseVersion != prim.Inst.DatabaseVersion || in.Region != prim.Inst.Region {
		return errInvalid("The database version and region of a read replica must match its primary.")
	}

	if !isPG(prim.Inst.DatabaseVersion) && !binlogOn(prim.Inst.Settings) {
		return errInvalid("Binary logging must be enabled on the primary instance to create a MySQL read replica.")
	}

	if in.Settings == nil {
		in.Settings = map[string]any{}
	}

	if settingString(in.Settings, "tier") == "" {
		in.Settings["tier"] = settingString(prim.Inst.Settings, "tier")
	}

	in.RootPassword = prim.RootPassword

	return nil
}

// linkReplicaLocked records the replica on its primary and copies the primary's databases and accounts.
func (b *InMemoryBackend) linkReplicaLocked(prim, rep *instRec) {
	rep.Inst.InstanceType = instanceTypeReplica
	rep.Inst.MasterInstanceName = prim.Inst.Project + ":" + prim.Inst.Name
	rep.Inst.ReplicaConfiguration = map[string]any{keyKind: "sql#replicaConfiguration"}

	prim.Inst.ReplicaNames = append(prim.Inst.ReplicaNames, rep.Inst.Name)
	slices.Sort(prim.Inst.ReplicaNames)
	b.touchLocked(prim)
	b.mirrorLocked(prim, rep)
}

// mirrorLocked makes dst's database and account records equal to src's.
func (b *InMemoryBackend) mirrorLocked(src, dst *instRec) {
	dst.Databases = map[string]*Database{}
	dst.Users = map[string]*userRec{}

	for name, d := range src.Databases {
		dst.Databases[name] = newDatabase(dst, name, d.Charset, d.Collation, d.System)
	}

	for key, u := range src.Users {
		cp := newUser(dst, u.User.Name, u.User.Host, u.User.Type)
		cp.User.PasswordPolicy, cp.User.DatabaseRoles, cp.Password = u.User.PasswordPolicy, u.User.DatabaseRoles, u.Password
		dst.Users[key] = cp
	}
}

// syncReplicasLocked mirrors a primary's records onto its replicas after a successful task.
func (b *InMemoryBackend) syncReplicasLocked(prim *instRec) {
	for _, n := range prim.Inst.ReplicaNames {
		if rep, ok := b.instances[instKey(prim.Inst.Project, n)]; ok && !rep.gone {
			b.mirrorLocked(prim, rep)
		}
	}
}

func (b *InMemoryBackend) unlinkReplicaLocked(rep *instRec) {
	name, err := masterName(rep.Inst.Project, rep.Inst.MasterInstanceName)
	if err != nil {
		return
	}

	if prim, ok := b.instances[instKey(rep.Inst.Project, name)]; ok {
		prim.Inst.ReplicaNames = slices.DeleteFunc(
			prim.Inst.ReplicaNames,
			func(n string) bool { return n == rep.Inst.Name },
		)
		b.touchLocked(prim)
	}
}

// primaryEngine waits for the primary of rec to settle; a nil engine with a nil error means the primary is
// state-only.
func (b *InMemoryBackend) primaryEngine(ctx context.Context, rec *instRec) (Engine, error) {
	project := rec.Inst.Project

	name, err := masterName(project, rec.Inst.MasterInstanceName)
	if err != nil {
		return nil, err
	}

	tick := time.NewTicker(primaryPollEvery)
	defer tick.Stop()

	for {
		b.mu.RLock("primaryEngine")
		prim, ok := b.instances[instKey(project, name)]

		var (
			eng   Engine
			state string
		)

		if ok && !prim.gone {
			eng, state = prim.eng, prim.Inst.State
		}
		b.mu.RUnlock()

		switch {
		case !ok:
			return nil, errNoInstance()
		case eng != nil:
			return eng, nil
		case state == stateRunnable:
			return nil, nil //nolint:nilnil // state-only primary
		case state == stateFailed || state == stateSuspended || state == stateDeleting:
			return nil, fmt.Errorf("%w: primary %s is %s", errExec, name, state)
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for primary %s: %w", name, ctx.Err())
		case <-tick.C:
		}
	}
}

func (b *InMemoryBackend) replicaOnly(project, name string) (*instRec, error) {
	rec, err := b.opRec(project, name)
	if err != nil {
		return nil, err
	}

	if !b.isReplica(rec) {
		return nil, errInvalid("The instance is not a read replica.")
	}

	return rec, nil
}

// PromoteReplica turns a read replica into a standalone primary.
func (b *InMemoryBackend) PromoteReplica(project, name string) (*Operation, error) {
	b.mu.Lock("PromoteReplica")
	defer b.mu.Unlock()

	rec, err := b.replicaOnly(project, name)
	if err != nil {
		return nil, err
	}

	return b.submitLocked(rec, project, "PROMOTE_REPLICA", opMeta{}, func(ctx context.Context) error {
		if eng := b.engineOf(rec); eng != nil {
			rp, ok := eng.(Replicator)
			if !ok {
				return errNoReplication
			}

			if perr := rp.Promote(ctx); perr != nil {
				return perr
			}
		}

		b.mu.Lock("PromoteReplica.task")
		defer b.mu.Unlock()

		b.unlinkReplicaLocked(rec)
		rec.Inst.MasterInstanceName, rec.Inst.ReplicaConfiguration = "", nil
		rec.Inst.InstanceType, rec.ReplicaPaused = instanceTypePrimary, false
		b.touchLocked(rec)

		return nil
	}), nil
}

// SetReplicaRunning starts (true) or stops (false) replication on a read replica.
func (b *InMemoryBackend) SetReplicaRunning(project, name string, on bool) (*Operation, error) {
	b.mu.Lock("SetReplicaRunning")
	defer b.mu.Unlock()

	rec, err := b.replicaOnly(project, name)
	if err != nil {
		return nil, err
	}

	opType := "STOP_REPLICA"
	if on {
		opType = "START_REPLICA"
	}

	return b.submitLocked(rec, project, opType, opMeta{}, func(ctx context.Context) error {
		if eng := b.engineOf(rec); eng != nil {
			rp, ok := eng.(Replicator)
			if !ok {
				return errNoReplication
			}

			if serr := rp.SetReplication(ctx, on); serr != nil {
				return serr
			}
		}

		b.mu.Lock("SetReplicaRunning.task")
		rec.ReplicaPaused = !on
		b.mu.Unlock()

		return nil
	}), nil
}

// ReplicaStatus reports a replica's replication health; it errors for non-replicas and state-only instances.
func (b *InMemoryBackend) ReplicaStatus(ctx context.Context, project, name string) (bool, float64, error) {
	b.mu.RLock("ReplicaStatus")
	rec, err := b.replicaOnly(project, name)

	var eng Engine
	if err == nil {
		eng = rec.eng
	}
	b.mu.RUnlock()

	if err != nil {
		return false, 0, err
	}

	rp, ok := eng.(Replicator)
	if !ok {
		return false, 0, errNoReplication
	}

	st, err := rp.ReplicationStatus(ctx)
	if err != nil {
		return false, 0, err
	}

	return st.Replicating, st.LagSeconds, nil
}
