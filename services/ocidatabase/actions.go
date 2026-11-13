package ocidatabase

import "slices"

// guard finds db, checks the etag and the state and returns it for modification. Callers hold the write lock.
func (b *InMemoryBackend) guard(id, ifMatch string, states ...string) (*ADB, *APIError) {
	db, aerr := b.activeDB(id)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = checkEtag(db.Etag, ifMatch); aerr != nil {
		return nil, aerr
	}

	if slices.Contains(states, db.State) {
		return db, nil
	}

	return nil, errState("The Autonomous Database " + id + " is in state " + db.State + " and cannot do this operation")
}

func (b *InMemoryBackend) transient(
	id, ifMatch string, from []string, s step, pre func(*ADB) *APIError,
) (adbWire, string, string, error) {
	b.mu.Lock(s.typ)
	defer b.mu.Unlock()

	db, aerr := b.guard(id, ifMatch, from...)
	if aerr != nil {
		return adbWire{}, "", "", aerr
	}

	if pre != nil {
		if aerr = pre(db); aerr != nil {
			return adbWire{}, "", "", aerr
		}
	}

	wr := b.begin(db, s)

	return b.view(db), wr.ID, db.Etag, nil
}

func (b *InMemoryBackend) startable(db *ADB) *APIError {
	if b.engineOn(db) && b.eng.live[db.ID] == nil && b.eng.passwords[db.ID] == "" {
		return errState(
			"The ADMIN password is not known after a restore; update the database with an adminPassword first",
		)
	}

	return nil
}

// StartDatabase starts a stopped database.
func (b *InMemoryBackend) StartDatabase(id, ifMatch string) (adbWire, string, string, error) {
	return b.transient(id, ifMatch, []string{stateStopped}, step{
		typ: "START_AUTONOMOUS_DATABASE", action: wrInProgress, state: stateStarting, target: stateAvailable,
		work: b.startWork(id),
	}, b.startable)
}

// StopDatabase stops an available database.
func (b *InMemoryBackend) StopDatabase(id, ifMatch string) (adbWire, string, string, error) {
	return b.transient(id, ifMatch, []string{stateAvailable}, step{
		typ: "STOP_AUTONOMOUS_DATABASE", action: wrInProgress, state: stateStopping, target: stateStopped,
		work: b.stopWork(id),
	}, nil)
}

// RestartDatabase restarts an available database.
func (b *InMemoryBackend) RestartDatabase(id, ifMatch string) (adbWire, string, string, error) {
	return b.transient(id, ifMatch, []string{stateAvailable}, step{
		typ: "RESTART_AUTONOMOUS_DATABASE", action: wrInProgress, state: stateRestarting, target: stateAvailable,
		work: b.restartWork(id),
	}, nil)
}

// RestoreDatabase restores an available database to a point in time. Only the lifecycle is emulated: no data
// changes.
func (b *InMemoryBackend) RestoreDatabase(id, ifMatch string, in restoreIn) (adbWire, string, string, error) {
	return b.transient(id, ifMatch, []string{stateAvailable}, step{
		typ: "RESTORE_AUTONOMOUS_DATABASE", action: wrInProgress, state: stateRestoring, target: stateAvailable,
	}, func(db *ADB) *APIError { return b.checkRestore(db, in) })
}

func (b *InMemoryBackend) checkRestore(db *ADB, in restoreIn) *APIError {
	if db.IsFreeTier {
		return errParam("Always Free databases do not support restore")
	}

	given := 0

	for _, ok := range []bool{in.Timestamp != nil, in.SCN != nil, deref(in.Latest, false)} {
		if ok {
			given++
		}
	}

	if given != 1 {
		return errParam("exactly one of timestamp, databaseSCN and latest is required")
	}

	if in.Timestamp != nil && (in.Timestamp.After(b.nowUTC()) || in.Timestamp.Before(db.Created)) {
		return errParam("timestamp must be between the creation time of the database and now")
	}

	return nil
}

// TerminateDatabase deletes a database; its listeners and container go with it.
func (b *InMemoryBackend) TerminateDatabase(id, ifMatch string, dropLongTerm bool) (string, error) {
	b.mu.Lock("TerminateDatabase")
	defer b.mu.Unlock()

	db, aerr := b.guard(id, ifMatch, stateAvailable, stateStopped, stateUnavailable)
	if aerr != nil {
		return "", aerr
	}

	for _, bk := range b.backups {
		if bk.DBID == id && bk.State != backupDeleted && (dropLongTerm || bk.Type != backupLongTerm) {
			b.dropBackup(bk)
		}
	}

	wr := b.begin(db, step{
		typ: "DELETE_AUTONOMOUS_DATABASE", action: "DELETED", state: stateTerminating, target: stateTerminated,
		work: b.terminateWork(id),
	})

	delete(b.pki, id)

	return wr.ID, nil
}

// ChangeCompartment moves a database and its backups.
func (b *InMemoryBackend) ChangeCompartment(id, ifMatch, compartment string) (string, string, error) {
	b.mu.Lock("ChangeCompartment")
	defer b.mu.Unlock()

	db, aerr := b.guard(id, ifMatch, stateAvailable, stateStopped, stateUnavailable)
	if aerr != nil {
		return "", "", aerr
	}

	if aerr = b.checkCompartment(compartment); aerr != nil {
		return "", "", aerr
	}

	db.CompartmentID, db.Etag = compartment, newEtag()

	for _, bk := range b.backups {
		if bk.DBID == id {
			bk.CompartmentID = compartment
		}
	}

	return b.instant(db, "CHANGE_AUTONOMOUS_DATABASE_COMPARTMENT").ID, db.Etag, nil
}

func (b *InMemoryBackend) dropBackup(bk *Backup) {
	at := b.nowUTC().Add(b.transition)
	bk.State, bk.TransitionAt = backupDeleting, &at
}
