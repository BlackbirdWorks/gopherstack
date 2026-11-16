package ocidatabase

import "context"

// VerifyAdb reports whether an Autonomous Database exists and can take a rotated ADMIN password.
func (b *InMemoryBackend) VerifyAdb(_ context.Context, id string) error {
	b.mu.RLock("VerifyAdb")
	defer b.mu.RUnlock()

	db, aerr := b.activeDB(id)
	if aerr != nil {
		return aerr
	}

	if db.State != stateAvailable {
		return errState("The Autonomous Database " + id + " is in state " + db.State)
	}

	return nil
}

// SetAdbAdminPassword changes the ADMIN password of an Autonomous Database, as a secret rotation does.
func (b *InMemoryBackend) SetAdbAdminPassword(_ context.Context, id, password string) error {
	_, _, _, err := b.UpdateDatabase(id, "", updateIn{AdminPassword: &password})

	return err
}
