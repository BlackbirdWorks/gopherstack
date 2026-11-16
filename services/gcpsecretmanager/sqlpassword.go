package gcpsecretmanager

import (
	"strings"
)

// SQLPasswordSetter changes the password of a Cloud SQL database user.
type SQLPasswordSetter interface {
	SetUserPassword(project, instance, user, password string) error
}

// SetSQLPasswordSetter wires managed rotation to the Cloud SQL emulator; nil keeps rotation to the secret only.
func (b *InMemoryBackend) SetSQLPasswordSetter(s SQLPasswordSetter) {
	b.mu.Lock("SetSQLPasswordSetter")
	defer b.mu.Unlock()

	b.sqlPw = s
}

// applySQLPassword runs without the backend lock held because the setter may reach another service.
func (b *InMemoryBackend) applySQLPassword(parent, instance, user string, password []byte) error {
	b.mu.RLock("applySQLPassword")
	setter := b.sqlPw
	b.mu.RUnlock()

	if setter == nil {
		return nil
	}

	project := strings.Split(parent, "/")[1]
	instance = instance[strings.LastIndex(instance, ":")+1:]

	if err := setter.SetUserPassword(project, instance, user, string(password)); err != nil {
		return errManaged("Could not set the password of user %q on Cloud SQL instance %q: %v.", user, instance, err)
	}

	return nil
}
