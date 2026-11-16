package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpsqlbackend "github.com/blackbirdworks/gopherstack/services/gcpsql"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

// wireGCPSQL connects Cloud SQL export and import to the GCS emulator and executeSql passwords to Secret Manager.
func wireGCPSQL(byName map[string]service.Registerable) {
	sqlH, ok := byName["GCPSQL"].(*gcpsqlbackend.Handler)
	if !ok {
		return
	}

	if gcs, isGCS := byName["GCPStorage"].(*gcpstoragebackend.Handler); isGCS {
		sqlH.Backend.SetObjectStore(gcsObjectStore{b: gcs.Backend})
	}

	if sm, isSM := byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler); isSM {
		sqlH.Backend.SetSecretReader(cloudBuildSecrets{b: sm.Backend})
		sm.Backend.SetSQLPasswordSetter(sqlUserPasswords{b: sqlH.Backend})
	}
}

type sqlUserPasswords struct {
	b *gcpsqlbackend.InMemoryBackend
}

func (s sqlUserPasswords) SetUserPassword(project, instance, user, password string) error {
	_, err := s.b.UpdateUser(project, instance, user, "", gcpsqlbackend.UserInput{Password: password})

	return err
}
