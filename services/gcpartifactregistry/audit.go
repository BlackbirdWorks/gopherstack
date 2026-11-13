package gcpartifactregistry

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

func auditPolicy() gcpaudit.Policy {
	dataWrites := []string{
		"CreateTag", "UpdateTag", "DeleteTag", "DeletePackage", "UpdatePackage", "DeleteVersion", "UpdateVersion",
		"BatchDeleteVersions", "DeleteFile", "UpdateFile", "CreateAttachment", "DeleteAttachment", "CreateRule",
		"UpdateRule", "DeleteRule", "ImportAptArtifacts", "ImportYumArtifacts", "ExportArtifact",
	}

	data := make(map[string]gcpaudit.Kind, len(dataWrites))
	for _, m := range dataWrites {
		data[m] = gcpaudit.DataWrite
	}

	return gcpaudit.Policy{
		ServiceName: "artifactregistry.googleapis.com",
		AdminPrefixes: []string{
			"CreateRepository", "UpdateRepository", "DeleteRepository", "SetIamPolicy", "UpdateProjectSettings",
			"UpdateVPCSCConfig",
		},
		Data: data,
	}
}

func (b *InMemoryBackend) auditNow() time.Time {
	b.mu.RLock("auditNow")
	defer b.mu.RUnlock()

	return b.now()
}
