package gcpsecretmanager

import (
	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

func auditPolicy() gcpaudit.Policy {
	return gcpaudit.Policy{
		ServiceName:   "secretmanager.googleapis.com",
		AdminPrefixes: []string{"Create", "Update", "Delete", "Add", "Enable", "Disable", "Destroy", "SetIamPolicy"},
		Data:          map[string]gcpaudit.Kind{"AccessSecretVersion": gcpaudit.DataRead},
	}
}
