package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	signinbackend "github.com/blackbirdworks/gopherstack/services/signin"
)

type signinOrganizations struct {
	org organizationsbackend.StorageBackend
}

func (s signinOrganizations) HasOrganization(id string) bool {
	o, err := s.org.DescribeOrganization()

	return err == nil && o.ID == id
}

func (s signinOrganizations) HasRoot(id string) bool {
	roots, err := s.org.ListRoots()
	if err != nil {
		return false
	}

	for _, r := range roots {
		if r.ID == id {
			return true
		}
	}

	return false
}

// wireSignin validates console authorization organization and root targets against Organizations.
func wireSignin(byName map[string]service.Registerable) {
	h, ok := byName["Signin"].(*signinbackend.Handler)
	if !ok {
		return
	}

	if o, orgOK := byName["Organizations"].(*organizationsbackend.Handler); orgOK && o.Backend != nil {
		h.Backend.SetOrganizationResolver(signinOrganizations{org: o.Backend})
	}
}
