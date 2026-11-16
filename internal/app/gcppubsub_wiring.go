package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpiambackend "github.com/blackbirdworks/gopherstack/services/gcpiam"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

// wireGCPPubSubIdentity gives push OIDC tokens the unique id of the service account in the IAM emulator.
func wireGCPPubSubIdentity(byName map[string]service.Registerable) {
	ps, ok := byName["GCPPubSub"].(*gcppubsubbackend.Handler)
	if !ok {
		return
	}

	iam, ok := byName["GCPIAM"].(*gcpiambackend.Handler)
	if !ok || iam.Backend == nil {
		return
	}

	ps.Backend.SetServiceAccountResolver(func(email string) (string, bool) {
		sa, err := iam.Backend.GetServiceAccount("projects/-/serviceAccounts/" + email)
		if err != nil {
			return "", false
		}

		return sa.GetUniqueId(), true
	})
}
