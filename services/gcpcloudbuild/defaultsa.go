package gcpcloudbuild

import (
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
)

const kwDefaultSA = "defaultServiceAccount"

// GetDefaultServiceAccount returns the service account builds of a project run as: the Compute Engine default
// service account, which has replaced the legacy Cloud Build account for new projects.
func (b *InMemoryBackend) GetDefaultServiceAccount(
	in *cloudbuildpb.GetDefaultServiceAccountRequest,
) (*cloudbuildpb.DefaultServiceAccount, error) {
	parts := strings.Split(in.GetName(), "/")

	if len(parts) != 5 || parts[0] != kwProjects || parts[2] != kwLocations || parts[4] != kwDefaultSA ||
		parts[1] == "" || !locationPattern.MatchString(parts[3]) {
		return nil, errInvalid(
			"Invalid name %q: want projects/{project}/locations/{location}/defaultServiceAccount.", in.GetName())
	}

	return &cloudbuildpb.DefaultServiceAccount{
		Name: in.GetName(),
		ServiceAccountEmail: "projects/" + parts[1] + "/serviceAccounts/" +
			projectNumber(parts[1]) + "-compute@developer.gserviceaccount.com",
	}, nil
}
