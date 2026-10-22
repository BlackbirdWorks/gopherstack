package gcpcloudbuild

import "cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"

// ExtractArchive unpacks a source archive into dest.
func ExtractArchive(data []byte, dest string) error { return extractArchive(data, dest) }

// RegistryTarget maps an Artifact Registry image onto the emulator's address.
func RegistryTarget(addr, image string) (string, error) { return registryTarget(addr, image) }

// Substitute expands substitutions in the string fields of build.
func Substitute(
	b *cloudbuildpb.Build,
	vals map[string]string,
) error {
	return substituteBuild(b, vals)
}

// ProjectNumber derives the project number of a project id.
func ProjectNumber(project string) string { return projectNumber(project) }

// Dependencies returns the step indexes step i waits for.
func Dependencies(steps []*cloudbuildpb.BuildStep, i int) []int { return dependencies(steps, i) }
