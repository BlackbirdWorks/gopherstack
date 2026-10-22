package gcpcloudbuild

import (
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
)

const (
	maxTimeout      = 24 * time.Hour
	maxListItems    = 500
	maxSubstitution = 200
	maxFieldLen     = 4096
)

var stepIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)

func validateBuild(b *cloudbuildpb.Build) error {
	if len(b.GetSteps()) == 0 {
		return errInvalid("Invalid build: build must have at least one step.")
	}

	if len(b.GetSteps()) > maxSteps {
		return errInvalid("Invalid build: at most %d steps are allowed.", maxSteps)
	}

	if err := validateSteps(b.GetSteps()); err != nil {
		return err
	}

	if err := validateLimits(b); err != nil {
		return err
	}

	if err := validateSubstitutionKeys(b.GetSubstitutions()); err != nil {
		return err
	}

	if err := validateSecretEnvs(b); err != nil {
		return err
	}

	return validateArtifacts(b.GetArtifacts())
}

func validateSecretEnvs(b *cloudbuildpb.Build) error {
	defined := map[string]bool{}

	for _, s := range b.GetAvailableSecrets().GetSecretManager() {
		defined[s.GetEnv()] = true
	}

	for _, s := range b.GetAvailableSecrets().GetInline() {
		for env := range s.GetEnvMap() {
			defined[env] = true
		}
	}

	for _, s := range b.GetSecrets() {
		for env := range s.GetSecretEnv() {
			defined[env] = true
		}
	}

	for i, st := range b.GetSteps() {
		for _, env := range st.GetSecretEnv() {
			if !defined[env] {
				return errInvalid(
					"Invalid build: step %d: secret_env %q is not defined by available_secrets.",
					i,
					env,
				)
			}
		}
	}

	return nil
}

func validateLimits(b *cloudbuildpb.Build) error {
	if d := b.GetTimeout(); d != nil && (d.AsDuration() < 0 || d.AsDuration() > maxTimeout) {
		return errInvalid("Invalid build: timeout must be between 0 and 24h.")
	}

	if len(b.GetImages()) > maxListItems || len(b.GetTags()) > maxListItems ||
		len(b.GetSubstitutions()) > maxSubstitution {
		return errInvalid("Invalid build: too many images, tags or substitutions.")
	}

	for _, img := range b.GetImages() {
		if img == "" || len(img) > maxFieldLen {
			return errInvalid("Invalid build: images must be non-empty image names.")
		}
	}

	return nil
}

func validateSteps(steps []*cloudbuildpb.BuildStep) error {
	seen := map[string]bool{}

	for i, st := range steps {
		if strings.TrimSpace(st.GetName()) == "" {
			return errInvalid("Invalid build: step %d: name is required.", i)
		}

		if err := validateStepShape(i, st); err != nil {
			return err
		}

		if id := st.GetId(); id != "" {
			if !stepIDPattern.MatchString(id) {
				return errInvalid("Invalid build: step %d: invalid id %q.", i, id)
			}

			if seen[id] {
				return errInvalid("Invalid build: duplicate step id %q.", id)
			}
		}

		if err := validateWaitFor(i, st, seen); err != nil {
			return err
		}

		if id := st.GetId(); id != "" {
			seen[id] = true
		}
	}

	return nil
}

func validateStepShape(i int, st *cloudbuildpb.BuildStep) error {
	if st.GetScript() != "" && (len(st.GetArgs()) > 0 || st.GetEntrypoint() != "") {
		return errInvalid(
			"Invalid build: step %d: script cannot be combined with args or entrypoint.",
			i,
		)
	}

	if d := st.GetTimeout(); d != nil && (d.AsDuration() < 0 || d.AsDuration() > maxTimeout) {
		return errInvalid("Invalid build: step %d: timeout must be between 0 and 24h.", i)
	}

	for _, v := range st.GetVolumes() {
		if v.GetName() == "" || !strings.HasPrefix(v.GetPath(), "/") {
			return errInvalid(
				"Invalid build: step %d: volumes need a name and an absolute path.",
				i,
			)
		}
	}

	return nil
}

func validateWaitFor(i int, st *cloudbuildpb.BuildStep, earlier map[string]bool) error {
	for _, w := range st.GetWaitFor() {
		if w == "-" && len(st.GetWaitFor()) == 1 {
			continue
		}

		if !earlier[w] {
			return errInvalid(
				"Invalid build: step %d: wait_for %q must name an earlier step id.",
				i,
				w,
			)
		}
	}

	return nil
}

func validateArtifacts(a *cloudbuildpb.Artifacts) error {
	o := a.GetObjects()
	if o == nil {
		return nil
	}

	if !strings.HasPrefix(o.GetLocation(), "gs://") ||
		len(strings.TrimPrefix(o.GetLocation(), "gs://")) == 0 {
		return errInvalid("Invalid build: artifacts.objects.location must be a gs:// URL.")
	}

	if len(o.GetPaths()) > maxListItems {
		return errInvalid("Invalid build: too many artifacts.objects.paths.")
	}

	return nil
}

func validateSource(s *cloudbuildpb.Source) error {
	if ss := s.GetStorageSource(); ss != nil && (ss.GetBucket() == "" || ss.GetObject() == "") {
		return errInvalid("Invalid build: source.storage_source needs a bucket and an object.")
	}

	return nil
}
