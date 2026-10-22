package gcpcloudbuild

import (
	"regexp"
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
)

var (
	substRef     = regexp.MustCompile(`\$\$|\$\{([A-Z_][A-Z0-9_]*)\}|\$([A-Z_][A-Z0-9_]*)`)
	userSubstKey = regexp.MustCompile(`^_[A-Z0-9_]+$`)
)

// substituter expands $VAR and ${VAR} references; $$ is a literal dollar sign.
type substituter struct {
	err   error
	vals  map[string]string
	loose bool
}

func (s *substituter) expand(in string) string {
	if !strings.Contains(in, "$") {
		return in
	}

	return substRef.ReplaceAllStringFunc(in, func(m string) string {
		if m == "$$" {
			return "$"
		}

		name := strings.Trim(m, "${}")
		if v, ok := s.vals[name]; ok {
			return v
		}

		if s.loose && strings.HasPrefix(name, "_") {
			return ""
		}

		if s.err == nil {
			s.err = errInvalid(
				"key in the template %q is not matched in the substitutions map.",
				name,
			)
		}

		return m
	})
}

func (s *substituter) all(in []string) {
	for i := range in {
		in[i] = s.expand(in[i])
	}
}

func validateSubstitutionKeys(subs map[string]string) error {
	for k := range subs {
		if !userSubstKey.MatchString(k) {
			return errInvalid(
				"key in the substitution map %q is not a valid user-defined substitution: "+
					"keys must start with an underscore and use only A-Z, 0-9 and _.",
				k,
			)
		}
	}

	return nil
}

// substituteBuild expands substitutions in place across every user-supplied field that supports them.
func substituteBuild(b *cloudbuildpb.Build, vals map[string]string) error {
	s := &substituter{
		vals: vals, loose: b.GetOptions().GetSubstitutionOption() == cloudbuildpb.BuildOptions_ALLOW_LOOSE,
	}

	for _, st := range b.GetSteps() {
		st.Name, st.Dir, st.Id, st.Entrypoint, st.Script = s.expand(
			st.GetName(),
		), s.expand(
			st.GetDir(),
		),
			s.expand(
				st.GetId(),
			), s.expand(
				st.GetEntrypoint(),
			), s.expand(
				st.GetScript(),
			)
		s.all(st.GetArgs())
		s.all(st.GetEnv())
		s.all(st.GetWaitFor())
		s.all(st.GetSecretEnv())
	}

	s.all(b.GetImages())
	s.all(b.GetTags())
	b.LogsBucket = s.expand(b.GetLogsBucket())
	b.ServiceAccount = s.expand(b.GetServiceAccount())

	substituteArtifacts(b.GetArtifacts(), s)

	if o := b.GetOptions(); o != nil {
		s.all(o.GetEnv())

		if o.GetPool() != nil {
			o.Pool.Name = s.expand(o.GetPool().GetName())
		}
	}

	if ss := b.GetSource().GetStorageSource(); ss != nil {
		ss.Bucket, ss.Object = s.expand(ss.GetBucket()), s.expand(ss.GetObject())
	}

	return s.err
}

func substituteArtifacts(a *cloudbuildpb.Artifacts, s *substituter) {
	if a == nil {
		return
	}

	s.all(a.GetImages())

	if o := a.GetObjects(); o != nil {
		o.Location = s.expand(o.GetLocation())
		s.all(o.GetPaths())
	}
}
