package dashboard

import (
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const cbMasked = "***"

// cbSecretName reports whether a substitution or environment variable name looks like it holds a secret.
func cbSecretName(name string) bool {
	u := strings.ToUpper(name)

	for _, w := range []string{"SECRET", "PASSWORD", "PASSWD", "TOKEN", "KEY", "CREDENTIAL", "PRIVATE"} {
		if strings.Contains(u, w) {
			return true
		}
	}

	return false
}

func cbMaskSubstitutions(subs map[string]string) {
	for k := range subs {
		if cbSecretName(k) {
			subs[k] = cbMasked
		}
	}
}

// cbMaskBuild clears KMS-encrypted secretEnv ciphertexts (names stay) and the values of secret-looking
// substitutions and step environment variables.
func cbMaskBuild(b *cloudbuildpb.Build) *cloudbuildpb.Build {
	if b == nil {
		return nil
	}

	out, _ := proto.Clone(b).(*cloudbuildpb.Build)

	for _, s := range out.GetSecrets() {
		for k := range s.GetSecretEnv() {
			s.SecretEnv[k] = nil
		}
	}

	for _, in := range out.GetAvailableSecrets().GetInline() {
		for k := range in.GetEnvMap() {
			in.EnvMap[k] = nil
		}
	}

	cbMaskSubstitutions(out.GetSubstitutions())

	for _, step := range out.GetSteps() {
		for i, e := range step.GetEnv() {
			if k, _, ok := strings.Cut(e, "="); ok && cbSecretName(k) {
				step.Env[i] = k + "=" + cbMasked
			}
		}
	}

	return out
}

func cbMaskTrigger(t *cloudbuildpb.BuildTrigger) *cloudbuildpb.BuildTrigger {
	if t == nil {
		return nil
	}

	out, _ := proto.Clone(t).(*cloudbuildpb.BuildTrigger)
	cbMaskSubstitutions(out.GetSubstitutions())

	if b := out.GetBuild(); b != nil {
		out.BuildTemplate = &cloudbuildpb.BuildTrigger_Build{Build: cbMaskBuild(b)}
	}

	return out
}

func cbMaskAny(a *anypb.Any) *anypb.Any {
	if a == nil {
		return nil
	}

	m, err := a.UnmarshalNew()
	if err != nil {
		return a
	}

	masked := cbMaskMessage(m)

	out, err := anypb.New(masked)
	if err != nil {
		return a
	}

	return out
}

// cbMaskMessage returns a copy of m that is safe to show: Build, BuildTrigger, their list responses and the
// operations wrapping them. Other messages pass through unchanged.
func cbMaskMessage(m proto.Message) proto.Message {
	switch t := m.(type) {
	case *cloudbuildpb.Build:
		return cbMaskBuild(t)
	case *cloudbuildpb.BuildTrigger:
		return cbMaskTrigger(t)
	case *cloudbuildpb.BuildOperationMetadata:
		out, _ := proto.Clone(t).(*cloudbuildpb.BuildOperationMetadata)
		out.Build = cbMaskBuild(t.GetBuild())

		return out
	case *cloudbuildpb.ListBuildsResponse:
		out, _ := proto.Clone(t).(*cloudbuildpb.ListBuildsResponse)
		for i, b := range out.GetBuilds() {
			out.Builds[i] = cbMaskBuild(b)
		}

		return out
	case *cloudbuildpb.ListBuildTriggersResponse:
		out, _ := proto.Clone(t).(*cloudbuildpb.ListBuildTriggersResponse)
		for i, tr := range out.GetTriggers() {
			out.Triggers[i] = cbMaskTrigger(tr)
		}

		return out
	case *longrunningpb.Operation:
		out, _ := proto.Clone(t).(*longrunningpb.Operation)
		out.Metadata = cbMaskAny(t.GetMetadata())

		if r, ok := t.GetResult().(*longrunningpb.Operation_Response); ok {
			out.Result = &longrunningpb.Operation_Response{Response: cbMaskAny(r.Response)}
		}

		return out
	}

	return m
}
