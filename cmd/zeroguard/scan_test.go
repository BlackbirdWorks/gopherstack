package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wantFinding struct {
	op        string
	field     string
	kind      string
	confident bool
}

func TestScanPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		src    string
		sdkOp  string
		sdkSrc string
		want   []wantFinding
	}{
		{
			// Pre-fix services/apigatewayv2/authorizers.go (commit
			// 406c1dcc3^): UpdateAuthorizerInput declared
			// AuthorizerResultTTLInSeconds int32 and EnableSimpleResponses
			// bool, guarded by non-zero/truthy checks. The real SDK's
			// UpdateAuthorizerInput (api_op_UpdateAuthorizer.go) declares
			// both as pointers -- an explicit 0/false was silently dropped.
			// This is the validation bar: the tool must flag both fields.
			name:  "apigatewayv2 update authorizer pre fix flags both fields",
			sdkOp: "UpdateAuthorizer",
			sdkSrc: `package apigatewayv2

type UpdateAuthorizerInput struct {
	AuthorizerResultTtlInSeconds *int32
	EnableSimpleResponses        *bool
}
`,
			src: `package apigatewayv2

type UpdateAuthorizerInput struct {
	AuthorizerResultTTLInSeconds int32
	EnableSimpleResponses        bool
}

func (b *InMemoryBackend) UpdateAuthorizer(
	apiID, authorizerID string,
	input UpdateAuthorizerInput,
) (*Authorizer, error) {
	a := &Authorizer{}

	if input.AuthorizerResultTTLInSeconds != 0 {
		a.AuthorizerResultTTLInSeconds = input.AuthorizerResultTTLInSeconds
	}

	if input.EnableSimpleResponses {
		a.EnableSimpleResponses = input.EnableSimpleResponses
	}

	return a, nil
}
`,
			want: []wantFinding{
				{op: "UpdateAuthorizer", field: "AuthorizerResultTTLInSeconds", kind: kindConfident, confident: true},
				{op: "UpdateAuthorizer", field: "EnableSimpleResponses", kind: kindConfident, confident: true},
			},
		},
		{
			// Post-fix (406c1dcc3): both fields are *int32/*bool, guarded by
			// a nil check and dereferenced. Must NOT be flagged.
			name:  "apigatewayv2 update authorizer post fix flags nothing",
			sdkOp: "UpdateAuthorizer",
			sdkSrc: `package apigatewayv2

type UpdateAuthorizerInput struct {
	AuthorizerResultTtlInSeconds *int32
	EnableSimpleResponses        *bool
}
`,
			src: `package apigatewayv2

type UpdateAuthorizerInput struct {
	AuthorizerResultTTLInSeconds *int32
	EnableSimpleResponses        *bool
}

func (b *InMemoryBackend) UpdateAuthorizer(
	apiID, authorizerID string,
	input UpdateAuthorizerInput,
) (*Authorizer, error) {
	a := &Authorizer{}

	if input.AuthorizerResultTTLInSeconds != nil {
		a.AuthorizerResultTTLInSeconds = *input.AuthorizerResultTTLInSeconds
	}

	if input.EnableSimpleResponses != nil {
		a.EnableSimpleResponses = *input.EnableSimpleResponses
	}

	return a, nil
}
`,
			want: nil,
		},
		{
			// Stage.AutoDeploy already avoids this class: UpdateStageInput
			// declares AutoDeploy *bool (this package's own correct pattern,
			// cited in 406c1dcc3's commit message as "sitting one file
			// away" from the bug it fixed).
			name:  "apigatewayv2 update stage autodeploy pointer pattern flags nothing",
			sdkOp: "UpdateStage",
			sdkSrc: `package apigatewayv2

type UpdateStageInput struct {
	AutoDeploy *bool
}
`,
			src: `package apigatewayv2

type UpdateStageInput struct {
	AutoDeploy *bool
}

func (b *InMemoryBackend) UpdateStage(apiID, stageName string, input UpdateStageInput) (*Stage, error) {
	s := &Stage{}

	if input.AutoDeploy != nil {
		s.AutoDeploy = *input.AutoDeploy
	}

	return s, nil
}
`,
			want: nil,
		},
		{
			name:  "plain field mismatch with no guard is needs review",
			sdkOp: "UpdateWidget",
			sdkSrc: `package testsvc

type UpdateWidgetInput struct {
	Name *string
}
`,
			src: `package testsvc

type UpdateWidgetInput struct {
	Name string
}

func (b *InMemoryBackend) UpdateWidget(id string, input UpdateWidgetInput) (*Widget, error) {
	w := &Widget{}
	w.Name = input.Name

	return w, nil
}
`,
			want: []wantFinding{
				{op: "UpdateWidget", field: "Name", kind: kindTypeMismatch, confident: false},
			},
		},
		{
			// A Create op takes a fresh resource with no prior state an
			// omission could erase -- out of updatePrefixes scope even
			// though the same zero-guard shape appears.
			name:  "create op is out of scope even with a zero guard",
			sdkOp: "CreateWidget",
			sdkSrc: `package testsvc

type CreateWidgetInput struct {
	Name *string
}
`,
			src: `package testsvc

type CreateWidgetInput struct {
	Name string
}

func (b *InMemoryBackend) CreateWidget(input CreateWidgetInput) (*Widget, error) {
	w := &Widget{}

	if input.Name != "" {
		w.Name = input.Name
	}

	return w, nil
}
`,
			want: nil,
		},
		{
			// servicediscovery.UpdateService's real shape (gopherstack-hwyq):
			// omitted DnsConfig/HealthCheckConfig should delete existing
			// state in real AWS, but gopherstack leaves it untouched. The
			// guard here is a nil check on an already-pointer parameter, and
			// the func doesn't even take an "...Input" struct -- a
			// different shape (cascading a delete on an omitted nested
			// struct) than this tool's scalar zero-guard signal covers, so
			// it correctly produces nothing rather than a wrong finding.
			name:  "servicediscovery nested pointer struct shape is out of scope",
			sdkOp: "UpdateService",
			sdkSrc: `package servicediscovery

type UpdateServiceInput struct {
	Id *string
}
`,
			src: `package servicediscovery

type DNSConfig struct {
	DNSRecords []string
}

func (b *InMemoryBackend) UpdateService(id, description string, dnsConfig *DNSConfig) (string, error) {
	if dnsConfig != nil {
		_ = dnsConfig
	}

	return "", nil
}
`,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svcDir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(svcDir, "fixture.go"), []byte(tt.src), 0o600))

			sdkDir := t.TempDir()
			require.NoError(
				t,
				os.WriteFile(filepath.Join(sdkDir, "api_op_"+tt.sdkOp+".go"), []byte(tt.sdkSrc), 0o600),
			)

			mods := []sdkModule{{name: "testsvc", path: sdkDir}}

			got, err := scanPackage(svcDir, svcDir, mods, newSDKOpFieldCache())
			require.NoError(t, err)

			assert.Equal(t, tt.want, stripPositions(got))
		})
	}
}

func stripPositions(findings []finding) []wantFinding {
	if len(findings) == 0 {
		return nil
	}

	out := make([]wantFinding, len(findings))
	for i, f := range findings {
		out[i] = wantFinding{op: f.Op, field: f.Field, kind: f.Kind, confident: f.Confident}
	}

	return out
}
