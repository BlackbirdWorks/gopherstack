package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func statusReg() *enumRegistry {
	return &enumRegistry{
		membersByType: map[string]map[string]bool{
			"DomainPackageStatus": {
				"ASSOCIATING": true, "ASSOCIATION_FAILED": true, "ACTIVE": true,
				"DISSOCIATING": true, "DISSOCIATION_FAILED": true,
			},
			"OtherStatus": {"ACTIVE": true},
			// inspector2@v1.54.1 types/enums.go: the real "status" wire key's
			// candidates, trimmed to the three needed to exercise the
			// ambiguous-key check -- Status/DelegatedAdminStatus both declare
			// ENABLED, EcrRescanDurationStatus (SUCCESS/PENDING/FAILED) does not.
			"Status":                  {"ENABLED": true, "DISABLED": true},
			"DelegatedAdminStatus":    {"ENABLED": true, "DISABLE_IN_PROGRESS": true},
			"EcrRescanDurationStatus": {"SUCCESS": true, "PENDING": true, "FAILED": true},
		},
		constByIdent: map[string]enumConst{
			"DomainPackageStatusActive": {typeName: "DomainPackageStatus", value: "ACTIVE"},
		},
	}
}

func TestCheckLiteralsInFunc(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wireKeys      map[string]wireKeyFact
		name          string
		src           string
		wantKind      string
		wantValue     string
		wantConfident bool
	}{
		{
			// real shape: services/elasticsearch/handler_packages.go:187,
			// caught pre-fix -- DomainPackageStatus has no "DISSOCIATED"
			// member (only DISSOCIATING/DISSOCIATION_FAILED).
			name: "literal not in enum is confident",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": "DISSOCIATED"}
}`,
			wireKeys:      map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantKind:      kindLiteral,
			wantConfident: true,
			wantValue:     "DISSOCIATED",
		},
		{
			name: "literal in enum is clean",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": "ACTIVE"}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			name: "sdk enum member selector is clean",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": types.DomainPackageStatusActive}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			name: "empty string placeholder is never flagged",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": ""}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// real shape: services/apigateway/export.go's OpenAPI "type"
			// key, which collides in name only with API Gateway's own
			// DocumentationPartType/AuthorizerType/IntegrationType. Neither
			// candidate has DISSOCIATED, so this is needs-review, not clean.
			name: "ambiguous key with non-universal value is needs review",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": "DISSOCIATED"}
}`,
			wireKeys: map[string]wireKeyFact{
				"DomainPackageStatus": {Enums: []string{"DomainPackageStatus", "OtherStatus"}},
			},
			wantKind:  kindAmbiguousKey,
			wantValue: "DISSOCIATED",
		},
		{
			// ACTIVE is a real member of both candidates, so every possible
			// sense of this key accepts it -- no signal, stays clean.
			name: "ambiguous key with universal value is clean",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": "ACTIVE"}
}`,
			wireKeys: map[string]wireKeyFact{
				"DomainPackageStatus": {Enums: []string{"DomainPackageStatus", "OtherStatus"}},
			},
		},
		{
			// real shape: comprehend's "ErrorCode", a plain *string on one
			// struct and types.PageBasedErrorCode on an unrelated one.
			name: "polymorphic key with non-member value is needs review",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": "DISSOCIATED"}
}`,
			wireKeys: map[string]wireKeyFact{
				"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}, Polymorphic: true},
			},
			wantKind:  kindAmbiguousKey,
			wantValue: "DISSOCIATED",
		},
		{
			name: "polymorphic key with member value is clean",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": "ACTIVE"}
}`,
			wireKeys: map[string]wireKeyFact{
				"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}, Polymorphic: true},
			},
		},
		{
			// real shape: services/inspector2/handler_enablement.go:127 --
			// ecrConfiguration.rescanDurationState reused statusEnabled
			// ("ENABLED") under the "status" key. ENABLED is a real member
			// of two of the key's candidates (Status, DelegatedAdminStatus)
			// but not of the EcrRescanDurationStatus actually in play, so
			// the all-or-nothing filter dropped this bug silently -- this is
			// exactly the shape the ambiguous-key tier exists to catch.
			name: "inspector2 rescanDurationState status reuse is needs review",
			src: `package svc
const keyStatus = "status"
const statusEnabled = "ENABLED"
func build() map[string]any {
	return map[string]any{keyStatus: statusEnabled}
}`,
			wireKeys: map[string]wireKeyFact{
				"status": {Enums: []string{"Status", "DelegatedAdminStatus", "EcrRescanDurationStatus"}},
			},
			wantKind:  kindAmbiguousKey,
			wantValue: "ENABLED",
		},
		{
			// real shape: services/comprehend's Resource.Status
			// (gopherstack-3dzb) -- the wrong value is assigned to a
			// struct field, not written directly at the map[string]any
			// call site, so the pre-fix scan's resolveConstString never
			// sees a BasicLit/Ident/SelectorExpr it can resolve at the
			// value position and silently skips this map entry entirely.
			name: "value assigned to struct field then read into map is confident",
			src: `package svc
type Resource struct {
	Status string
}
func build() map[string]any {
	r := Resource{}
	r.Status = "DISSOCIATED"
	return map[string]any{"DomainPackageStatus": r.Status}
}`,
			wireKeys:      map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantKind:      kindLiteral,
			wantConfident: true,
			wantValue:     "DISSOCIATED",
		},
		{
			name: "struct field assigned a member value is clean",
			src: `package svc
type Resource struct {
	Status string
}
func build() map[string]any {
	r := Resource{}
	r.Status = "ACTIVE"
	return map[string]any{"DomainPackageStatus": r.Status}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// two different local variables sharing a field name ("Status")
			// must resolve independently -- field identity is scoped to the
			// (variable, field) pair, not the bare field name, so this must
			// NOT pick up dp.Status's DISSOCIATED value under other.Status's
			// read.
			name: "same field name on a different local variable does not collide",
			src: `package svc
type Resource struct {
	Status string
}
type Other struct {
	Status string
}
func build() map[string]any {
	dp := Resource{}
	dp.Status = "DISSOCIATED"
	other := Other{}
	other.Status = "ACTIVE"
	return map[string]any{"DomainPackageStatus": other.Status}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// a field assigned more than once in the function is ambiguous
			// dataflow (which assignment is live at the read?) -- single-hop
			// resolution refuses rather than guessing, same discipline as
			// the existing ident single-hop rule.
			name: "struct field reassigned more than once is never flagged",
			src: `package svc
type Resource struct {
	Status string
}
func build(cond bool) map[string]any {
	r := Resource{}
	r.Status = "DISSOCIATED"
	if cond {
		r.Status = "ACTIVE"
	}
	return map[string]any{"DomainPackageStatus": r.Status}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// real shape: an enum assigned via the SDK's own selector, then
			// carried through a struct field before reaching the wire key --
			// the value resolves as certainly as the direct-selector case
			// already covered above, just one hop further away.
			name: "sdk enum member selector assigned through a struct field is clean",
			src: `package svc
type Resource struct {
	Status string
}
func build() map[string]any {
	r := Resource{}
	r.Status = types.DomainPackageStatusActive
	return map[string]any{"DomainPackageStatus": r.Status}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// real shape: services/comprehend/handler_resources.go's
			// resourceMap (the actual gopherstack-3dzb/8f6239230 bug site):
			// `out := cloneMap(...); out["Status"] = ...` -- an
			// index-assignment onto an already-built map, never a
			// composite-literal KeyValueExpr, so checkLiteralsInFunc's
			// ast.Inspect(*ast.CompositeLit) never visits it at all.
			name: "index-assignment onto an existing map is confident",
			src: `package svc
func build() map[string]any {
	out := map[string]any{}
	out["DomainPackageStatus"] = "DISSOCIATED"
	return out
}`,
			wireKeys:      map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantKind:      kindLiteral,
			wantConfident: true,
			wantValue:     "DISSOCIATED",
		},
		{
			name: "index-assignment with a member value is clean",
			src: `package svc
func build() map[string]any {
	out := map[string]any{}
	out["DomainPackageStatus"] = "ACTIVE"
	return out
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// combines both new resolution paths: a struct field assigned an
			// SDK enum member, read back via index-assignment.
			name: "struct field read through an index-assignment is clean",
			src: `package svc
type Resource struct {
	Status string
}
func build() map[string]any {
	r := Resource{}
	r.Status = types.DomainPackageStatusActive
	out := map[string]any{}
	out["DomainPackageStatus"] = r.Status
	return out
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			name: "unresolvable runtime value is never flagged",
			src: `package svc
func build(status string) map[string]any {
	return map[string]any{"DomainPackageStatus": status}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// A nested closure's own local `status := "INVALID"` must not
			// leak into the enclosing function's const table: the outer
			// map[string]any{"DomainPackageStatus": status} reads the
			// outer runtime parameter, a different variable that merely
			// shares the closure-local's name (Go scoping, not aliasing).
			// Pre-fix, localStringConsts walked into the closure and
			// recorded vals["status"] = "INVALID" for the whole function,
			// producing a false confident finding here.
			name: "closure-local binding does not shadow an outer runtime parameter",
			src: `package svc
func build(status string) map[string]any {
	normalize := func() string {
		status := "INVALID"
		return status
	}
	_ = normalize()

	return map[string]any{"DomainPackageStatus": status}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			name: "unrelated key is never flagged",
			src: `package svc
func build() map[string]any {
	return map[string]any{"someOtherKey": "DISSOCIATED"}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(tc.src), 0o600))

			findings, err := scanPackage(dir, statusReg(), tc.wireKeys, dir)
			require.NoError(t, err)

			if tc.wantKind == "" {
				assert.Empty(t, findings)

				return
			}

			require.Len(t, findings, 1)
			got := findings[0]
			assert.Equal(t, tc.wantConfident, got.Confident)
			assert.Equal(t, tc.wantKind, got.Kind)
			assert.Equal(t, tc.wantValue, got.Value)
		})
	}
}

// TestCheckLiteralsInFunc_CrossModuleContamination is gopherstack-7fps's
// Class A: services/ec2 imports both the ec2 SDK and the outposts SDK;
// outposts' unrelated "ResourceType" enum (OUTPOST/ORDER) was the ONLY
// candidate the tool could see for an ec2 "ResourceType" wire key, since
// ec2's own ec2query/XML deserializers.go contributes nothing (outside this
// tool's JSON-family scope). These cases mirror that shape directly against
// enumRegistry.confidentModuleOK rather than real SDK fixtures.
func TestCheckLiteralsInFunc_CrossModuleContamination(t *testing.T) {
	t.Parallel()

	const src = `package svc
func build() map[string]any {
	return map[string]any{"ResourceType": "ec2:Instance"}
}`
	wireKeys := map[string]wireKeyFact{"ResourceType": {Enums: []string{"ResourceType"}}}

	baseReg := func() *enumRegistry {
		return &enumRegistry{
			membersByType: map[string]map[string]bool{
				"ResourceType": {"OUTPOST": true, "ORDER": true},
			},
			constByIdent: map[string]enumConst{},
		}
	}

	t.Run("sole candidate from a non-native secondary import is refused", func(t *testing.T) {
		t.Parallel()

		reg := baseReg()
		reg.nativeModules = map[string]bool{"ec2": true}
		reg.recordKeyEnumModule("ResourceType", "ResourceType", "outposts")

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(src), 0o600))

		findings, err := scanPackage(dir, reg, wireKeys, dir)
		require.NoError(t, err)
		assert.Empty(t, findings, "outposts' ResourceType is not native to an ec2 directory")
	})

	t.Run("sole candidate from the native module is still confident", func(t *testing.T) {
		t.Parallel()

		reg := baseReg()
		reg.nativeModules = map[string]bool{"ec2": true}
		reg.recordKeyEnumModule("ResourceType", "ResourceType", "ec2")

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(src), 0o600))

		findings, err := scanPackage(dir, reg, wireKeys, dir)
		require.NoError(t, err)
		require.Len(t, findings, 1, "a legitimate second-SDK enum native to this directory must still be caught")

		got := findings[0]
		assert.True(t, got.Confident)
		assert.Equal(t, kindLiteral, got.Kind)
		assert.Equal(t, "ec2:Instance", got.Value)
	})

	t.Run("empty nativeModules never refuses", func(t *testing.T) {
		t.Parallel()

		reg := baseReg()
		reg.recordKeyEnumModule("ResourceType", "ResourceType", "outposts")

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(src), 0o600))

		findings, err := scanPackage(dir, reg, wireKeys, dir)
		require.NoError(t, err)
		require.Len(
			t, findings, 1,
			"a directory whose own SDK module can't be positively named keeps its prior coverage",
		)
		assert.True(t, findings[0].Confident)
	})
}
