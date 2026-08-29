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
			name: "unresolvable runtime value is never flagged",
			src: `package svc
func build(status string) map[string]any {
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
