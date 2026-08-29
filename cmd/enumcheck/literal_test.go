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
		},
		constByIdent: map[string]enumConst{
			"DomainPackageStatusActive": {typeName: "DomainPackageStatus", value: "ACTIVE"},
		},
	}
}

func TestCheckLiteralsInFunc(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wireKeys  map[string]wireKeyFact
		name      string
		src       string
		wantValue string
		wantHit   bool
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
			wireKeys:  map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantHit:   true,
			wantValue: "DISSOCIATED",
		},
		{
			name: "literal in enum is clean",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": "ACTIVE"}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantHit:  false,
		},
		{
			name: "sdk enum member selector is clean",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": types.DomainPackageStatusActive}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantHit:  false,
		},
		{
			name: "empty string placeholder is never flagged",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": ""}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantHit:  false,
		},
		{
			// real shape: services/apigateway/export.go's OpenAPI "type"
			// key, which collides in name only with API Gateway's own
			// DocumentationPartType/AuthorizerType/IntegrationType.
			name: "ambiguous key is never flagged",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": "DISSOCIATED"}
}`,
			wireKeys: map[string]wireKeyFact{
				"DomainPackageStatus": {Enums: []string{"DomainPackageStatus", "OtherStatus"}},
			},
			wantHit: false,
		},
		{
			// real shape: comprehend's "ErrorCode", a plain *string on one
			// struct and types.PageBasedErrorCode on an unrelated one.
			name: "polymorphic key is never flagged",
			src: `package svc
func build() map[string]any {
	return map[string]any{"DomainPackageStatus": "DISSOCIATED"}
}`,
			wireKeys: map[string]wireKeyFact{
				"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}, Polymorphic: true},
			},
			wantHit: false,
		},
		{
			name: "unresolvable runtime value is never flagged",
			src: `package svc
func build(status string) map[string]any {
	return map[string]any{"DomainPackageStatus": status}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantHit:  false,
		},
		{
			name: "unrelated key is never flagged",
			src: `package svc
func build() map[string]any {
	return map[string]any{"someOtherKey": "DISSOCIATED"}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantHit:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(tc.src), 0o600))

			findings, err := scanPackage(dir, statusReg(), tc.wireKeys, dir)
			require.NoError(t, err)

			if !tc.wantHit {
				assert.Empty(t, findings)

				return
			}

			require.Len(t, findings, 1)
			got := findings[0]
			assert.True(t, got.Confident)
			assert.Equal(t, kindLiteral, got.Kind)
			assert.Equal(t, tc.wantValue, got.Value)
		})
	}
}
