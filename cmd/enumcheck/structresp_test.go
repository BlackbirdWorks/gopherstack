package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckStructResponsesInFunc(t *testing.T) {
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
			// the blind spot itself: a named response struct's own composite
			// literal, never a map[string]any -- gopherstack's real
			// `c.JSON(http.StatusOK, SomeType{...})` convention.
			name: "bad value on a tagged struct field response is confident",
			src: `package svc
type GetThingOutput struct {
	DomainPackageStatus string ` + "`json:\"DomainPackageStatus\"`" + `
}
func build() *GetThingOutput {
	return &GetThingOutput{DomainPackageStatus: "DISSOCIATED"}
}`,
			wireKeys:      map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantKind:      kindLiteral,
			wantConfident: true,
			wantValue:     "DISSOCIATED",
		},
		{
			name: "member value on a tagged struct field response is clean",
			src: `package svc
type GetThingOutput struct {
	DomainPackageStatus string ` + "`json:\"DomainPackageStatus\"`" + `
}
func build() *GetThingOutput {
	return &GetThingOutput{DomainPackageStatus: "ACTIVE"}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// real shape: services/lambda's StatementID field, tagged
			// `json:"StatementId"` -- the Go name and the wire name differ, so
			// resolution must read the tag rather than assume they match.
			name: "wire key resolves from json tag, not the Go field name",
			src: `package svc
type Thing struct {
	GoFieldName string ` + "`json:\"DomainPackageStatus\"`" + `
}
func build() *Thing {
	return &Thing{GoFieldName: "DISSOCIATED"}
}`,
			wireKeys:      map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantKind:      kindLiteral,
			wantConfident: true,
			wantValue:     "DISSOCIATED",
		},
		{
			// no tag at all: encoding/json's own default is the Go field
			// name verbatim.
			name: "untagged field falls back to the Go field name as wire key",
			src: `package svc
type Thing struct {
	DomainPackageStatus string
}
func build() *Thing {
	return &Thing{DomainPackageStatus: "DISSOCIATED"}
}`,
			wireKeys:      map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantKind:      kindLiteral,
			wantConfident: true,
			wantValue:     "DISSOCIATED",
		},
		{
			// json:"-" removes the field from the wire entirely -- even
			// though its Go name matches a real wire key, it must never be
			// checked against that key.
			name: "json dash tag excludes the field from wire matching",
			src: `package svc
type Thing struct {
	DomainPackageStatus string ` + "`json:\"-\"`" + `
}
func build() *Thing {
	return &Thing{DomainPackageStatus: "DISSOCIATED"}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// two struct types sharing a bare field name ("Status") must
			// resolve independently through their own tags -- field identity
			// is (struct type, field), never the bare name, the same
			// discipline localFieldConsts already applies to (variable,
			// field) within a function.
			name: "two struct types sharing a field name resolve to different wire keys without collision",
			src: `package svc
type Alpha struct {
	Status string ` + "`json:\"DomainPackageStatus\"`" + `
}
type Beta struct {
	Status string ` + "`json:\"OtherKey\"`" + `
}
func build() (*Alpha, *Beta) {
	a := &Alpha{Status: "ACTIVE"}
	b := &Beta{Status: "DISSOCIATED"}
	return a, b
}`,
			wireKeys: map[string]wireKeyFact{
				"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}},
				"OtherKey":            {Enums: []string{"DomainPackageStatus"}},
			},
			wantKind:      kindLiteral,
			wantConfident: true,
			wantValue:     "DISSOCIATED",
		},
		{
			// nested struct literal: the sub-struct field's own value is a
			// separate CompositeLit ast.Inspect reaches on its own, no extra
			// handling required.
			name: "nested struct literal field is reached",
			src: `package svc
type Inner struct {
	DomainPackageStatus string ` + "`json:\"DomainPackageStatus\"`" + `
}
type Outer struct {
	Configuration *Inner ` + "`json:\"Configuration\"`" + `
}
func build() *Outer {
	return &Outer{Configuration: &Inner{DomainPackageStatus: "DISSOCIATED"}}
}`,
			wireKeys:      map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantKind:      kindLiteral,
			wantConfident: true,
			wantValue:     "DISSOCIATED",
		},
		{
			// a value carried through a struct-field local var (the
			// gopherstack-3dzb single-hop resolution) must also resolve when
			// it lands on a NAMED struct response field, not only a
			// map[string]any entry.
			name: "value carried through a local struct field resolves into a response struct field",
			src: `package svc
type Resource struct {
	Status string
}
type GetThingOutput struct {
	DomainPackageStatus string ` + "`json:\"DomainPackageStatus\"`" + `
}
func build() *GetThingOutput {
	r := Resource{}
	r.Status = "DISSOCIATED"
	return &GetThingOutput{DomainPackageStatus: r.Status}
}`,
			wireKeys:      map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
			wantKind:      kindLiteral,
			wantConfident: true,
			wantValue:     "DISSOCIATED",
		},
		{
			// unkeyed (positional) struct literal element: no field identity
			// to resolve a wire name from, so it must be skipped, never
			// mis-flagged and never a crash.
			name: "positional struct literal element is never flagged",
			src: `package svc
type GetThingOutput struct {
	DomainPackageStatus string ` + "`json:\"DomainPackageStatus\"`" + `
}
func build() *GetThingOutput {
	return &GetThingOutput{"DISSOCIATED"}
}`,
			wireKeys: map[string]wireKeyFact{"DomainPackageStatus": {Enums: []string{"DomainPackageStatus"}}},
		},
		{
			// ambiguous-key tier must fire through this path exactly like it
			// does for the map[string]any path -- same evalKeyValue decision,
			// reused rather than reimplemented.
			name: "ambiguous key on a struct field is needs review",
			src: `package svc
type GetThingOutput struct {
	DomainPackageStatus string ` + "`json:\"DomainPackageStatus\"`" + `
}
func build() *GetThingOutput {
	return &GetThingOutput{DomainPackageStatus: "DISSOCIATED"}
}`,
			wireKeys: map[string]wireKeyFact{
				"DomainPackageStatus": {Enums: []string{"DomainPackageStatus", "OtherStatus"}},
			},
			wantKind:  kindAmbiguousKey,
			wantValue: "DISSOCIATED",
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

// TestCheckStructResponsesInFunc_PhantomField is gopherstack-7fps's Class B:
// cloudtrail's Event.EventCategory (real types.Event has no such field; a
// naive key-name match against "EventCategory" elsewhere in the SDK found
// EventCategoryAggregation's unrelated enum) and sagemaker's
// PipelineExecutionStep.StepType (same shape, matched enum was Inference
// Recommender's). Mirrors that shape directly against
// enumRegistry.wireFieldsByType.
func TestCheckStructResponsesInFunc_PhantomField(t *testing.T) {
	t.Parallel()

	wireKeys := map[string]wireKeyFact{"EventCategory": {Enums: []string{"EventCategoryAggregation"}}}

	regWithRealType := func() *enumRegistry {
		reg := statusReg()
		reg.wireFieldsByType = map[string]map[string]bool{
			// real types.Event's own field set -- no EventCategory at all.
			"Event": {"EventId": true, "EventName": true, "EventSource": true},
		}

		return reg
	}

	t.Run("field absent from the real same-named type is a phantom field, not a wrong value", func(t *testing.T) {
		t.Parallel()

		const src = `package svc
type Event struct {
	EventCategory string ` + "`json:\"EventCategory\"`" + `
}
func build() *Event {
	return &Event{EventCategory: "Management"}
}`

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(src), 0o600))

		findings, err := scanPackage(dir, regWithRealType(), wireKeys, dir)
		require.NoError(t, err)
		require.Len(t, findings, 1)

		got := findings[0]
		assert.False(
			t,
			got.Confident,
			"phantom-field is never confident: the enum it would compare against is unrelated",
		)
		assert.Equal(t, kindPhantomField, got.Kind)
		assert.Equal(t, "EventCategory", got.Key)
		assert.Equal(t, "Management", got.Value)
		assert.Equal(t, "Event", got.Enum)
	})

	t.Run("field present on the real same-named type is checked normally, not treated as phantom", func(t *testing.T) {
		t.Parallel()

		const src = `package svc
type Event struct {
	EventId string ` + "`json:\"EventId\"`" + `
}
func build() *Event {
	return &Event{EventId: "abc"}
}`

		reg := regWithRealType()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(src), 0o600))

		findings, err := scanPackage(dir, reg, map[string]wireKeyFact{}, dir)
		require.NoError(t, err)
		assert.Empty(
			t,
			findings,
			"EventId is a real field on Event -- no phantom finding, and no wireKeys entry to check it against",
		)
	})

	t.Run("struct type with no real same-named type gets no phantom finding", func(t *testing.T) {
		t.Parallel()

		const src = `package svc
type ImageReferenceEntry struct {
	EventCategory string ` + "`json:\"EventCategory\"`" + `
}
func build() *ImageReferenceEntry {
	return &ImageReferenceEntry{EventCategory: "Management"}
}`

		reg := regWithRealType()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(src), 0o600))

		findings, err := scanPackage(dir, reg, wireKeys, dir)
		require.NoError(t, err)
		require.Len(
			t,
			findings,
			1,
			"no real-type ground truth for ImageReferenceEntry, so this falls through to the ordinary check",
		)

		got := findings[0]
		assert.Equal(t, kindLiteral, got.Kind)
		assert.True(t, got.Confident)
	})
}
