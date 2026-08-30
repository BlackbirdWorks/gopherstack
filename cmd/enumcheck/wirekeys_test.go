package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// guarddutyEnumsFixture is a trimmed but real-shaped types/enums.go: every
// declared string enum in this codegen is `type X string` plus a `const (
// XFoo X = "FOO"; ... )` block repeating the type on every line.
const guarddutyEnumsFixture = `package types

type DataSource string

const (
	DataSourceFlowLogs DataSource = "FLOW_LOGS"
	DataSourceS3Logs   DataSource = "S3_LOGS"
)

type UsageFeature string

const (
	UsageFeatureS3DataEvents UsageFeature = "S3_DATA_EVENTS"
)
`

func TestLoadEnumRegistry(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "enums.go"), []byte(guarddutyEnumsFixture), 0o600))

	reg, err := loadEnumRegistry(filepath.Join(dir, "enums.go"))
	require.NoError(t, err)

	assert.Equal(t, map[string]bool{"FLOW_LOGS": true, "S3_LOGS": true}, reg.membersByType["DataSource"])
	assert.Equal(t, map[string]bool{"S3_DATA_EVENTS": true}, reg.membersByType["UsageFeature"])
	assert.Equal(t, enumConst{typeName: "DataSource", value: "FLOW_LOGS"}, reg.constByIdent["DataSourceFlowLogs"])
}

// deserializersFixture mirrors the real codegen shape this scan depends on:
// the enum-conversion assignment is nested inside `if value != nil { ... }`,
// never a direct top-level statement in the case body -- a real generated
// deserializer never assigns the zero value on a nil field. Missing this
// nesting was an early bug in wireEnumKeys that made it resolve zero wire
// keys against every real pinned SDK (caught live against
// guardduty@v1.85.4, whose "dataSource"/"feature" both nest exactly this
// way); this fixture pins the regression.
const deserializersFixture = `package guardduty

func deserializeDocumentUsageDataSourceResult(v **types.UsageDataSourceResult, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	var sv *types.UsageDataSourceResult
	for key, value := range shape {
		switch key {
		case "dataSource":
			if value != nil {
				jtv, ok := value.(string)
				if !ok {
					return fmt.Errorf("bad")
				}
				sv.DataSource = types.DataSource(jtv)
			}
		case "total":
			if err := deserializeDocumentTotal(&sv.Total, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func deserializeDocumentUsageFeatureResult(v **types.UsageFeatureResult, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	var sv *types.UsageFeatureResult
	for key, value := range shape {
		switch key {
		case "feature":
			if value != nil {
				jtv, ok := value.(string)
				if !ok {
					return fmt.Errorf("bad")
				}
				sv.Feature = types.UsageFeature(jtv)
			}
		}
	}
	return nil
}

func deserializeDocumentFreeTrialFeature(v **types.FreeTrialFeature, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	var sv *types.FreeTrialFeature
	for key, value := range shape {
		switch key {
		case "feature":
			if value != nil {
				jtv, ok := value.(string)
				if !ok {
					return fmt.Errorf("bad")
				}
				sv.Feature = ptr.String(jtv)
			}
		}
	}
	return nil
}
`

func TestWireEnumKeys(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deserializers.go"), []byte(deserializersFixture), 0o600))

	reg := &enumRegistry{
		membersByType: map[string]map[string]bool{
			"DataSource":   {"FLOW_LOGS": true, "S3_LOGS": true},
			"UsageFeature": {"S3_DATA_EVENTS": true},
		},
		constByIdent: map[string]enumConst{},
	}

	got, fields, err := wireGroundTruth(filepath.Join(dir, "deserializers.go"), reg)
	require.NoError(t, err)

	require.Contains(t, got, "dataSource")
	assert.Equal(t, []string{"DataSource"}, got["dataSource"].Enums)
	assert.False(t, got["dataSource"].Polymorphic)

	require.Contains(t, got, "feature")
	assert.Equal(t, []string{"UsageFeature"}, got["feature"].Enums)
	assert.True(t, got["feature"].Polymorphic, "feature also deserializes as a plain *string on FreeTrialFeature")

	assert.NotContains(t, got, "total", "a nested-object case contributes no enum candidate")

	assert.Equal(t, map[string]bool{"dataSource": true, "total": true}, fields["UsageDataSourceResult"])
	assert.Equal(t, map[string]bool{"feature": true}, fields["UsageFeatureResult"])
	assert.Equal(t, map[string]bool{"feature": true}, fields["FreeTrialFeature"])
}

// jobTypesFixture mirrors amplify's real shape: Job wraps Steps []Step and
// Summary *JobSummary, and Job's own Status/Type fields actually live on
// the nested JobSummary, never on Job directly.
const jobTypesFixture = `package types

type Job struct {
	Steps   []Step
	Summary *JobSummary
	noSmithyDocumentSerde
}

type JobSummary struct {
	Status JobStatus
	Type   JobType
}

type Step struct {
	StepName *string
}
`

func TestLoadNestedTypeRefs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "types.go"), []byte(jobTypesFixture), 0o600))

	refs, err := loadNestedTypeRefs(filepath.Join(dir, "types.go"))
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"Step", "JobSummary"}, refs["Job"])
	// namedTypeRef doesn't distinguish an enum type name from a struct type
	// name -- harmless, since expandOneHopNestedFields only ever looks up
	// direct[refType], and an enum type name is never a key in direct (only
	// deserializeDocument<Type> functions -- one per real STRUCT type --
	// populate it).
	assert.ElementsMatch(t, []string{"JobStatus", "JobType"}, refs["JobSummary"])
}

func TestExpandOneHopNestedFields(t *testing.T) {
	t.Parallel()

	direct := map[string]map[string]bool{
		"Job":        {"steps": true, "summary": true},
		"JobSummary": {"status": true, "type": true},
	}
	refs := map[string][]string{"Job": {"Step", "JobSummary"}}

	got := expandOneHopNestedFields(direct, refs)

	assert.Equal(
		t, map[string]bool{"steps": true, "summary": true, "status": true, "type": true}, got["Job"],
		"Job's flattened field set includes its one-hop nested JobSummary's own fields",
	)
	assert.Equal(t, map[string]bool{"status": true, "type": true}, got["JobSummary"], "unaffected: no refs of its own")
}
