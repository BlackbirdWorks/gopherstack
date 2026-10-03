package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fixtureSpecPath      = "testdata/cfn_resource_spec.json"
	cloudformationSrcDir = "../../services/cloudformation"
	committedTablePath   = "../../services/cloudformation/cfn_attributes.json"
)

// TestGeneratedTableUpToDate guards gopherstack-erj2j: regenerating from the
// committed spec fixture and the current services/cloudformation source must
// reproduce services/cloudformation/cfn_attributes.json byte-for-byte. A
// mismatch means someone added a resTypeXxx constant (or the fixture
// changed) without running `make cfn-attrs-gen`, so coverage silently
// drifted from what the source now supports.
func TestGeneratedTableUpToDate(t *testing.T) {
	t.Parallel()

	spec, err := loadSpec(fixtureSpecPath)
	require.NoError(t, err)

	files, err := parseDir(cloudformationSrcDir)
	require.NoError(t, err)

	supportedTypes := collectResourceTypeConsts(files)
	require.NotEmpty(t, supportedTypes, "expected to find resTypeXxx constants in %s", cloudformationSrcDir)

	table := buildTable(spec, supportedTypes)

	got, err := renderJSON(table)
	require.NoError(t, err)

	want, err := os.ReadFile(committedTablePath)
	require.NoError(t, err)

	assert.Equal(t, string(want), string(got),
		"cfn_attributes.json is stale -- run `make cfn-attrs-gen` and commit the result")
}

// TestBuildTable_CoversEverySupportedSpecType asserts buildTable never drops
// a type the provisioner supports (a declared resTypeXxx constant) once the
// spec documents a non-empty Attributes set for it -- the exact regression
// class that shrank coverage from 97 to 59 types before this fix.
func TestBuildTable_CoversEverySupportedSpecType(t *testing.T) {
	t.Parallel()

	spec, err := loadSpec(fixtureSpecPath)
	require.NoError(t, err)

	files, err := parseDir(cloudformationSrcDir)
	require.NoError(t, err)

	supportedTypes := collectResourceTypeConsts(files)
	table := buildTable(spec, supportedTypes)

	for value := range supportedTypes {
		rt, inSpec := spec.ResourceTypes[value]
		if !inSpec || len(rt.Attributes) == 0 {
			continue
		}

		assert.Contains(t, table, value, "supported type documented in the spec must be in the table")
	}
}

func TestRenderJSON_Deterministic(t *testing.T) {
	t.Parallel()

	table := map[string][]string{
		"AWS::S3::Bucket": {"Arn", "DomainName"},
		"AWS::SNS::Topic": {"TopicArn"},
	}

	first, err := renderJSON(table)
	require.NoError(t, err)

	second, err := renderJSON(table)
	require.NoError(t, err)

	assert.Equal(t, first, second)

	var roundTrip map[string][]string
	require.NoError(t, json.Unmarshal(first, &roundTrip))
	assert.Equal(t, table, roundTrip)
}
