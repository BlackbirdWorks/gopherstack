package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mapperReasonFor(t *testing.T, cands []candidate, code string) string {
	t.Helper()

	for _, c := range cands {
		if c.Code == code {
			return c.MapperReason
		}
	}

	require.Failf(t, "no candidate found", "code %q, candidates: %+v", code, cands)

	return ""
}

// TestDemoteMapperConsumedSentinels_RDSShape pins the exact structural
// signature this pass was built to see through: services/rds/errors.go
// declares ErrSubnetGroupNotFound's own message as "DBSubnetGroupNotFound"
// (matching mechAwserrNew), but rdsErrorCode's local `errorMapping` table
// (handler_dispatch.go) maps that SAME sentinel to the wire code
// "DBSubnetGroupNotFoundFault" -- a suffix mismatch, and the exact
// false-positive class this tool's first pass on rds mistook for a bug.
// The sentinel's own literal must be demoted (never confidently reported on
// its own text); the mapper's OUTPUT literal must survive as its own,
// separately-checkable candidate.
func TestDemoteMapperConsumedSentinels_RDSShape(t *testing.T) {
	t.Parallel()

	src := `package rds

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var ErrSubnetGroupNotFound = awserr.New("DBSubnetGroupNotFound", awserr.ErrNotFound)
var ErrInstanceNotFound = awserr.New("DBInstanceNotFound", awserr.ErrNotFound)

func rdsErrorCode(opErr error) string {
	type errorMapping struct {
		sentinel error
		code     string
	}

	mappings := []errorMapping{
		{ErrSubnetGroupNotFound, "DBSubnetGroupNotFoundFault"},
		{ErrInstanceNotFound, "DBInstanceNotFound"},
	}

	for _, m := range mappings {
		if errors.Is(opErr, m.sentinel) {
			return m.code
		}
	}

	return ""
}
`

	cands := extractFixture(t, src)

	assert.NotEmpty(
		t,
		mapperReasonFor(t, cands, "DBSubnetGroupNotFound"),
		"ErrSubnetGroupNotFound's own declared literal is only matched by errors.Is "+
			"identity in the table below; it must be demoted, not trusted on its own text",
	)
	assert.NotEmpty(
		t,
		mapperReasonFor(t, cands, "DBInstanceNotFound"),
		"ErrInstanceNotFound's declaration must be demoted even though its text "+
			"happens to equal the mapper's own output for this row",
	)

	assert.Empty(
		t,
		mapperReasonFor(t, cands, "DBSubnetGroupNotFoundFault"),
		"the mapper's own OUTPUT literal is real signal and must never be demoted",
	)
}

// TestDemoteMapperConsumedSentinels_NeptuneShape pins neptune's own
// version of the same table shape (neptuneErrorCode in handler.go): a
// second, independently-declared local errorMapping struct in a different
// service, confirming the detection is structural (keyed on the
// error-field/string-field struct shape and errors.Is usage) and not
// hardcoded to rds's own function or type names.
func TestDemoteMapperConsumedSentinels_NeptuneShape(t *testing.T) {
	t.Parallel()

	src := `package neptune

import "errors"

var ErrClusterParameterGroupNotFound = errors.New("DBClusterParameterGroupNotFound")

func neptuneErrorCode(opErr error) string {
	type errorMapping struct {
		sentinel error
		code     string
	}

	mappings := []errorMapping{
		{ErrClusterParameterGroupNotFound, "DBParameterGroupNotFound"},
	}

	for _, m := range mappings {
		if errors.Is(opErr, m.sentinel) {
			return m.code
		}
	}

	return ""
}
`

	cands := extractFixture(t, src)

	assert.NotEmpty(
		t,
		mapperReasonFor(t, cands, "DBClusterParameterGroupNotFound"),
		"neptune has no distinct cluster-parameter-group fault; its sentinel's own "+
			"text must not be trusted since the table reuses the plain code instead",
	)
	assert.Empty(t, mapperReasonFor(t, cands, "DBParameterGroupNotFound"))
}

// TestDemoteMapperConsumedSentinels_SwitchShape pins fis's classifyError
// shape: a switch whose cases match errors.Is directly (no table at all)
// and whose branches return a struct literal carrying the real code in a
// field with no "Code"/"Type" name at all (fis's own "exceptionType").
func TestDemoteMapperConsumedSentinels_SwitchShape(t *testing.T) {
	t.Parallel()

	src := `package fis

import "errors"

var ErrTemplateNotFound = errors.New("ExperimentTemplateNotFound")

type errorClass struct {
	exceptionType string
	httpStatus    int
}

func classifyError(err error) errorClass {
	switch {
	case errors.Is(err, ErrTemplateNotFound):
		return errorClass{exceptionType: "ResourceNotFoundException", httpStatus: 404}
	default:
		return errorClass{exceptionType: "InternalServerError", httpStatus: 500}
	}
}
`

	cands := extractFixture(t, src)

	assert.NotEmpty(
		t,
		mapperReasonFor(t, cands, "ExperimentTemplateNotFound"),
		"fis's own sentinel text is never itself an AWS FIS exception type",
	)
	assert.Empty(t, mapperReasonFor(t, cands, "ResourceNotFoundException"))
}

// TestDemoteMapperConsumedSentinels_PerCallSiteShape pins elasticache's
// shape: no central mapper function at all -- errors.Is guards a hardcoded
// literal at each call site, scattered across ordinary handler functions
// whose own names never mention "error".
func TestDemoteMapperConsumedSentinels_PerCallSiteShape(t *testing.T) {
	t.Parallel()

	src := `package elasticache

import "errors"

var ErrReplicationGroupNotFound = errors.New("ReplicationGroupNotFound")

type xmlErrorDetail struct {
	Code    string
	Message string
}

func xmlError(c int, status int, code, message string) error {
	_ = xmlErrorDetail{Code: code, Message: message}

	return nil
}

func deleteSnapshot(c int, err error) error {
	if errors.Is(err, ErrReplicationGroupNotFound) {
		return xmlError(c, 404, "ReplicationGroupNotFoundFault", "not found")
	}

	return nil
}
`

	cands := extractFixture(t, src)

	assert.NotEmpty(
		t,
		mapperReasonFor(t, cands, "ReplicationGroupNotFound"),
		"the sentinel's own text drops the real Fault suffix every call site actually emits",
	)
	assert.Empty(t, mapperReasonFor(t, cands, "ReplicationGroupNotFoundFault"))
}

// TestDemoteMapperConsumedSentinels_NoMapperUnaffected pins ecs's own
// shape as the negative case: a sentinel whose text is read back out via
// its own error chain (never matched by errors.Is against the specific
// per-resource sentinel) must never be demoted. This is what the ECS
// validation bar (scan_test.go) exercises end-to-end; this test isolates
// the same guarantee at the extractCandidates layer.
func TestDemoteMapperConsumedSentinels_NoMapperUnaffected(t *testing.T) {
	t.Parallel()

	src := `package ecs

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var ErrClusterAlreadyExists = awserr.New("ClusterAlreadyExistsException", awserr.ErrAlreadyExists)
`

	cands := extractFixture(t, src)

	assert.Empty(t, mapperReasonFor(t, cands, "ClusterAlreadyExistsException"))
}
