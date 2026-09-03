package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseErrorCodeMethods_RealShape uses the real codegen shape read live
// from iam@v1.58.1/types/errors.go: ErrorCode()'s fallback branch returns a
// bare literal that can differ from the Go type name
// (NoSuchEntityException.ErrorCode() returns "NoSuchEntity"), and the
// override branch returns a pointer deref that must never be collected as
// a literal.
func TestParseErrorCodeMethods_RealShape(t *testing.T) {
	t.Parallel()

	src := `package types

type NoSuchEntityException struct {
	Message           *string
	ErrorCodeOverride *string
}

func (e *NoSuchEntityException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "NoSuchEntity"
	}
	return *e.ErrorCodeOverride
}

type EntityAlreadyExistsException struct {
	Message           *string
	ErrorCodeOverride *string
}

func (e *EntityAlreadyExistsException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "EntityAlreadyExists"
	}
	return *e.ErrorCodeOverride
}
`

	dir := t.TempDir()
	path := filepath.Join(dir, "errors.go")
	require.NoError(t, os.WriteFile(path, []byte(src), 0o600))

	codes, err := parseErrorCodeMethods(path)
	require.NoError(t, err)

	assert.True(t, codes["NoSuchEntity"], "expected the ErrorCode() literal, not the type name")
	assert.True(t, codes["EntityAlreadyExists"])
	assert.False(t, codes["NoSuchEntityException"], "the Go type name is not itself a wire code")
}

func TestParseDeserializerCodes_MatchedAndUnmatched(t *testing.T) {
	t.Parallel()

	src := `package pkg

func awsAwsjson11_deserializeOpErrorCreateCluster(response *smithyhttp.Response, metadata *middleware.Metadata) error {
	switch {
	case strings.EqualFold("AccessDeniedException", errorCode):
		return nil
	case strings.EqualFold("ClientException", errorCode):
		return nil
	default:
		return nil
	}
}

func awsAwsjson11_deserializeOpErrorDeleteCluster(response *smithyhttp.Response, metadata *middleware.Metadata) error {
	switch {
	default:
		return nil
	}
}

func unrelatedHelper() {
	_ = strings.EqualFold("NotACode", "x")
}
`

	dir := t.TempDir()
	path := filepath.Join(dir, "deserializers.go")
	require.NoError(t, os.WriteFile(path, []byte(src), 0o600))

	codes, opFuncs, matchedOpFuncs, err := parseDeserializerCodes(path)
	require.NoError(t, err)

	assert.True(t, codes["AccessDeniedException"])
	assert.True(t, codes["ClientException"])
	assert.False(t, codes["NotACode"], "an EqualFold call outside a deserializeOpError* function is out of scope")
	assert.Equal(t, 2, opFuncs)
	assert.Equal(t, 1, matchedOpFuncs, "DeleteCluster's switch models no code at all")
}

func TestModuleCodes_SparselyModeled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		opFuncs        int
		matchedOpFuncs int
		want           bool
	}{
		{name: "s3-like 18 percent coverage is sparse", opFuncs: 112, matchedOpFuncs: 20, want: true},
		{name: "cloudformation-like 69 percent is not sparse", opFuncs: 90, matchedOpFuncs: 62, want: false},
		{name: "ecs-like 100 percent is not sparse", opFuncs: 77, matchedOpFuncs: 77, want: false},
		{name: "no op functions at all is not sparse", opFuncs: 0, matchedOpFuncs: 0, want: false},
		{name: "exactly at the threshold is not sparse", opFuncs: 10, matchedOpFuncs: 5, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mc := &moduleCodes{opFuncs: tt.opFuncs, matchedOpFuncs: tt.matchedOpFuncs}
			assert.Equal(t, tt.want, mc.sparselyModeled())
		})
	}
}
