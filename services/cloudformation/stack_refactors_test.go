package cloudformation_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStackRefactor covers CreateStackRefactor, DescribeStackRefactor,
// ExecuteStackRefactor, ListStackRefactors, ListStackRefactorActions.
func TestStackRefactor(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// CreateStackRefactor
	rec := postForm(t, h, url.Values{
		"Action":      []string{"CreateStackRefactor"},
		"Description": []string{"test refactor"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateStackRefactorResponse")
	refactorID := extractField(rec.Body.String(), "StackRefactorId")
	require.NotEmpty(t, refactorID, "StackRefactorId must be non-empty")

	// DescribeStackRefactor — should be CREATE_COMPLETE
	rec = postForm(t, h, url.Values{
		"Action":          []string{"DescribeStackRefactor"},
		"StackRefactorId": []string{refactorID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CREATE_COMPLETE")

	// ExecuteStackRefactor
	rec = postForm(t, h, url.Values{
		"Action":          []string{"ExecuteStackRefactor"},
		"StackRefactorId": []string{refactorID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeStackRefactor after execute — should be EXECUTE_COMPLETE
	rec = postForm(t, h, url.Values{
		"Action":          []string{"DescribeStackRefactor"},
		"StackRefactorId": []string{refactorID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "EXECUTE_COMPLETE")

	// ListStackRefactors
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListStackRefactors"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListStackRefactorsResponse")

	// ListStackRefactorActions
	rec = postForm(t, h, url.Values{
		"Action":          []string{"ListStackRefactorActions"},
		"StackRefactorId": []string{refactorID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestDescribeStackRefactor_NotFound verifies that DescribeStackRefactor
// returns StackRefactorNotFoundException for an unknown ID, matching the SDK's
// modeled error for this op — unlike CreateStackRefactor/ExecuteStackRefactor/
// List*, which have no modeled errors at all (verified against
// deserializers.go) and so are correctly fire-and-forget. Previously
// DescribeStackRefactor silently returned 200 with an empty Status.
func TestDescribeStackRefactor_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action":          []string{"DescribeStackRefactor"},
		"StackRefactorId": []string{"does-not-exist"},
	}.Encode())

	assert.NotEqual(t, http.StatusOK, rec.Code,
		"DescribeStackRefactor on an unknown ID must fail")
	assert.Contains(t, rec.Body.String(), "StackRefactorNotFoundException")
}
