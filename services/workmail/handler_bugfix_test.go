package workmail_test

// Regression tests for parity-4 bug fixes (audit against
// aws-sdk-go-v2/service/workmail@v1.37.2):
//
//   - CreateOrganization.Domains is a list of Domain objects
//     ({"DomainName":...,"HostedZoneId":...}), not a list of bare strings --
//     the old []string-typed request field made json.Unmarshal fail (500
//     InternalServiceError) for any real SDK client that specified a domain.
//   - DescribeEntity resolves by primary email address (the only field the
//     real API documents), which the backend never implemented.
//   - ListMailboxExportJobs reuses the full MailboxExportJob wire shape (not
//     a narrower summary), so RoleArn/KmsKeyArn/S3Path/S3Prefix/
//     EstimatedProgress/ErrorInfo must round-trip through the list, not just
//     DescribeMailboxExportJob.

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBugfix_WorkMail_CreateOrganizationWithDomains(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// This is the real wire shape aws-sdk-go-v2 sends: Domains is an array of
	// {DomainName, HostedZoneId} objects. Before the fix, the handler's
	// request struct typed Domains as []string, so json.Unmarshal of this
	// exact body would fail and the op would 500.
	rec := doOp(t, h, "CreateOrganization", `{
		"Alias": "domainorg",
		"Domains": [{"DomainName": "example.com", "HostedZoneId": "Z123456"}]
	}`)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	m := decodeJSON(t, rec)
	orgID, ok := m["OrganizationId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, orgID)

	// The domain from the request must actually be registered.
	rec2 := doOp(t, h, "GetMailDomain", fmt.Sprintf(
		`{"OrganizationId":%q,"DomainName":"example.com"}`, orgID,
	))
	require.Equal(t, http.StatusOK, rec2.Code)
	dm := decodeJSON(t, rec2)
	assert.Equal(t, false, dm["IsDefault"])
}

func TestBugfix_WorkMail_DescribeEntityByEmail(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "deentemail")
	userID := createTestUser(t, h, orgID, "emailuser", "Email User")

	email := "emailuser@deentemail.awsapps.com"
	rec := doOp(t, h, "RegisterToWorkMail", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q,"Email":%q}`, orgID, userID, email,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	// Real DescribeEntityInput.Email is the entity's email address, not its
	// ID -- this must resolve now that the backend indexes by email.
	rec2 := doOp(t, h, "DescribeEntity", fmt.Sprintf(
		`{"OrganizationId":%q,"Email":%q}`, orgID, email,
	))
	require.Equal(t, http.StatusOK, rec2.Code, "body: %s", rec2.Body.String())
	m := decodeJSON(t, rec2)
	assert.Equal(t, userID, m["EntityId"])
	assert.Equal(t, "emailuser", m["Name"])
	assert.Equal(t, "USER", m["Type"])
}

func TestBugfix_WorkMail_ListAccessControlRulesIpRangesCasing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "acrcasing")

	rec := doOp(t, h, "PutAccessControlRule", fmt.Sprintf(
		`{"OrganizationId":%q,"Name":"r1","Effect":"DENY","IpRanges":["10.0.0.0/8"],"NotIpRanges":["10.1.0.0/16"]}`,
		orgID,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doOp(t, h, "ListAccessControlRules", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	require.Equal(t, http.StatusOK, rec2.Code)
	m := decodeJSON(t, rec2)
	rules, ok := m["Rules"].([]any)
	require.True(t, ok)
	require.Len(t, rules, 1)
	r := rules[0].(map[string]any)

	// Real wire keys are IpRanges/NotIpRanges (lowercase "p"); a real SDK
	// client deserializes case-sensitively.
	ipRanges, ok := r["IpRanges"].([]any)
	require.True(t, ok, "expected IpRanges key, got: %v", r)
	assert.Equal(t, []any{"10.0.0.0/8"}, ipRanges)

	notIPRanges, ok := r["NotIpRanges"].([]any)
	require.True(t, ok, "expected NotIpRanges key, got: %v", r)
	assert.Equal(t, []any{"10.1.0.0/16"}, notIPRanges)

	_, hasWrongKey := r["IPRanges"]
	assert.False(t, hasWrongKey, "response must not use the wrong IPRanges casing")
}

func TestBugfix_WorkMail_ListMailboxExportJobsFullShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	orgID := createTestOrg(t, h, "exportjobsfull")
	userID := createTestUser(t, h, orgID, "expuser", "Export User")

	rec := doOp(t, h, "StartMailboxExportJob", fmt.Sprintf(
		`{"OrganizationId":%q,"EntityId":%q,"Description":"d","RoleArn":"arn:aws:iam::000000000000:role/r",`+
			`"KmsKeyArn":"arn:aws:kms:us-east-1:000000000000:key/k","S3BucketName":"bkt","S3Prefix":"pfx"}`,
		orgID, userID,
	))
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doOp(t, h, "ListMailboxExportJobs", fmt.Sprintf(`{"OrganizationId":%q}`, orgID))
	require.Equal(t, http.StatusOK, rec2.Code)
	m := decodeJSON(t, rec2)
	jobs, ok := m["Jobs"].([]any)
	require.True(t, ok)
	require.Len(t, jobs, 1)
	j := jobs[0].(map[string]any)

	assert.Equal(t, "arn:aws:iam::000000000000:role/r", j["RoleArn"])
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/k", j["KmsKeyArn"])
	assert.Equal(t, "pfx", j["S3Prefix"])
	assert.NotEmpty(t, j["S3Path"])
	assert.Contains(t, j, "EstimatedProgress")
}
