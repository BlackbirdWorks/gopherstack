package ses_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSES_IdentityPolicy(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postForm(t, h, "Action=VerifyEmailIdentity&EmailAddress=test@example.com")

	// PutIdentityPolicy
	rec := postForm(t, h, url.Values{
		"Action":     []string{"PutIdentityPolicy"},
		"Identity":   []string{"test@example.com"},
		"PolicyName": []string{"my-policy"},
		"Policy":     []string{`{"Version":"2012-10-17","Statement":[]}`},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// GetIdentityPolicies
	rec = postForm(t, h, url.Values{
		"Action":               []string{"GetIdentityPolicies"},
		"Identity":             []string{"test@example.com"},
		"PolicyNames.member.1": []string{"my-policy"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// ListIdentityPolicies
	rec = postForm(t, h, url.Values{
		"Action":   []string{"ListIdentityPolicies"},
		"Identity": []string{"test@example.com"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DeleteIdentityPolicy
	rec = postForm(t, h, url.Values{
		"Action":     []string{"DeleteIdentityPolicy"},
		"Identity":   []string{"test@example.com"},
		"PolicyName": []string{"my-policy"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}

func TestSES_IdentityAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	postForm(t, h, "Action=VerifyEmailIdentity&EmailAddress=attr@example.com")

	// GetIdentityDkimAttributes
	rec := postForm(t, h, url.Values{
		"Action":              []string{"GetIdentityDkimAttributes"},
		"Identities.member.1": []string{"attr@example.com"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// GetIdentityMailFromDomainAttributes
	rec = postForm(t, h, url.Values{
		"Action":              []string{"GetIdentityMailFromDomainAttributes"},
		"Identities.member.1": []string{"attr@example.com"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// GetIdentityNotificationAttributes
	rec = postForm(t, h, url.Values{
		"Action":              []string{"GetIdentityNotificationAttributes"},
		"Identities.member.1": []string{"attr@example.com"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// SetIdentityDkimEnabled
	rec = postForm(t, h, url.Values{
		"Action":      []string{"SetIdentityDkimEnabled"},
		"Identity":    []string{"attr@example.com"},
		"DkimEnabled": []string{"true"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// SetIdentityFeedbackForwardingEnabled
	rec = postForm(t, h, url.Values{
		"Action":            []string{"SetIdentityFeedbackForwardingEnabled"},
		"Identity":          []string{"attr@example.com"},
		"ForwardingEnabled": []string{"true"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// SetIdentityMailFromDomain
	rec = postForm(t, h, url.Values{
		"Action":         []string{"SetIdentityMailFromDomain"},
		"Identity":       []string{"attr@example.com"},
		"MailFromDomain": []string{"mail.example.com"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}
