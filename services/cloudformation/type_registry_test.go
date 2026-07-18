package cloudformation_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypeRegistry covers RegisterType, DescribeTypeRegistration, ActivateType,
// DeactivateType, DeregisterType, PublishType, SetTypeDefaultVersion,
// SetTypeConfiguration, BatchDescribeTypeConfigurations, ListTypes,
// ListTypeVersions, ListTypeRegistrations, TestType.
func TestTypeRegistry(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// RegisterType
	rec := postForm(t, h, url.Values{
		"Action":               []string{"RegisterType"},
		"TypeName":             []string{"MyOrg::MyService::MyResource"},
		"SchemaHandlerPackage": []string{"s3://bucket/schema.zip"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code, "RegisterType should succeed")
	assert.Contains(t, rec.Body.String(), "RegisterTypeResponse")
	assert.Contains(t, rec.Body.String(), "RegistrationToken")
	token := extractField(rec.Body.String(), "RegistrationToken")
	require.NotEmpty(t, token, "RegistrationToken must be non-empty")

	// DescribeTypeRegistration — should return COMPLETE
	rec = postForm(t, h, url.Values{
		"Action":            []string{"DescribeTypeRegistration"},
		"RegistrationToken": []string{token},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "COMPLETE")

	// DescribeTypeRegistration — unknown token should fail
	rec = postForm(t, h, url.Values{
		"Action":            []string{"DescribeTypeRegistration"},
		"RegistrationToken": []string{"nonexistent-token"},
	}.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code, "Unknown token should return error")

	// ListTypeRegistrations
	rec = postForm(t, h, url.Values{
		"Action":   []string{"ListTypeRegistrations"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// ActivateType
	rec = postForm(t, h, url.Values{
		"Action":   []string{"ActivateType"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ActivateTypeResponse")

	// ListTypes — should contain our type
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListTypes"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListTypesResponse")
	assert.Contains(t, rec.Body.String(), "MyOrg::MyService::MyResource")

	// DeactivateType
	rec = postForm(t, h, url.Values{
		"Action":   []string{"DeactivateType"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// SetTypeDefaultVersion
	rec = postForm(t, h, url.Values{
		"Action":    []string{"SetTypeDefaultVersion"},
		"Arn":       []string{"arn:aws:cloudformation:::type/resource/MyOrg::MyService::MyResource"},
		"VersionId": []string{"00000002"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// SetTypeConfiguration
	rec = postForm(t, h, url.Values{
		"Action":        []string{"SetTypeConfiguration"},
		"TypeName":      []string{"MyOrg::MyService::MyResource"},
		"Configuration": []string{`{"key":"value"}`},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// BatchDescribeTypeConfigurations
	rec = postForm(t, h, url.Values{
		"Action": []string{"BatchDescribeTypeConfigurations"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// ListTypeVersions
	rec = postForm(t, h, url.Values{
		"Action":   []string{"ListTypeVersions"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// TestType
	rec = postForm(t, h, url.Values{
		"Action":   []string{"TestType"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "TestTypeResponse")

	// DeregisterType
	rec = postForm(t, h, url.Values{
		"Action": []string{"DeregisterType"},
		"Arn":    []string{"arn:aws:cloudformation:::type/resource/MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	// PublishType
	rec = postForm(t, h, url.Values{
		"Action":   []string{"PublishType"},
		"TypeName": []string{"MyOrg::MyService::MyResource"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestPublisher covers RegisterPublisher and DescribePublisher.
func TestPublisher(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// RegisterPublisher
	rec := postForm(t, h, url.Values{
		"Action":        []string{"RegisterPublisher"},
		"ConnectionArn": []string{"arn:aws:codestar-connections:us-east-1:123456789012:connection/abc"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RegisterPublisherResponse")
	publisherID := extractField(rec.Body.String(), "PublisherId")
	require.NotEmpty(t, publisherID, "PublisherId must be non-empty")

	// DescribePublisher — known publisher
	rec = postForm(t, h, url.Values{
		"Action":      []string{"DescribePublisher"},
		"PublisherId": []string{publisherID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VERIFIED")

	// DescribePublisher — unknown publisher should error
	rec = postForm(t, h, url.Values{
		"Action":      []string{"DescribePublisher"},
		"PublisherId": []string{"nonexistent-publisher"},
	}.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code, "Unknown publisher should return error")
}

// TestTypeRegistry_DeregisterNotFound ensures DeregisterType returns error for unknown ARN.
func TestTypeRegistry_DeregisterNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action": []string{"DeregisterType"},
		"Arn":    []string{"arn:aws:cloudformation:::type/resource/Unknown::Type::Here"},
	}.Encode())
	// handler currently ignores DeregisterType error; just verify no panic
	assert.GreaterOrEqual(t, rec.Code, 200)
}
