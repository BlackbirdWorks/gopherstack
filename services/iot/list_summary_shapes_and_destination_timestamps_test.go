package iot_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestListDomainConfigurations_NoStatusLeak covers gopherstack-21my:
// types.DomainConfigurationSummary (schemas.go:4429-4437, iot@v1.83.0) has
// only domainConfigurationName, domainConfigurationArn and serviceType --
// no domainConfigurationStatus, which is Describe-only. ListDomainConfigurations
// fabricated that member on every summary; a real client silently ignores an
// unknown key, so the proof is the raw wire body.
func TestListDomainConfigurations_NoStatusLeak(t *testing.T) {
	t.Parallel()

	h := newIoTHandler(t)

	createRec := doIoTRequest(t, h, http.MethodPost, "/domainConfigurations/dc-21my", map[string]any{
		"serviceType": "DATA",
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	listRec := doIoTRequest(t, h, http.MethodGet, "/domainConfigurations", nil)
	require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

	body := listRec.Body.String()
	assert.Contains(t, body, `"domainConfigurationName":"dc-21my"`)
	assert.Contains(t, body, `"serviceType":"DATA"`)
	assert.NotContains(t, body, "domainConfigurationStatus",
		"ListDomainConfigurations must not leak the Describe-only domainConfigurationStatus member")
}

// TestListProvisioningTemplateVersions_NoTemplateBodyLeak covers
// gopherstack-21my: types.ProvisioningTemplateVersionSummary
// (schemas.go:7113-7117, iot@v1.83.0) has only versionId, creationDate and
// isDefaultVersion -- no templateBody, which is Describe-only.
// ListProvisioningTemplateVersions serialized the whole stored version
// (including its non-empty templateBody) into the summary; the leak is
// unobservable through the typed client (no such field to decode into), so
// the proof is the raw wire body plus a typed round-trip of the fields that
// do belong on the summary.
func TestListProvisioningTemplateVersions_NoTemplateBodyLeak(t *testing.T) {
	t.Parallel()

	h := newIoTHandler(t)
	client := newTestIoTClient(t, h)
	ctx := t.Context()

	createRec := doIoTRequest(t, h, http.MethodPost, "/provisioning-templates", map[string]any{
		"templateName": "pt-21my",
		"templateBody": `{"Parameters":{}}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	listRec := doIoTRequest(t, h, http.MethodGet, "/provisioning-templates/pt-21my/versions", nil)
	require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())
	assert.NotContains(t, listRec.Body.String(), "templateBody",
		"ListProvisioningTemplateVersions must not leak the Describe-only templateBody member")

	out, err := client.ListProvisioningTemplateVersions(ctx, &iotsdk.ListProvisioningTemplateVersionsInput{
		TemplateName: aws.String("pt-21my"),
	})
	require.NoError(t, err)
	require.Len(t, out.Versions, 1)
	assert.Equal(t, int32(1), aws.ToInt32(out.Versions[0].VersionId))
	assert.True(t, out.Versions[0].IsDefaultVersion)
	assert.False(t, aws.ToTime(out.Versions[0].CreationDate).IsZero())
}

// TestTopicRuleDestination_Timestamps covers gopherstack-21my:
// types.TopicRuleDestination and types.TopicRuleDestinationSummary both
// carry createdAt/lastUpdatedAt (CreateTopicRuleDestination.go,
// ListTopicRuleDestinations.go, iot@v1.83.0); neither the backend model nor
// any wire builder tracked them, so every real client saw nil for both no
// matter how long a destination had existed. Proven via CreateTopicRuleDestination,
// ListTopicRuleDestinations and GetTopicRuleDestination, plus that a status
// change bumps LastUpdatedAt.
//
// The Update-bumps-LastUpdatedAt assertion needs Create and Update to land
// in different whole seconds: the wire only carries epoch-seconds
// (awstime.Epoch), and this test drives a real httptest.Server/SDK client
// round trip, so it can't be moved into a synctest bubble (bubble can't
// durably block on real network I/O -- see wire_field_fixes_test.go in
// iotanalytics and the gopherstack-tests skill). There is also no injectable
// clock on this backend. So the test backdates the stored timestamps via
// SetTopicRuleDestinationTimestampsInternal (mirrors AddRuleInternal/
// AddCommandInternal) rather than sleeping, real or fake.
func TestTopicRuleDestination_Timestamps(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	client := newTestIoTClient(t, h)
	ctx := t.Context()

	created, err := client.CreateTopicRuleDestination(ctx, &iotsdk.CreateTopicRuleDestinationInput{
		DestinationConfiguration: &iottypes.TopicRuleDestinationConfiguration{
			HttpUrlConfiguration: &iottypes.HttpUrlDestinationConfiguration{
				ConfirmationUrl: aws.String("https://example.com/confirm"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.TopicRuleDestination.CreatedAt)
	require.NotNil(t, created.TopicRuleDestination.LastUpdatedAt)
	assert.False(t, aws.ToTime(created.TopicRuleDestination.CreatedAt).IsZero())
	assert.Equal(t,
		aws.ToTime(created.TopicRuleDestination.CreatedAt),
		aws.ToTime(created.TopicRuleDestination.LastUpdatedAt),
	)

	arn := aws.ToString(created.TopicRuleDestination.Arn)

	listed, err := client.ListTopicRuleDestinations(ctx, &iotsdk.ListTopicRuleDestinationsInput{})
	require.NoError(t, err)
	require.Len(t, listed.DestinationSummaries, 1)
	assert.False(t, aws.ToTime(listed.DestinationSummaries[0].CreatedAt).IsZero())
	assert.False(t, aws.ToTime(listed.DestinationSummaries[0].LastUpdatedAt).IsZero())

	backdated := time.Now().Add(-1 * time.Hour)
	backend.SetTopicRuleDestinationTimestampsInternal(arn, backdated, backdated)

	beforeUpdate, err := client.GetTopicRuleDestination(ctx, &iotsdk.GetTopicRuleDestinationInput{
		Arn: aws.String(arn),
	})
	require.NoError(t, err)

	_, err = client.UpdateTopicRuleDestination(ctx, &iotsdk.UpdateTopicRuleDestinationInput{
		Arn:    aws.String(arn),
		Status: iottypes.TopicRuleDestinationStatusDisabled,
	})
	require.NoError(t, err)

	got, err := client.GetTopicRuleDestination(ctx, &iotsdk.GetTopicRuleDestinationInput{
		Arn: aws.String(arn),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToTime(got.TopicRuleDestination.LastUpdatedAt).After(
		aws.ToTime(beforeUpdate.TopicRuleDestination.LastUpdatedAt)),
		"UpdateTopicRuleDestination must bump LastUpdatedAt")
	assert.Equal(t,
		aws.ToTime(beforeUpdate.TopicRuleDestination.CreatedAt),
		aws.ToTime(got.TopicRuleDestination.CreatedAt),
		"CreatedAt must not change on update")
}
