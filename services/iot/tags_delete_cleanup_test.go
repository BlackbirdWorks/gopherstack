package iot_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// tagCleanupCase exercises one taggable IoT resource type's Delete* path:
// create, tag, delete, recreate under the same key, and check the ARN is
// deterministic (name/id-keyed) so a stale resourceTags entry would be
// observable via ListTagsForResource on the recreated resource.
type tagCleanupCase struct {
	create func(b *iot.InMemoryBackend, key string) (string, error)
	del    func(b *iot.InMemoryBackend, key string) error
	name   string
}

// arnOrErr turns a backend Create call's (out, err) pair into the (string,
// error) a tagCleanupCase.create needs, reading the ARN off out only once
// err is known nil.
func arnOrErr[T any](out T, err error, arn func(T) string) (string, error) {
	if err != nil {
		return "", err
	}

	return arn(out), nil
}

func tagCleanupCases() []tagCleanupCase {
	return []tagCleanupCase{
		{
			name: "billing_group",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateBillingGroup(&iot.CreateBillingGroupInput{BillingGroupName: key})

				return arnOrErr(out, err, func(o *iot.BillingGroup) string { return o.BillingGroupARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteBillingGroup(key, 0) },
		},
		{
			name: "scheduled_audit",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateScheduledAudit(&iot.CreateScheduledAuditInput{
					ScheduledAuditName: key,
					Frequency:          "DAILY",
				})

				return arnOrErr(out, err, func(o *iot.ScheduledAudit) string { return o.ScheduledAuditARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteScheduledAudit(key) },
		},
		{
			name: "mitigation_action",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateMitigationAction(&iot.CreateMitigationActionInput{ActionName: key})

				return arnOrErr(out, err, func(o *iot.MitigationAction) string { return o.ActionARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteMitigationAction(key) },
		},
		{
			name: "authorizer",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateAuthorizer(&iot.CreateAuthorizerInput{
					AuthorizerName:        key,
					AuthorizerFunctionARN: "arn:aws:lambda:us-east-1:123456789012:function:auth",
				})

				return arnOrErr(out, err, func(o *iot.Authorizer) string { return o.AuthorizerARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteAuthorizer(key) },
		},
		{
			name: "command",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateCommand(key, "display", "desc", "namespace", nil, nil)

				return arnOrErr(out, err, func(o *iot.IoTCommand) string { return o.CommandARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteCommand(key) },
		},
		{
			name: "certificate_provider",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateCertificateProvider(&iot.CreateCertificateProviderInput{
					CertificateProviderName: key,
					LambdaFunctionARN:       "arn:aws:lambda:us-east-1:123456789012:function:cp",
				})

				return arnOrErr(out, err, func(o *iot.CertificateProvider) string { return o.ARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteCertificateProvider(key) },
		},
		{
			name: "fleet_metric",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateFleetMetric(&iot.CreateFleetMetricInput{
					MetricName:  key,
					QueryString: "connectivity.connected = true",
				})

				return arnOrErr(out, err, func(o *iot.FleetMetric) string { return o.MetricARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteFleetMetric(key, 0) },
		},
		{
			name: "custom_metric",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateCustomMetric(&iot.CreateCustomMetricInput{MetricName: key, MetricType: "number"})

				return arnOrErr(out, err, func(o *iot.CustomMetric) string { return o.MetricARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteCustomMetric(key) },
		},
		{
			name: "dimension",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateDimension(&iot.CreateDimensionInput{
					Name:         key,
					Type:         "TOPIC_FILTER",
					StringValues: []string{"a/b"},
				})

				return arnOrErr(out, err, func(o *iot.Dimension) string { return o.ARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteDimension(key) },
		},
		{
			name: "ota_update",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateOTAUpdate(key, "desc", "arn:aws:iam::123456789012:role/ota", nil, nil, nil)

				return arnOrErr(out, err, func(o *iot.OTAUpdate) string { return o.OTAUpdateARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteOTAUpdate(key) },
		},
		{
			name: "package",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateIoTPackage(key, "desc", nil)

				return arnOrErr(out, err, func(o *iot.IoTPackage) string { return o.PackageARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteIoTPackage(key) },
		},
		{
			name: "package_version",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateIoTPackageVersion(key, "v1", "desc", nil, iot.CreateIoTPackageVersionOptions{})

				return arnOrErr(out, err, func(o *iot.IoTPackageVersion) string { return o.PackageVersionARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteIoTPackageVersion(key, "v1") },
		},
		{
			name: "role_alias",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateRoleAlias(&iot.CreateRoleAliasInput{
					RoleAlias: key,
					RoleARN:   "arn:aws:iam::123456789012:role/x",
				})

				return arnOrErr(out, err, func(o *iot.RoleAlias) string { return o.RoleAliasARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteRoleAlias(key) },
		},
		{
			name: "domain_configuration",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateDomainConfiguration(
					&iot.CreateDomainConfigurationInput{DomainConfigurationName: key},
				)

				return arnOrErr(out, err, func(o *iot.DomainConfiguration) string { return o.DomainConfigurationARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteDomainConfiguration(key) },
		},
		{
			name: "provisioning_template",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateProvisioningTemplate(&iot.CreateProvisioningTemplateInput{
					TemplateName: key,
					TemplateBody: "{}",
				})

				return arnOrErr(out, err, func(o *iot.ProvisioningTemplate) string { return o.TemplateARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteProvisioningTemplate(key) },
		},
		{
			name: "stream",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateStream(&iot.CreateStreamInput{StreamID: key})

				return arnOrErr(out, err, func(o *iot.IoTStream) string { return o.StreamARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteStream(key) },
		},
		{
			name: "job",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateJob(&iot.CreateJobInput{JobID: key, Targets: []string{}})

				return arnOrErr(out, err, func(o *iot.Job) string { return o.JobARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteJob(key) },
		},
		{
			name: "job_template",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateJobTemplate(&iot.CreateJobTemplateInput{JobTemplateID: key})

				return arnOrErr(out, err, func(o *iot.JobTemplate) string { return o.JobTemplateARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteJobTemplate(key) },
		},
		{
			name: "topic_rule",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				if err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
					RuleName:         key,
					TopicRulePayload: &iot.TopicRulePayload{SQL: "SELECT *", Actions: []iot.RuleAction{}},
				}); err != nil {
					return "", err
				}

				out, err := b.GetTopicRule(key)

				return arnOrErr(out, err, func(o *iot.TopicRule) string { return o.ARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteTopicRule(key) },
		},
		{
			name: "thing_group",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: key})

				return arnOrErr(out, err, func(o *iot.ThingGroup) string { return o.ThingGroupARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteThingGroup(key, 0) },
		},
		{
			name: "dynamic_thing_group",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateDynamicThingGroup(&iot.CreateThingGroupInput{
					ThingGroupName: key,
					QueryString:    "connectivity.connected = true",
				})

				return arnOrErr(out, err, func(o *iot.ThingGroup) string { return o.ThingGroupARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteDynamicThingGroup(key, 0) },
		},
		{
			name: "thing_type",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: key})

				return arnOrErr(out, err, func(o *iot.ThingType) string { return o.ThingTypeARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error {
				if err := b.DeprecateThingType(&iot.DeprecateThingTypeInput{ThingTypeName: key}); err != nil {
					return err
				}

				return b.DeleteThingType(key)
			},
		},
		{
			name: "security_profile",
			create: func(b *iot.InMemoryBackend, key string) (string, error) {
				out, err := b.CreateSecurityProfile(&iot.CreateSecurityProfileInput{SecurityProfileName: key})

				return arnOrErr(out, err, func(o *iot.SecurityProfile) string { return o.SecurityProfileARN })
			},
			del: func(b *iot.InMemoryBackend, key string) error { return b.DeleteSecurityProfile(key, 0) },
		},
	}
}

// TestDeleteResource_ClearsResourceTagsOnRecreate proves, for every taggable
// IoT resource type whose Delete* path now clears resourceTags, that
// deleting and recreating a resource under the same name/id yields no
// stale tags on the recreated resource -- the observable shape of the leak
// (gopherstack-1ycq). Each subtest fails independently if its own Delete*
// cleanup regresses.
func TestDeleteResource_ClearsResourceTagsOnRecreate(t *testing.T) {
	t.Parallel()

	for _, tc := range tagCleanupCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()

			key := "reused-" + tc.name
			arn1, err := tc.create(b, key)
			require.NoError(t, err)
			require.NoError(t, b.TagResourceGeneric(arn1, map[string]string{"env": "prod"}))
			require.Equal(t, map[string]string{"env": "prod"}, b.ListTagsForResource(arn1))

			require.NoError(t, tc.del(b, key))

			arn2, err := tc.create(b, key)
			require.NoError(t, err)
			require.Equal(t, arn1, arn2, "ARN must be deterministic (name/id-keyed) for this leak to be observable")
			assert.Empty(t, b.ListTagsForResource(arn2),
				"recreated resource must not inherit the deleted resource's tags")
		})
	}
}

// TestDeleteResource_LeavesOtherResourceTagsIntact is the negative case for
// gopherstack-1ycq: deleting one resource of a given type must not disturb
// another surviving resource of the same type.
func TestDeleteResource_LeavesOtherResourceTagsIntact(t *testing.T) {
	t.Parallel()

	for _, tc := range tagCleanupCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()

			goneKey := "gone-" + tc.name
			keptKey := "kept-" + tc.name

			goneARN, err := tc.create(b, goneKey)
			require.NoError(t, err)
			keptARN, err := tc.create(b, keptKey)
			require.NoError(t, err)

			require.NoError(t, b.TagResourceGeneric(goneARN, map[string]string{"env": "prod"}))
			require.NoError(t, b.TagResourceGeneric(keptARN, map[string]string{"env": "dev"}))

			require.NoError(t, tc.del(b, goneKey))

			assert.Empty(t, b.ListTagsForResource(goneARN))
			assert.Equal(t, map[string]string{"env": "dev"}, b.ListTagsForResource(keptARN))
		})
	}
}
