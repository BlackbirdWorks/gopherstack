package elasticbeanstalk_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

const testSolutionStack = "64bit Amazon Linux 2023 v4.0.0 running Python 3.11"

func newEBClient31(t *testing.T) *ebsdk.Client {
	t.Helper()

	h := elasticbeanstalk.NewHandler(
		elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1"),
	)

	return newTestEBClient(t, h)
}

// Test_SDKRoundTrip_AccountAndStorage drives DescribeAccountAttributes,
// CreateStorageLocation, and ListAvailableSolutionStacks through the real
// typed client.
func Test_SDKRoundTrip_AccountAndStorage(t *testing.T) {
	t.Parallel()

	client := newEBClient31(t)
	ctx := t.Context()

	attrs, err := client.DescribeAccountAttributes(ctx, &ebsdk.DescribeAccountAttributesInput{})
	require.NoError(t, err)
	require.NotNil(t, attrs.ResourceQuotas)
	require.NotNil(t, attrs.ResourceQuotas.EnvironmentQuota)
	assert.Positive(t, aws.ToInt32(attrs.ResourceQuotas.EnvironmentQuota.Maximum))

	storage, err := client.CreateStorageLocation(ctx, &ebsdk.CreateStorageLocationInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(storage.S3Bucket))

	stacks, err := client.ListAvailableSolutionStacks(
		ctx,
		&ebsdk.ListAvailableSolutionStacksInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, stacks.SolutionStacks)
}

// Test_SDKRoundTrip_ApplicationAndVersionLifecycle drives UpdateApplication,
// UpdateApplicationResourceLifecycle, and UpdateApplicationVersion through
// the real typed client.
func Test_SDKRoundTrip_ApplicationAndVersionLifecycle(t *testing.T) {
	t.Parallel()

	client := newEBClient31(t)
	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &ebsdk.CreateApplicationInput{
		ApplicationName: aws.String("app-31"),
		Description:     aws.String("initial"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateApplication(ctx, &ebsdk.UpdateApplicationInput{
		ApplicationName: aws.String("app-31"),
		Description:     aws.String("updated"),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Application)
	assert.Equal(t, "updated", aws.ToString(updated.Application.Description))

	lifecycle, err := client.UpdateApplicationResourceLifecycle(
		ctx,
		&ebsdk.UpdateApplicationResourceLifecycleInput{
			ApplicationName: aws.String("app-31"),
			ResourceLifecycleConfig: &ebtypes.ApplicationResourceLifecycleConfig{
				ServiceRole: aws.String("arn:aws:iam::123456789012:role/eb-lifecycle"),
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, lifecycle.ResourceLifecycleConfig)
	assert.Equal(
		t,
		"arn:aws:iam::123456789012:role/eb-lifecycle",
		aws.ToString(lifecycle.ResourceLifecycleConfig.ServiceRole),
	)

	_, err = client.CreateApplicationVersion(ctx, &ebsdk.CreateApplicationVersionInput{
		ApplicationName: aws.String("app-31"),
		VersionLabel:    aws.String("v1"),
		Description:     aws.String("first"),
	})
	require.NoError(t, err)

	verUpdated, err := client.UpdateApplicationVersion(ctx, &ebsdk.UpdateApplicationVersionInput{
		ApplicationName: aws.String("app-31"),
		VersionLabel:    aws.String("v1"),
		Description:     aws.String("second"),
	})
	require.NoError(t, err)
	require.NotNil(t, verUpdated.ApplicationVersion)
	assert.Equal(t, "second", aws.ToString(verUpdated.ApplicationVersion.Description))
	assert.Equal(t, "v1", aws.ToString(verUpdated.ApplicationVersion.VersionLabel))
}

// Test_SDKRoundTrip_ConfigurationTemplateLifecycle drives
// DescribeConfigurationSettings, DescribeConfigurationOptions,
// UpdateConfigurationTemplate, ValidateConfigurationSettings, and
// DeleteConfigurationTemplate through the real typed client.
func Test_SDKRoundTrip_ConfigurationTemplateLifecycle(t *testing.T) {
	t.Parallel()

	client := newEBClient31(t)
	ctx := t.Context()

	_, err := client.CreateApplication(
		ctx,
		&ebsdk.CreateApplicationInput{ApplicationName: aws.String("app-31-tmpl")},
	)
	require.NoError(t, err)

	_, err = client.CreateConfigurationTemplate(ctx, &ebsdk.CreateConfigurationTemplateInput{
		ApplicationName:   aws.String("app-31-tmpl"),
		TemplateName:      aws.String("tmpl-31"),
		SolutionStackName: aws.String(testSolutionStack),
		Description:       aws.String("initial template"),
	})
	require.NoError(t, err)

	settings, err := client.DescribeConfigurationSettings(
		ctx,
		&ebsdk.DescribeConfigurationSettingsInput{
			ApplicationName: aws.String("app-31-tmpl"),
			TemplateName:    aws.String("tmpl-31"),
		},
	)
	require.NoError(t, err)
	require.Len(t, settings.ConfigurationSettings, 1)
	assert.Equal(t, "tmpl-31", aws.ToString(settings.ConfigurationSettings[0].TemplateName))
	assert.Equal(
		t,
		testSolutionStack,
		aws.ToString(settings.ConfigurationSettings[0].SolutionStackName),
	)

	options, err := client.DescribeConfigurationOptions(
		ctx,
		&ebsdk.DescribeConfigurationOptionsInput{
			ApplicationName:   aws.String("app-31-tmpl"),
			TemplateName:      aws.String("tmpl-31"),
			SolutionStackName: aws.String(testSolutionStack),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, testSolutionStack, aws.ToString(options.SolutionStackName))

	var foundEnvType bool
	for _, opt := range options.Options {
		if aws.ToString(opt.Namespace) == "aws:elasticbeanstalk:environment" &&
			aws.ToString(opt.Name) == "EnvironmentType" {
			foundEnvType = true
		}
	}
	assert.True(
		t,
		foundEnvType,
		"DescribeConfigurationOptions must include the EnvironmentType option",
	)

	updatedTmpl, err := client.UpdateConfigurationTemplate(
		ctx,
		&ebsdk.UpdateConfigurationTemplateInput{
			ApplicationName: aws.String("app-31-tmpl"),
			TemplateName:    aws.String("tmpl-31"),
			Description:     aws.String("updated template"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "updated template", aws.ToString(updatedTmpl.Description))

	valid, err := client.ValidateConfigurationSettings(
		ctx,
		&ebsdk.ValidateConfigurationSettingsInput{
			ApplicationName: aws.String("app-31-tmpl"),
			TemplateName:    aws.String("tmpl-31"),
			OptionSettings: []ebtypes.ConfigurationOptionSetting{
				{
					Namespace:  aws.String("aws:elasticbeanstalk:environment"),
					OptionName: aws.String("EnvironmentType"),
					Value:      aws.String("LoadBalanced"),
				},
			},
		},
	)
	require.NoError(t, err)
	assert.Empty(
		t,
		valid.Messages,
		"a known namespace/option must not produce a validation message",
	)

	invalid, err := client.ValidateConfigurationSettings(
		ctx,
		&ebsdk.ValidateConfigurationSettingsInput{
			ApplicationName: aws.String("app-31-tmpl"),
			TemplateName:    aws.String("tmpl-31"),
			OptionSettings: []ebtypes.ConfigurationOptionSetting{
				{
					Namespace:  aws.String("aws:bogus:namespace"),
					OptionName: aws.String("Nope"),
					Value:      aws.String("x"),
				},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, invalid.Messages, 1)
	assert.Equal(t, "aws:bogus:namespace", aws.ToString(invalid.Messages[0].Namespace))
	assert.Equal(t, "error", string(invalid.Messages[0].Severity))

	_, err = client.DeleteConfigurationTemplate(ctx, &ebsdk.DeleteConfigurationTemplateInput{
		ApplicationName: aws.String("app-31-tmpl"),
		TemplateName:    aws.String("tmpl-31"),
	})
	require.NoError(t, err)

	afterDelete, err := client.DescribeConfigurationSettings(
		ctx,
		&ebsdk.DescribeConfigurationSettingsInput{
			ApplicationName: aws.String("app-31-tmpl"),
			TemplateName:    aws.String("tmpl-31"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, afterDelete.ConfigurationSettings)
}

// Test_SDKRoundTrip_EnvironmentOperations drives CheckDNSAvailability,
// AssociateEnvironmentOperationsRole, DisassociateEnvironmentOperationsRole,
// DescribeConfigurationSettings(environment), DeleteEnvironmentConfiguration,
// RestartAppServer, RebuildEnvironment, AbortEnvironmentUpdate,
// RequestEnvironmentInfo, RetrieveEnvironmentInfo,
// DescribeEnvironmentManagedActions, and TerminateEnvironment through the
// real typed client.
func Test_SDKRoundTrip_EnvironmentOperations(t *testing.T) {
	t.Parallel()

	client := newEBClient31(t)
	ctx := t.Context()

	_, err := client.CreateApplication(
		ctx,
		&ebsdk.CreateApplicationInput{ApplicationName: aws.String("app-31-env")},
	)
	require.NoError(t, err)

	dns, err := client.CheckDNSAvailability(ctx, &ebsdk.CheckDNSAvailabilityInput{
		CNAMEPrefix: aws.String("eb-31-cname"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(dns.Available))
	assert.Contains(t, aws.ToString(dns.FullyQualifiedCNAME), "eb-31-cname")

	envOut, err := client.CreateEnvironment(ctx, &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("app-31-env"),
		EnvironmentName:   aws.String("env-31"),
		SolutionStackName: aws.String(testSolutionStack),
	})
	require.NoError(t, err)
	envName := aws.ToString(envOut.EnvironmentName)
	require.Equal(t, "env-31", envName)

	_, err = client.AssociateEnvironmentOperationsRole(
		ctx,
		&ebsdk.AssociateEnvironmentOperationsRoleInput{
			EnvironmentName: aws.String(envName),
			OperationsRole:  aws.String("arn:aws:iam::123456789012:role/eb-ops"),
		},
	)
	require.NoError(t, err)

	described, err := client.DescribeEnvironments(ctx, &ebsdk.DescribeEnvironmentsInput{
		EnvironmentNames: []string{envName},
	})
	require.NoError(t, err)
	require.Len(t, described.Environments, 1)
	assert.Equal(
		t,
		"arn:aws:iam::123456789012:role/eb-ops",
		aws.ToString(described.Environments[0].OperationsRole),
	)

	_, err = client.DisassociateEnvironmentOperationsRole(
		ctx,
		&ebsdk.DisassociateEnvironmentOperationsRoleInput{
			EnvironmentName: aws.String(envName),
		},
	)
	require.NoError(t, err)

	describedAfter, err := client.DescribeEnvironments(ctx, &ebsdk.DescribeEnvironmentsInput{
		EnvironmentNames: []string{envName},
	})
	require.NoError(t, err)
	require.Len(t, describedAfter.Environments, 1)
	assert.Empty(t, aws.ToString(describedAfter.Environments[0].OperationsRole))

	envSettings, err := client.DescribeConfigurationSettings(
		ctx,
		&ebsdk.DescribeConfigurationSettingsInput{
			ApplicationName: aws.String("app-31-env"),
			EnvironmentName: aws.String(envName),
		},
	)
	require.NoError(t, err)
	require.Len(t, envSettings.ConfigurationSettings, 1)
	assert.Equal(t, envName, aws.ToString(envSettings.ConfigurationSettings[0].EnvironmentName))

	_, err = client.DeleteEnvironmentConfiguration(ctx, &ebsdk.DeleteEnvironmentConfigurationInput{
		ApplicationName: aws.String("app-31-env"),
		EnvironmentName: aws.String(envName),
	})
	require.NoError(t, err)

	_, err = client.RestartAppServer(
		ctx,
		&ebsdk.RestartAppServerInput{EnvironmentName: aws.String(envName)},
	)
	require.NoError(t, err)

	_, err = client.RebuildEnvironment(
		ctx,
		&ebsdk.RebuildEnvironmentInput{EnvironmentName: aws.String(envName)},
	)
	require.NoError(t, err)

	_, err = client.AbortEnvironmentUpdate(
		ctx,
		&ebsdk.AbortEnvironmentUpdateInput{EnvironmentName: aws.String(envName)},
	)
	require.NoError(t, err)

	_, err = client.RequestEnvironmentInfo(ctx, &ebsdk.RequestEnvironmentInfoInput{
		EnvironmentName: aws.String(envName),
		InfoType:        ebtypes.EnvironmentInfoTypeTail,
	})
	require.NoError(t, err)

	retrieved, err := client.RetrieveEnvironmentInfo(ctx, &ebsdk.RetrieveEnvironmentInfoInput{
		EnvironmentName: aws.String(envName),
		InfoType:        ebtypes.EnvironmentInfoTypeTail,
	})
	require.NoError(t, err)
	assert.NotNil(t, retrieved.EnvironmentInfo)

	managed, err := client.DescribeEnvironmentManagedActions(
		ctx,
		&ebsdk.DescribeEnvironmentManagedActionsInput{
			EnvironmentName: aws.String(envName),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, managed.ManagedActions)

	terminated, err := client.TerminateEnvironment(ctx, &ebsdk.TerminateEnvironmentInput{
		EnvironmentName: aws.String(envName),
	})
	require.NoError(t, err)
	assert.Equal(t, envName, aws.ToString(terminated.EnvironmentName))
}

// Test_SDKRoundTrip_ComposeAndSwapEnvironments drives ComposeEnvironments
// and SwapEnvironmentCNAMEs through the real typed client.
func Test_SDKRoundTrip_ComposeAndSwapEnvironments(t *testing.T) {
	t.Parallel()

	client := newEBClient31(t)
	ctx := t.Context()

	_, err := client.CreateApplication(
		ctx,
		&ebsdk.CreateApplicationInput{ApplicationName: aws.String("app-31-compose")},
	)
	require.NoError(t, err)

	_, err = client.CreateEnvironment(ctx, &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("app-31-compose"),
		EnvironmentName:   aws.String("env-31-a"),
		SolutionStackName: aws.String(testSolutionStack),
	})
	require.NoError(t, err)

	_, err = client.CreateEnvironment(ctx, &ebsdk.CreateEnvironmentInput{
		ApplicationName:   aws.String("app-31-compose"),
		EnvironmentName:   aws.String("env-31-b"),
		SolutionStackName: aws.String(testSolutionStack),
	})
	require.NoError(t, err)

	// ComposeEnvironments in this backend returns the application's existing
	// environments (it does not synthesize new ones from env.yaml manifests
	// named by VersionLabels -- see typed/NOTES.md's 2026-09-12 entry
	// and this pass's PARITY.md addendum: a disclosed simplification, not
	// independently fixable within this pass's scope).
	composed, err := client.ComposeEnvironments(ctx, &ebsdk.ComposeEnvironmentsInput{
		ApplicationName: aws.String("app-31-compose"),
	})
	require.NoError(t, err)
	require.Len(t, composed.Environments, 2)

	swapped, err := client.SwapEnvironmentCNAMEs(ctx, &ebsdk.SwapEnvironmentCNAMEsInput{
		SourceEnvironmentName:      aws.String("env-31-a"),
		DestinationEnvironmentName: aws.String("env-31-b"),
	})
	require.NoError(t, err)
	_ = swapped
}

// Test_SDKRoundTrip_UpdateTagsForResource drives UpdateTagsForResource
// through the real typed client, confirmed via ListTagsForResource.
func Test_SDKRoundTrip_UpdateTagsForResource(t *testing.T) {
	t.Parallel()

	client := newEBClient31(t)
	ctx := t.Context()

	created, err := client.CreateApplication(ctx, &ebsdk.CreateApplicationInput{
		ApplicationName: aws.String("app-31-tags"),
	})
	require.NoError(t, err)
	appARN := created.Application.ApplicationArn

	_, err = client.UpdateTagsForResource(ctx, &ebsdk.UpdateTagsForResourceInput{
		ResourceArn: appARN,
		TagsToAdd:   []ebtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	tags, err := client.ListTagsForResource(
		ctx,
		&ebsdk.ListTagsForResourceInput{ResourceArn: appARN},
	)
	require.NoError(t, err)
	require.Len(t, tags.ResourceTags, 1)
	assert.Equal(t, "env", aws.ToString(tags.ResourceTags[0].Key))
	assert.Equal(t, "test", aws.ToString(tags.ResourceTags[0].Value))

	_, err = client.UpdateTagsForResource(ctx, &ebsdk.UpdateTagsForResourceInput{
		ResourceArn:  appARN,
		TagsToRemove: []string{"env"},
	})
	require.NoError(t, err)

	tagsAfter, err := client.ListTagsForResource(
		ctx,
		&ebsdk.ListTagsForResourceInput{ResourceArn: appARN},
	)
	require.NoError(t, err)
	assert.Empty(t, tagsAfter.ResourceTags)
}
