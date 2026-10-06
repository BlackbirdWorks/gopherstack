package elasticbeanstalk_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvironmentOps_ResolveEnvironment_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(t *testing.T, c *ebsdk.Client, name string) error
		name    string
		known   bool
		wantErr bool
	}{
		{
			name: "restart known", known: true,
			call: func(t *testing.T, c *ebsdk.Client, name string) error {
				t.Helper()
				_, err := c.RestartAppServer(t.Context(), &ebsdk.RestartAppServerInput{
					EnvironmentName: aws.String(name),
				})

				return err
			},
		},
		{
			name: "restart unknown", wantErr: true,
			call: func(t *testing.T, c *ebsdk.Client, name string) error {
				t.Helper()
				_, err := c.RestartAppServer(t.Context(), &ebsdk.RestartAppServerInput{
					EnvironmentName: aws.String(name),
				})

				return err
			},
		},
		{
			name: "rebuild unknown", wantErr: true,
			call: func(t *testing.T, c *ebsdk.Client, name string) error {
				t.Helper()
				_, err := c.RebuildEnvironment(t.Context(), &ebsdk.RebuildEnvironmentInput{
					EnvironmentName: aws.String(name),
				})

				return err
			},
		},
		{
			name: "abort unknown", wantErr: true,
			call: func(t *testing.T, c *ebsdk.Client, name string) error {
				t.Helper()
				_, err := c.AbortEnvironmentUpdate(t.Context(), &ebsdk.AbortEnvironmentUpdateInput{
					EnvironmentName: aws.String(name),
				})

				return err
			},
		},
		{
			name: "managed action unknown", wantErr: true,
			call: func(t *testing.T, c *ebsdk.Client, name string) error {
				t.Helper()
				_, err := c.ApplyEnvironmentManagedAction(t.Context(), &ebsdk.ApplyEnvironmentManagedActionInput{
					EnvironmentName: aws.String(name), ActionId: aws.String("a"),
				})

				return err
			},
		},
		{
			name: "describe managed actions unknown", wantErr: true,
			call: func(t *testing.T, c *ebsdk.Client, name string) error {
				t.Helper()
				in := &ebsdk.DescribeEnvironmentManagedActionsInput{EnvironmentName: aws.String(name)}
				_, err := c.DescribeEnvironmentManagedActions(t.Context(), in)

				return err
			},
		},
		{
			name: "instances health unknown", wantErr: true,
			call: func(t *testing.T, c *ebsdk.Client, name string) error {
				t.Helper()
				_, err := c.DescribeInstancesHealth(t.Context(), &ebsdk.DescribeInstancesHealthInput{
					EnvironmentName: aws.String(name),
				})

				return err
			},
		},
		{
			name: "validate unknown env", wantErr: true,
			call: func(t *testing.T, c *ebsdk.Client, name string) error {
				t.Helper()
				_, err := c.ValidateConfigurationSettings(t.Context(), &ebsdk.ValidateConfigurationSettingsInput{
					ApplicationName: aws.String("eb-app"), EnvironmentName: aws.String(name),
				})

				return err
			},
		},
		{
			name: "validate unknown template", wantErr: true,
			call: func(t *testing.T, c *ebsdk.Client, name string) error {
				t.Helper()
				_, err := c.ValidateConfigurationSettings(t.Context(), &ebsdk.ValidateConfigurationSettingsInput{
					ApplicationName: aws.String("eb-app"), TemplateName: aws.String(name),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newWireFixClient(t)
			createTaggedApp(t, c, "eb-app")

			_, err := c.CreateEnvironment(t.Context(), &ebsdk.CreateEnvironmentInput{
				ApplicationName: aws.String("eb-app"), EnvironmentName: aws.String("known"),
			})
			require.NoError(t, err)

			name := "missing"
			if tc.known {
				name = "known"
			}

			err = tc.call(t, c, name)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCreateEnvironment_Template_SDK(t *testing.T) {
	t.Parallel()

	const stack = "64bit Amazon Linux 2023 v4.0.0 running Python 3.11"

	c := newWireFixClient(t)
	ctx := t.Context()
	createTaggedApp(t, c, "eb-app")

	_, err := c.CreateConfigurationTemplate(ctx, &ebsdk.CreateConfigurationTemplateInput{
		ApplicationName: aws.String("eb-app"), TemplateName: aws.String("tmpl"), SolutionStackName: aws.String(stack),
		OptionSettings: []types.ConfigurationOptionSetting{
			{Namespace: aws.String("aws:autoscaling:asg"), OptionName: aws.String("MinSize"), Value: aws.String("2")},
			{Namespace: aws.String("aws:autoscaling:asg"), OptionName: aws.String("MaxSize"), Value: aws.String("4")},
		},
	})
	require.NoError(t, err)

	out, err := c.CreateEnvironment(ctx, &ebsdk.CreateEnvironmentInput{
		ApplicationName: aws.String("eb-app"),
		EnvironmentName: aws.String("from-tmpl"),
		TemplateName:    aws.String("tmpl"),
		OptionSettings: []types.ConfigurationOptionSetting{
			{Namespace: aws.String("aws:autoscaling:asg"), OptionName: aws.String("MinSize"), Value: aws.String("3")},
		},
		OptionsToRemove: []types.OptionSpecification{
			{Namespace: aws.String("aws:autoscaling:asg"), OptionName: aws.String("MaxSize")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, stack, aws.ToString(out.SolutionStackName))

	settings, err := c.DescribeConfigurationSettings(ctx, &ebsdk.DescribeConfigurationSettingsInput{
		ApplicationName: aws.String("eb-app"), EnvironmentName: aws.String("from-tmpl"),
	})
	require.NoError(t, err)
	require.Len(t, settings.ConfigurationSettings, 1)

	got := map[string]string{}
	for _, o := range settings.ConfigurationSettings[0].OptionSettings {
		got[aws.ToString(o.OptionName)] = aws.ToString(o.Value)
	}

	assert.Equal(t, "3", got["MinSize"])
	assert.NotContains(t, got, "MaxSize")

	_, err = c.CreateEnvironment(ctx, &ebsdk.CreateEnvironmentInput{
		ApplicationName: aws.String("eb-app"), EnvironmentName: aws.String("bad"), TemplateName: aws.String("nope"),
	})
	require.Error(t, err)
}
