package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aisdk "github.com/aws/aws-sdk-go-v2/service/appintegrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	aibackend "github.com/blackbirdworks/gopherstack/services/appintegrations"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func newAppIntegrationsEnv(t *testing.T) (familyStackEnv, *aisdk.Client) {
	t.Helper()

	handler := aibackend.NewHandler(aibackend.NewInMemoryBackend("000000000000", "us-east-1"))
	env := newFamilyStackEnv(t, handler, func(b *cloudformation.ServiceBackends) { b.AppIntegrations = handler })

	return env, aisdk.NewFromConfig(env.cfg, func(o *aisdk.Options) { o.BaseEndpoint = aws.String(env.url) })
}

func appIntegrationsTemplate(t *testing.T, accessURL, desc, team string) string {
	t.Helper()

	return stackTemplate(t, map[string]any{
		"App": cfnRes("AWS::AppIntegrations::Application", map[string]any{
			"Name": "cfn-app", "Namespace": "cfn.app", "Description": desc,
			"ApplicationSourceConfig": map[string]any{"ExternalUrlConfig": map[string]any{
				"AccessUrl": accessURL, "ApprovedOrigins": []string{"https://a.example.com"},
			}},
			"Permissions": []string{"Calendar.Read"},
			"Tags":        cfnTags(map[string]string{"Team": team}),
		}),
		"Data": cfnRes("AWS::AppIntegrations::DataIntegration", map[string]any{
			"Name": "cfn-data", "Description": desc, "KmsKey": "alias/cfn", "SourceURI": "salesforce://src",
			"ScheduleConfig": map[string]any{
				"ScheduleExpression": "rate(1 hour)",
				"FirstExecutionFrom": "2024-01-01",
			},
			"FileConfiguration": map[string]any{"Folders": []string{"inbox"}},
			"Tags":              cfnTags(map[string]string{"Team": team}),
		}),
		"Event": cfnRes("AWS::AppIntegrations::EventIntegration", map[string]any{
			"Name": "cfn-event", "Description": desc, "EventBridgeBus": "default",
			"EventFilter": map[string]any{"Source": "aws.partner/example/src"},
			"Tags":        cfnTags(map[string]string{"Team": team}),
		}),
	}, map[string]any{
		"AppRef": cfnRef("App"), "AppArn": cfnGetAtt("App", "ApplicationArn"), "AppId": cfnGetAtt("App", "Id"),
		"DataRef": cfnRef(
			"Data",
		), "DataArn": cfnGetAtt("Data", "DataIntegrationArn"), "DataId": cfnGetAtt("Data", "Id"),
		"EventRef": cfnRef("Event"), "EventArn": cfnGetAtt("Event", "EventIntegrationArn"),
	})
}

func TestCreateStack_AppIntegrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, env familyStackEnv, ai *aisdk.Client)
		name string
	}{
		{name: "create ref and getatt", run: func(t *testing.T, env familyStackEnv, ai *aisdk.Client) {
			t.Helper()

			out := createStackAndGetOutputs(
				t,
				env.cfn,
				"ai-create",
				appIntegrationsTemplate(t, "https://x.example.com", "d1", "emu"),
			)
			assert.Equal(t, out["AppArn"], out["AppRef"])
			assert.Equal(t, out["DataId"], out["DataRef"])
			assert.Equal(t, "cfn-event", out["EventRef"])
			assert.Contains(t, out["EventArn"], ":event-integration/cfn-event")
			assert.Contains(t, out["DataArn"], ":data-integration/"+out["DataId"])

			app, err := ai.GetApplication(t.Context(), &aisdk.GetApplicationInput{Arn: aws.String(out["AppArn"])})
			require.NoError(t, err)
			assert.Equal(t, out["AppId"], aws.ToString(app.Id))
			assert.Equal(t, "cfn.app", aws.ToString(app.Namespace))
			assert.Equal(
				t,
				"https://x.example.com",
				aws.ToString(app.ApplicationSourceConfig.ExternalUrlConfig.AccessUrl),
			)
			assert.Equal(t, map[string]string{"Team": "emu"}, app.Tags)

			data, err := ai.GetDataIntegration(
				t.Context(),
				&aisdk.GetDataIntegrationInput{Identifier: aws.String(out["DataRef"])},
			)
			require.NoError(t, err)
			assert.Equal(t, out["DataArn"], aws.ToString(data.Arn))
			assert.Equal(t, "rate(1 hour)", aws.ToString(data.ScheduleConfiguration.ScheduleExpression))
			assert.Equal(t, []string{"inbox"}, data.FileConfiguration.Folders)

			ev, err := ai.GetEventIntegration(
				t.Context(),
				&aisdk.GetEventIntegrationInput{Name: aws.String("cfn-event")},
			)
			require.NoError(t, err)
			assert.Equal(t, "default", aws.ToString(ev.EventBridgeBus))
			assert.Equal(t, "aws.partner/example/src", aws.ToString(ev.EventFilter.Source))
		}},
		{name: "update in place", run: func(t *testing.T, env familyStackEnv, ai *aisdk.Client) {
			t.Helper()

			out := createStackAndGetOutputs(
				t,
				env.cfn,
				"ai-update",
				appIntegrationsTemplate(t, "https://x.example.com", "d1", "emu"),
			)
			env.updateStack(t, "ai-update", appIntegrationsTemplate(t, "https://y.example.com", "d2", "dev"))

			app, err := ai.GetApplication(t.Context(), &aisdk.GetApplicationInput{Arn: aws.String(out["AppArn"])})
			require.NoError(t, err)
			assert.Equal(t, "d2", aws.ToString(app.Description))
			assert.Equal(
				t,
				"https://y.example.com",
				aws.ToString(app.ApplicationSourceConfig.ExternalUrlConfig.AccessUrl),
			)
			assert.Equal(t, map[string]string{"Team": "dev"}, app.Tags)

			data, err := ai.GetDataIntegration(
				t.Context(),
				&aisdk.GetDataIntegrationInput{Identifier: aws.String(out["DataRef"])},
			)
			require.NoError(t, err)
			assert.Equal(t, out["DataId"], aws.ToString(data.Id), "update must not replace the data integration")
			assert.Equal(t, "d2", aws.ToString(data.Description))
			assert.Equal(t, map[string]string{"Team": "dev"}, data.Tags)

			ev, err := ai.GetEventIntegration(
				t.Context(),
				&aisdk.GetEventIntegrationInput{Name: aws.String("cfn-event")},
			)
			require.NoError(t, err)
			assert.Equal(t, "d2", aws.ToString(ev.Description))
		}},
		{name: "delete stack", run: func(t *testing.T, env familyStackEnv, ai *aisdk.Client) {
			t.Helper()

			createStackAndGetOutputs(
				t,
				env.cfn,
				"ai-delete",
				appIntegrationsTemplate(t, "https://x.example.com", "d1", "emu"),
			)
			env.deleteStack(t, "ai-delete")

			apps, err := ai.ListApplications(t.Context(), &aisdk.ListApplicationsInput{})
			require.NoError(t, err)
			assert.Empty(t, apps.Applications)

			datas, err := ai.ListDataIntegrations(t.Context(), &aisdk.ListDataIntegrationsInput{})
			require.NoError(t, err)
			assert.Empty(t, datas.DataIntegrations)

			evs, err := ai.ListEventIntegrations(t.Context(), &aisdk.ListEventIntegrationsInput{})
			require.NoError(t, err)
			assert.Empty(t, evs.EventIntegrations)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, ai := newAppIntegrationsEnv(t)
			tc.run(t, env, ai)
		})
	}
}
