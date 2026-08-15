package ses_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	sestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestEmptyResultElement_RealClient covers every ses op whose real output shape has
// zero members but whose deserializer still calls decoder.GetElement("<Op>Result")
// (ses@v1.37.4 deserializers.go, confirmed per-op). gopherstack omitted the element
// on these seventeen, so every real SDK client failed deserialization with
// "deserialization failed: failed to decode response body ... node not found" even
// though the backend mutation succeeded. The assertion is exactly that the call
// deserializes without error -- there is nothing else to check on an empty output.
func TestEmptyResultElement_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *sessdk.Client) error
		name string
	}{
		{
			name: "clonereceiptruleset",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateReceiptRuleSet(t.Context(), &sessdk.CreateReceiptRuleSetInput{
					RuleSetName: aws.String("empty-result-clone-src"),
				})
				require.NoError(t, err)

				_, err = client.CloneReceiptRuleSet(t.Context(), &sessdk.CloneReceiptRuleSetInput{
					OriginalRuleSetName: aws.String("empty-result-clone-src"),
					RuleSetName:         aws.String("empty-result-clone-dst"),
				})

				return err
			},
		},
		{
			name: "createconfigurationset",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateConfigurationSet(t.Context(), &sessdk.CreateConfigurationSetInput{
					ConfigurationSet: &sestypes.ConfigurationSet{
						Name: aws.String("empty-result-create-cs"),
					},
				})

				return err
			},
		},
		{
			name: "createconfigurationseteventdestination",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateConfigurationSet(t.Context(), &sessdk.CreateConfigurationSetInput{
					ConfigurationSet: &sestypes.ConfigurationSet{
						Name: aws.String("empty-result-cs-eventdest"),
					},
				})
				require.NoError(t, err)

				_, err = client.CreateConfigurationSetEventDestination(
					t.Context(), &sessdk.CreateConfigurationSetEventDestinationInput{
						ConfigurationSetName: aws.String("empty-result-cs-eventdest"),
						EventDestination: &sestypes.EventDestination{
							Name:               aws.String("empty-result-dest"),
							MatchingEventTypes: []sestypes.EventType{sestypes.EventTypeSend},
						},
					},
				)

				return err
			},
		},
		{
			name: "createconfigurationsettrackingoptions",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateConfigurationSet(t.Context(), &sessdk.CreateConfigurationSetInput{
					ConfigurationSet: &sestypes.ConfigurationSet{
						Name: aws.String("empty-result-cs-tracking"),
					},
				})
				require.NoError(t, err)

				_, err = client.CreateConfigurationSetTrackingOptions(
					t.Context(), &sessdk.CreateConfigurationSetTrackingOptionsInput{
						ConfigurationSetName: aws.String("empty-result-cs-tracking"),
						TrackingOptions: &sestypes.TrackingOptions{
							CustomRedirectDomain: aws.String("track.example.com"),
						},
					},
				)

				return err
			},
		},
		{
			name: "createreceiptfilter",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateReceiptFilter(t.Context(), &sessdk.CreateReceiptFilterInput{
					Filter: &sestypes.ReceiptFilter{
						Name: aws.String("empty-result-filter"),
						IpFilter: &sestypes.ReceiptIpFilter{
							Cidr:   aws.String("10.0.0.1/32"),
							Policy: sestypes.ReceiptFilterPolicyAllow,
						},
					},
				})

				return err
			},
		},
		{
			name: "createreceiptrule",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateReceiptRuleSet(t.Context(), &sessdk.CreateReceiptRuleSetInput{
					RuleSetName: aws.String("empty-result-rule-set"),
				})
				require.NoError(t, err)

				_, err = client.CreateReceiptRule(t.Context(), &sessdk.CreateReceiptRuleInput{
					RuleSetName: aws.String("empty-result-rule-set"),
					Rule: &sestypes.ReceiptRule{
						Name: aws.String("empty-result-rule"),
					},
				})

				return err
			},
		},
		{
			name: "createreceiptruleset",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateReceiptRuleSet(t.Context(), &sessdk.CreateReceiptRuleSetInput{
					RuleSetName: aws.String("empty-result-fresh-rule-set"),
				})

				return err
			},
		},
		{
			name: "createtemplate",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateTemplate(t.Context(), &sessdk.CreateTemplateInput{
					Template: &sestypes.Template{
						TemplateName: aws.String("empty-result-create-template"),
					},
				})

				return err
			},
		},
		{
			name: "deleteconfigurationset",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateConfigurationSet(t.Context(), &sessdk.CreateConfigurationSetInput{
					ConfigurationSet: &sestypes.ConfigurationSet{
						Name: aws.String("empty-result-delete-cs"),
					},
				})
				require.NoError(t, err)

				_, err = client.DeleteConfigurationSet(t.Context(), &sessdk.DeleteConfigurationSetInput{
					ConfigurationSetName: aws.String("empty-result-delete-cs"),
				})

				return err
			},
		},
		{
			name: "deleteconfigurationseteventdestination",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateConfigurationSet(t.Context(), &sessdk.CreateConfigurationSetInput{
					ConfigurationSet: &sestypes.ConfigurationSet{
						Name: aws.String("empty-result-cs-delete-dest"),
					},
				})
				require.NoError(t, err)

				_, err = client.CreateConfigurationSetEventDestination(
					t.Context(), &sessdk.CreateConfigurationSetEventDestinationInput{
						ConfigurationSetName: aws.String("empty-result-cs-delete-dest"),
						EventDestination: &sestypes.EventDestination{
							Name:               aws.String("empty-result-delete-dest"),
							MatchingEventTypes: []sestypes.EventType{sestypes.EventTypeSend},
						},
					},
				)
				require.NoError(t, err)

				_, err = client.DeleteConfigurationSetEventDestination(
					t.Context(), &sessdk.DeleteConfigurationSetEventDestinationInput{
						ConfigurationSetName: aws.String("empty-result-cs-delete-dest"),
						EventDestinationName: aws.String("empty-result-delete-dest"),
					},
				)

				return err
			},
		},
		{
			name: "deleteconfigurationsettrackingoptions",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateConfigurationSet(t.Context(), &sessdk.CreateConfigurationSetInput{
					ConfigurationSet: &sestypes.ConfigurationSet{
						Name: aws.String("empty-result-cs-delete-tracking"),
					},
				})
				require.NoError(t, err)

				_, err = client.CreateConfigurationSetTrackingOptions(
					t.Context(), &sessdk.CreateConfigurationSetTrackingOptionsInput{
						ConfigurationSetName: aws.String("empty-result-cs-delete-tracking"),
						TrackingOptions: &sestypes.TrackingOptions{
							CustomRedirectDomain: aws.String("track.example.com"),
						},
					},
				)
				require.NoError(t, err)

				_, err = client.DeleteConfigurationSetTrackingOptions(
					t.Context(), &sessdk.DeleteConfigurationSetTrackingOptionsInput{
						ConfigurationSetName: aws.String("empty-result-cs-delete-tracking"),
					},
				)

				return err
			},
		},
		{
			name: "deletereceiptfilter",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateReceiptFilter(t.Context(), &sessdk.CreateReceiptFilterInput{
					Filter: &sestypes.ReceiptFilter{
						Name: aws.String("empty-result-delete-filter"),
						IpFilter: &sestypes.ReceiptIpFilter{
							Cidr:   aws.String("10.0.0.2/32"),
							Policy: sestypes.ReceiptFilterPolicyAllow,
						},
					},
				})
				require.NoError(t, err)

				_, err = client.DeleteReceiptFilter(t.Context(), &sessdk.DeleteReceiptFilterInput{
					FilterName: aws.String("empty-result-delete-filter"),
				})

				return err
			},
		},
		{
			name: "deletereceiptrule",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateReceiptRuleSet(t.Context(), &sessdk.CreateReceiptRuleSetInput{
					RuleSetName: aws.String("empty-result-delete-rule-set"),
				})
				require.NoError(t, err)

				_, err = client.CreateReceiptRule(t.Context(), &sessdk.CreateReceiptRuleInput{
					RuleSetName: aws.String("empty-result-delete-rule-set"),
					Rule: &sestypes.ReceiptRule{
						Name: aws.String("empty-result-delete-rule"),
					},
				})
				require.NoError(t, err)

				_, err = client.DeleteReceiptRule(t.Context(), &sessdk.DeleteReceiptRuleInput{
					RuleSetName: aws.String("empty-result-delete-rule-set"),
					RuleName:    aws.String("empty-result-delete-rule"),
				})

				return err
			},
		},
		{
			name: "deletereceiptruleset",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateReceiptRuleSet(t.Context(), &sessdk.CreateReceiptRuleSetInput{
					RuleSetName: aws.String("empty-result-delete-rule-set-2"),
				})
				require.NoError(t, err)

				_, err = client.DeleteReceiptRuleSet(t.Context(), &sessdk.DeleteReceiptRuleSetInput{
					RuleSetName: aws.String("empty-result-delete-rule-set-2"),
				})

				return err
			},
		},
		{
			name: "deletetemplate",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateTemplate(t.Context(), &sessdk.CreateTemplateInput{
					Template: &sestypes.Template{
						TemplateName: aws.String("empty-result-delete-template"),
					},
				})
				require.NoError(t, err)

				_, err = client.DeleteTemplate(t.Context(), &sessdk.DeleteTemplateInput{
					TemplateName: aws.String("empty-result-delete-template"),
				})

				return err
			},
		},
		{
			name: "setactivereceiptruleset",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateReceiptRuleSet(t.Context(), &sessdk.CreateReceiptRuleSetInput{
					RuleSetName: aws.String("empty-result-active-rule-set"),
				})
				require.NoError(t, err)

				_, err = client.SetActiveReceiptRuleSet(t.Context(), &sessdk.SetActiveReceiptRuleSetInput{
					RuleSetName: aws.String("empty-result-active-rule-set"),
				})

				return err
			},
		},
		{
			name: "updatetemplate",
			call: func(t *testing.T, client *sessdk.Client) error {
				t.Helper()

				_, err := client.CreateTemplate(t.Context(), &sessdk.CreateTemplateInput{
					Template: &sestypes.Template{
						TemplateName: aws.String("empty-result-update-template"),
					},
				})
				require.NoError(t, err)

				_, err = client.UpdateTemplate(t.Context(), &sessdk.UpdateTemplateInput{
					Template: &sestypes.Template{
						TemplateName: aws.String("empty-result-update-template"),
						SubjectPart:  aws.String("updated subject"),
					},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ses.NewHandler(ses.NewInMemoryBackend())
			client := newTestSESClient(t, h)

			require.NoError(t, tt.call(t, client))
		})
	}
}
