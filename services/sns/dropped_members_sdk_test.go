package sns_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

func TestCreateTopic_AppliesDataProtectionPolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"Name":"p","Version":"2021-06-01","Statement":[{"Sid":"s","DataDirection":"Inbound",` +
		`"Principal":["*"],"DataIdentifier":["arn:aws:dataprotection::aws:data-identifier/EmailAddress"],` +
		`"Operation":{"Deny":{}}}]}`

	tests := []struct {
		name    string
		policy  string
		wantErr bool
	}{
		{name: "valid", policy: policy},
		{name: "invalid_json", policy: "not json", wantErr: true},
		{name: "missing_members", policy: `{"Name":"p"}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSNSClient(t, sns.NewHandler(sns.NewInMemoryBackend()))

			out, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{
				Name:                 aws.String("dpp-topic"),
				DataProtectionPolicy: aws.String(tt.policy),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := client.GetDataProtectionPolicy(t.Context(), &snssdk.GetDataProtectionPolicyInput{
				ResourceArn: out.TopicArn,
			})
			require.NoError(t, err)
			assert.JSONEq(t, tt.policy, aws.ToString(got.DataProtectionPolicy))
		})
	}
}

func TestGetSMSAttributes_NamesFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  map[string]string
		name  string
		names []string
	}{
		{name: "all", want: map[string]string{"DefaultSenderID": "me", "DefaultSMSType": "Promotional"}},
		{name: "subset", names: []string{"DefaultSMSType"}, want: map[string]string{"DefaultSMSType": "Promotional"}},
		{name: "unset_omitted", names: []string{"DefaultSMSType", "MonthlySpendLimit"},
			want: map[string]string{"DefaultSMSType": "Promotional"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSNSClient(t, sns.NewHandler(sns.NewInMemoryBackend()))

			_, err := client.SetSMSAttributes(t.Context(), &snssdk.SetSMSAttributesInput{
				Attributes: map[string]string{"DefaultSenderID": "me", "DefaultSMSType": "Promotional"},
			})
			require.NoError(t, err)

			out, err := client.GetSMSAttributes(t.Context(), &snssdk.GetSMSAttributesInput{Attributes: tt.names})
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.Attributes)
		})
	}
}

func TestAddPermission_MemberListsApplied(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	client := newTestSNSClient(t, sns.NewHandler(b))

	topic, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{Name: aws.String("perm-topic")})
	require.NoError(t, err)

	_, err = client.AddPermission(t.Context(), &snssdk.AddPermissionInput{
		TopicArn:     topic.TopicArn,
		Label:        aws.String("lbl"),
		AWSAccountId: []string{"111122223333", "444455556666"},
		ActionName:   []string{"Publish", "Subscribe"},
	})
	require.NoError(t, err)

	attrs, err := client.GetTopicAttributes(t.Context(), &snssdk.GetTopicAttributesInput{TopicArn: topic.TopicArn})
	require.NoError(t, err)

	pol := attrs.Attributes["Policy"]
	for _, want := range []string{"111122223333", "444455556666", "SNS:Publish", "SNS:Subscribe", "lbl"} {
		assert.Contains(t, pol, want)
	}
}
