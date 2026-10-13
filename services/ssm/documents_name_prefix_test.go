package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestListDocuments_NameFilterIsPrefix: types.DocumentKeyValuesFilter documents Name as a prefix match.
func TestListDocuments_NameFilterIsPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		want   []string
	}{
		{name: "prefix_matches_several", prefix: "mw-", want: []string{"mw-a", "mw-b", "mw-c"}},
		{name: "exact_still_matches", prefix: "mw-b", want: []string{"mw-b"}},
		{name: "builtin_prefix", prefix: "AWS-Run", want: []string{"AWS-RunPowerShellScript", "AWS-RunShellScript"}},
		{name: "no_match", prefix: "zzz", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			seedWindowsAndDocs(t, c)

			out, err := c.ListDocuments(t.Context(), &ssmsdk.ListDocumentsInput{
				Filters: []ssmtypes.DocumentKeyValuesFilter{{Key: aws.String("Name"), Values: []string{tt.prefix}}},
			})
			require.NoError(t, err)

			got := make([]string, 0)
			for _, d := range out.DocumentIdentifiers {
				got = append(got, aws.ToString(d.Name))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCreateDocument_ReservedNamePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		docName string
		wantErr bool
	}{
		{name: "aws", docName: "aws-custom", wantErr: true},
		{name: "aws_upper", docName: "AWS-Custom", wantErr: true},
		{name: "amazon", docName: "amazon-doc", wantErr: true},
		{name: "amzn", docName: "amzn-doc", wantErr: true},
		{name: "awsec2", docName: "AWSEC2-x", wantErr: true},
		{name: "config_remediation", docName: "AWSConfigRemediation-x", wantErr: true},
		{name: "support", docName: "AWSSupport-x", wantErr: true},
		{name: "ok", docName: "my-doc", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
			_, err := c.CreateDocument(t.Context(), &ssmsdk.CreateDocumentInput{
				Name:    aws.String(tt.docName),
				Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}
			require.NoError(t, err)
		})
	}
}
