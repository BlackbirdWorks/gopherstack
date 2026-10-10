package accessanalyzer_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

func unusedCfg(age int32) aatypes.AnalyzerConfiguration {
	return &aatypes.AnalyzerConfigurationMemberUnusedAccess{
		Value: aatypes.UnusedAccessConfiguration{UnusedAccessAge: aws.Int32(age)},
	}
}

func TestCreateAnalyzer_InputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     aatypes.AnalyzerConfiguration
		name    string
		aName   string
		typ     aatypes.Type
		wantErr bool
	}{
		{name: "valid", aName: "ok.name_1-x", typ: aatypes.TypeAccount},
		{name: "space", aName: "bad name", typ: aatypes.TypeAccount, wantErr: true},
		{name: "leading digit", aName: "1abc", typ: aatypes.TypeAccount, wantErr: true},
		{name: "too long", aName: "a" + strings.Repeat("b", 255), typ: aatypes.TypeAccount, wantErr: true},
		{name: "unknown type", aName: "t1", typ: "BOGUS", wantErr: true},
		{name: "unused ok", aName: "u1", typ: aatypes.TypeAccountUnusedAccess, cfg: unusedCfg(90)},
		{name: "unused age zero", aName: "u2", typ: aatypes.TypeAccountUnusedAccess, cfg: unusedCfg(0), wantErr: true},
		{name: "unused age high", aName: "u3", typ: aatypes.TypeAccountUnusedAccess, cfg: unusedCfg(366),
			wantErr: true},
		{name: "unused config on account", aName: "u4", typ: aatypes.TypeAccount, cfg: unusedCfg(30), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAccessAnalyzerClient(t, newTestHandler(t))

			_, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String(tt.aName), Type: tt.typ, Configuration: tt.cfg,
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var ve *aatypes.ValidationException
			require.ErrorAs(t, err, &ve)
			assert.False(t, strings.HasPrefix(aws.ToString(ve.Message), "ValidationException"), ve.Message)
		})
	}
}

func TestGetAnalyzer_CreatingUntilDelayElapses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status aatypes.AnalyzerStatus
		delay  time.Duration
	}{
		{name: "default", delay: 0, status: aatypes.AnalyzerStatusActive},
		{name: "delayed", delay: time.Hour, status: aatypes.AnalyzerStatusCreating},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			b.SetAnalyzerCreateDelay(tt.delay)
			client := newTestAccessAnalyzerClient(t, accessanalyzer.NewHandler(b))

			_, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String("life"), Type: aatypes.TypeAccount,
			})
			require.NoError(t, err)

			got, err := client.GetAnalyzer(t.Context(), &aasdk.GetAnalyzerInput{AnalyzerName: aws.String("life")})
			require.NoError(t, err)
			assert.Equal(t, tt.status, got.Analyzer.Status)

			list, err := client.ListAnalyzers(t.Context(), &aasdk.ListAnalyzersInput{})
			require.NoError(t, err)
			require.Len(t, list.Analyzers, 1)
			assert.Equal(t, tt.status, list.Analyzers[0].Status)
		})
	}
}

func TestListFindings_UnknownNextTokenRejected(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestAccessAnalyzerClient(t, accessanalyzer.NewHandler(b))

	an, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
		AnalyzerName: aws.String("tok"), Type: aatypes.TypeAccount,
	})
	require.NoError(t, err)

	_, err = b.AddFinding("tok", "AWS::S3::Bucket", "arn:aws:s3:::b", []string{"s3:GetObject"}, nil, aws.Bool(true))
	require.NoError(t, err)

	tests := []struct {
		call func() error
		name string
	}{
		{name: "v1", call: func() error {
			_, e := client.ListFindings(t.Context(), &aasdk.ListFindingsInput{
				AnalyzerArn: an.Arn, NextToken: aws.String("bogus"),
			})

			return e
		}},
		{name: "v2", call: func() error {
			_, e := client.ListFindingsV2(t.Context(), &aasdk.ListFindingsV2Input{
				AnalyzerArn: an.Arn, NextToken: aws.String("bogus"),
			})

			return e
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var ve *aatypes.ValidationException
			require.ErrorAs(t, tt.call(), &ve)
		})
	}
}
