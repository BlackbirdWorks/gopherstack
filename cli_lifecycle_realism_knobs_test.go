package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/aws/aws-sdk-go-v2/service/appstream"
	appstreamtypes "github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/aws/aws-sdk-go-v2/service/detective"
	detectivetypes "github.com/aws/aws-sdk-go-v2/service/detective/types"
	"github.com/stretchr/testify/require"
)

func lifecycleKnobs() []lifecycleKnob {
	return append(baseLifecycleKnobs(), realismLifecycleKnobs()...)
}

func realismLifecycleKnobs() []lifecycleKnob {
	return []lifecycleKnob{
		{
			name: "accessanalyzer",
			set:  func(l *LifecycleSettings, d time.Duration) { l.AccessAnalyzer = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := accessanalyzer.NewFromConfig(cfg)
				_, err := c.CreateAnalyzer(t.Context(), &accessanalyzer.CreateAnalyzerInput{
					AnalyzerName: aws.String("lcres"), Type: aatypes.TypeAccount,
				})
				require.NoError(t, err)

				return func() bool {
					out, gErr := c.GetAnalyzer(
						t.Context(),
						&accessanalyzer.GetAnalyzerInput{AnalyzerName: aws.String("lcres")},
					)
					require.NoError(t, gErr)

					return out.Analyzer.Status == aatypes.AnalyzerStatusCreating
				}
			},
		},
		{
			name: "acmpca",
			set:  func(l *LifecycleSettings, d time.Duration) { l.ACMPCA = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := acmpca.NewFromConfig(cfg)
				ca, err := c.CreateCertificateAuthority(t.Context(), &acmpca.CreateCertificateAuthorityInput{
					CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
					CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
						KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
						SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
						Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("lcres")},
					},
				})
				require.NoError(t, err)

				rep, err := c.CreateCertificateAuthorityAuditReport(
					t.Context(),
					&acmpca.CreateCertificateAuthorityAuditReportInput{
						CertificateAuthorityArn:   ca.CertificateAuthorityArn,
						S3BucketName:              aws.String("audit-bucket"),
						AuditReportResponseFormat: acmpcatypes.AuditReportResponseFormatJson,
					},
				)
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeCertificateAuthorityAuditReport(t.Context(),
						&acmpca.DescribeCertificateAuthorityAuditReportInput{
							CertificateAuthorityArn: ca.CertificateAuthorityArn, AuditReportId: rep.AuditReportId,
						})
					require.NoError(t, dErr)

					return out.AuditReportStatus == acmpcatypes.AuditReportStatusCreating
				}
			},
		},
		{
			name: "appstream",
			set:  func(l *LifecycleSettings, d time.Duration) { l.AppStream = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := appstream.NewFromConfig(cfg)
				_, err := c.CreateFleet(t.Context(), &appstream.CreateFleetInput{
					Name: aws.String("lcres"), InstanceType: aws.String("stream.standard.medium"),
					ComputeCapacity: &appstreamtypes.ComputeCapacity{DesiredInstances: aws.Int32(1)},
				})
				require.NoError(t, err)
				_, err = c.StartFleet(t.Context(), &appstream.StartFleetInput{Name: aws.String("lcres")})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeFleets(t.Context(), &appstream.DescribeFleetsInput{Names: []string{"lcres"}})
					require.NoError(t, dErr)

					return out.Fleets[0].State == appstreamtypes.FleetStateStarting
				}
			},
		},
		{
			name: "detective",
			set:  func(l *LifecycleSettings, d time.Duration) { l.Detective = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := detective.NewFromConfig(cfg)
				g, err := c.CreateGraph(t.Context(), &detective.CreateGraphInput{})
				require.NoError(t, err)

				now := time.Now()
				inv, err := c.StartInvestigation(t.Context(), &detective.StartInvestigationInput{
					GraphArn:       g.GraphArn,
					EntityArn:      aws.String("arn:aws:iam::000000000000:user/bob"),
					ScopeStartTime: aws.Time(now.Add(-time.Hour)),
					ScopeEndTime:   aws.Time(now),
				})
				require.NoError(t, err)

				return func() bool {
					out, gErr := c.GetInvestigation(t.Context(), &detective.GetInvestigationInput{
						GraphArn: g.GraphArn, InvestigationId: inv.InvestigationId,
					})
					require.NoError(t, gErr)

					return out.Status == detectivetypes.StatusRunning
				}
			},
		},
	}
}
