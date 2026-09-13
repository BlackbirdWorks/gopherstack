package support_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	supportsdk "github.com/aws/aws-sdk-go-v2/service/support"
	supporttypes "github.com/aws/aws-sdk-go-v2/service/support/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/support"
)

// newRealClient stands up the real aws-sdk-go-v2 support client
// against an httptest server running this package's Handler through the
// same pkgs/service registry/router used in production -- this package had
// no real-client helper at all before this slice (gopherstack-n3zi).
func newRealClient(t *testing.T) *supportsdk.Client {
	t.Helper()

	backend := support.NewInMemoryBackend()
	h := support.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return supportsdk.NewFromConfig(cfg, func(o *supportsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestRealClient_AttachmentsAndTrustedAdvisor drives support's remaining
// typed-coverage-blind ops (gopherstack-n3zi) through the real
// aws-sdk-go-v2 client:
// AddAttachmentsToSet, DescribeAttachment, DescribeCommunications,
// DescribeCreateCaseOptions, DescribeServices, DescribeSeverityLevels,
// DescribeSupportedLanguages, DescribeTrustedAdvisorCheckRefreshStatuses,
// DescribeTrustedAdvisorCheckResult, DescribeTrustedAdvisorCheckSummaries,
// DescribeTrustedAdvisorChecks, RefreshTrustedAdvisorCheck.
func TestRealClient_AttachmentsAndTrustedAdvisor(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "attachments and communications",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)
				ctx := t.Context()

				createOut, err := client.CreateCase(ctx, &supportsdk.CreateCaseInput{
					Subject:           aws.String("s21 attachment case"),
					ServiceCode:       aws.String("general-info"),
					CategoryCode:      aws.String("using-aws"),
					SeverityCode:      aws.String("low"),
					CommunicationBody: aws.String("s21 initial communication"),
				})
				require.NoError(t, err)
				caseID := createOut.CaseId

				addAttOut, err := client.AddAttachmentsToSet(ctx, &supportsdk.AddAttachmentsToSetInput{
					Attachments: []supporttypes.Attachment{
						{FileName: aws.String("s21-log.txt"), Data: []byte("s21 attachment contents")},
					},
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(addAttOut.AttachmentSetId))
				assert.NotEmpty(t, aws.ToString(addAttOut.ExpiryTime))

				_, err = client.AddCommunicationToCase(ctx, &supportsdk.AddCommunicationToCaseInput{
					CaseId:            caseID,
					CommunicationBody: aws.String("s21 communication with attachment"),
					AttachmentSetId:   addAttOut.AttachmentSetId,
				})
				require.NoError(t, err)

				descCommOut, err := client.DescribeCommunications(
					ctx,
					&supportsdk.DescribeCommunicationsInput{
						CaseId: caseID,
					},
				)
				require.NoError(t, err)
				require.NotEmpty(t, descCommOut.Communications)

				var attachmentID *string

				for _, comm := range descCommOut.Communications {
					if len(comm.AttachmentSet) > 0 {
						attachmentID = comm.AttachmentSet[0].AttachmentId
					}
				}

				require.NotNil(
					t,
					attachmentID,
					"expected the communication carrying the attachment set to echo it back",
				)

				descAttOut, err := client.DescribeAttachment(ctx, &supportsdk.DescribeAttachmentInput{
					AttachmentId: attachmentID,
				})
				require.NoError(t, err)
				require.NotNil(t, descAttOut.Attachment)
				assert.Equal(t, "s21-log.txt", aws.ToString(descAttOut.Attachment.FileName))
				assert.Equal(t, []byte("s21 attachment contents"), descAttOut.Attachment.Data)
			},
		},
		{
			name: "catalog ops",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)
				ctx := t.Context()

				svcOut, err := client.DescribeServices(ctx, &supportsdk.DescribeServicesInput{})
				require.NoError(t, err)
				assert.NotEmpty(t, svcOut.Services)

				sevOut, err := client.DescribeSeverityLevels(ctx, &supportsdk.DescribeSeverityLevelsInput{})
				require.NoError(t, err)
				assert.NotEmpty(t, sevOut.SeverityLevels)

				langOut, err := client.DescribeSupportedLanguages(
					ctx,
					&supportsdk.DescribeSupportedLanguagesInput{
						IssueType: aws.String("technical"), ServiceCode: aws.String("general-info"),
						CategoryCode: aws.String("using-aws"),
					},
				)
				require.NoError(t, err)
				assert.NotNil(t, langOut.SupportedLanguages)

				optsOut, err := client.DescribeCreateCaseOptions(
					ctx,
					&supportsdk.DescribeCreateCaseOptionsInput{
						IssueType: aws.String("technical"), ServiceCode: aws.String("general-info"),
						CategoryCode: aws.String("using-aws"), Language: aws.String("en"),
					},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, optsOut.LanguageAvailability)
			},
		},
		{
			name: "trusted advisor",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)
				ctx := t.Context()
				checkID := "Pfx0RwqBli" // "Service Limits" -- real built-in catalog entry (trusted_advisor.go)

				checksOut, err := client.DescribeTrustedAdvisorChecks(
					ctx,
					&supportsdk.DescribeTrustedAdvisorChecksInput{
						Language: aws.String("en"),
					},
				)
				require.NoError(t, err)
				require.NotEmpty(t, checksOut.Checks)

				var found bool

				for _, c := range checksOut.Checks {
					if aws.ToString(c.Id) == checkID {
						found = true
					}
				}

				assert.True(t, found, "expected %s in the built-in check catalog", checkID)

				refreshOut, err := client.RefreshTrustedAdvisorCheck(
					ctx,
					&supportsdk.RefreshTrustedAdvisorCheckInput{
						CheckId: aws.String(checkID),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, refreshOut.Status)
				assert.Equal(t, checkID, aws.ToString(refreshOut.Status.CheckId))

				statusesOut, err := client.DescribeTrustedAdvisorCheckRefreshStatuses(
					ctx, &supportsdk.DescribeTrustedAdvisorCheckRefreshStatusesInput{
						CheckIds: []*string{&checkID},
					},
				)
				require.NoError(t, err)
				require.Len(t, statusesOut.Statuses, 1)
				assert.Equal(t, checkID, aws.ToString(statusesOut.Statuses[0].CheckId))

				resultOut, err := client.DescribeTrustedAdvisorCheckResult(
					ctx,
					&supportsdk.DescribeTrustedAdvisorCheckResultInput{
						CheckId: aws.String(checkID),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, resultOut.Result)
				assert.Equal(t, checkID, aws.ToString(resultOut.Result.CheckId))

				summariesOut, err := client.DescribeTrustedAdvisorCheckSummaries(
					ctx, &supportsdk.DescribeTrustedAdvisorCheckSummariesInput{
						CheckIds: []*string{&checkID},
					},
				)
				require.NoError(t, err)
				require.Len(t, summariesOut.Summaries, 1)
				assert.Equal(t, checkID, aws.ToString(summariesOut.Summaries[0].CheckId))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
