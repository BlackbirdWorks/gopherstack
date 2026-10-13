package support_test

import (
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	supportsdk "github.com/aws/aws-sdk-go-v2/service/support"
	"github.com/aws/aws-sdk-go-v2/service/support/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/support"
)

func TestCreateCase_CCEmailValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		emails  []string
		wantErr bool
	}{
		{name: "valid", emails: []string{"a@example.com", "b@example.org"}},
		{name: "no at", emails: []string{"nope"}, wantErr: true},
		{name: "display name", emails: []string{"Bob <b@example.com>"}, wantErr: true},
		{name: "empty entry", emails: []string{""}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			_, err := client.CreateCase(t.Context(), &supportsdk.CreateCaseInput{
				Subject: aws.String("s"), CommunicationBody: aws.String("body"),
				CcEmailAddresses: tt.emails,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ccEmailAddresses")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAddCommunication_CCEmailValidation(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	created, err := client.CreateCase(t.Context(), &supportsdk.CreateCaseInput{
		Subject: aws.String("s"), CommunicationBody: aws.String("body"),
	})
	require.NoError(t, err)

	_, err = client.AddCommunicationToCase(t.Context(), &supportsdk.AddCommunicationToCaseInput{
		CaseId: created.CaseId, CommunicationBody: aws.String("more"), CcEmailAddresses: []string{"bad"},
	})
	require.Error(t, err)
}

func TestErrors_NoCodePrefixInMessage(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)

	_, err := client.ResolveCase(t.Context(), &supportsdk.ResolveCaseInput{CaseId: aws.String("case-nope")})

	var nf *types.CaseIdNotFound
	require.ErrorAs(t, err, &nf)
	assert.NotContains(t, nf.ErrorMessage(), "CaseIdNotFound")
	assert.Contains(t, nf.ErrorMessage(), "case-nope")
	assert.Contains(t, nf.ErrorMessage(), "not found")

	_, err = client.DescribeAttachment(
		t.Context(),
		&supportsdk.DescribeAttachmentInput{AttachmentId: aws.String("att-x")},
	)

	var anf *types.AttachmentIdNotFound
	require.ErrorAs(t, err, &anf)
	assert.NotContains(t, anf.ErrorMessage(), "AttachmentIdNotFound")
}

func TestValidation_MessageHasNoCodePrefix(t *testing.T) {
	t.Parallel()

	h := newTestSupportHandler(t)
	rec := doSupportRequest(t, h, "CreateCase", map[string]any{
		"subject": "s", "communicationBody": "b", "severityCode": strings.Repeat("x", 3),
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	msg, _ := decodeSupportResponse(t, rec)["message"].(string)
	assert.NotContains(t, msg, "ValidationError")
}

func TestCaseStatus_ProgressDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		delay time.Duration
		wait  time.Duration
	}{
		{name: "disabled", delay: 0, wait: time.Hour, want: "opened"},
		{name: "before", delay: 10 * time.Second, wait: 5 * time.Second, want: "opened"},
		{name: "after", delay: 10 * time.Second, wait: 10 * time.Second, want: "work-in-progress"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := support.NewInMemoryBackend()
				b.SetCaseProgressDelay(tt.delay)

				cs, err := b.CreateCaseWithOptions(support.CreateCaseOptions{
					Subject: "s", CommunicationBody: "body",
				})
				require.NoError(t, err)

				time.Sleep(tt.wait)

				got, _, err := b.DescribeCasesWithOptions(support.DescribeCasesOptions{CaseIDs: []string{cs.CaseID}})
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, tt.want, got[0].Status)

				initial, final, err := b.ResolveCaseWithStatus(cs.CaseID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, initial)
				assert.Equal(t, "resolved", final.Status)
			})
		})
	}
}
