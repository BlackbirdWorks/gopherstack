package app

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	supportsdk "github.com/aws/aws-sdk-go-v2/service/support"
	supporttypes "github.com/aws/aws-sdk-go-v2/service/support/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestSupportAttachmentLinks_FullRouter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
	}{
		{name: "small", payload: "hello support"},
		{name: "binary", payload: "\x00\x01\x02\xff"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, cfg := devEndpointsServer(t)
			client := supportsdk.NewFromConfig(cfg)
			ctx := t.Context()
			payload := []byte(tt.payload)

			links, err := client.GetAttachmentUploadLinks(ctx, &supportsdk.GetAttachmentUploadLinksInput{
				FileName: aws.String("evidence.bin"), FileSizeBytes: aws.Int64(int64(len(payload))),
			})
			require.NoError(t, err)
			require.Len(t, links.UploadUrls, 1)

			url := aws.ToString(links.UploadUrls[0].Url)
			require.True(t, strings.HasPrefix(url, srv.URL+"/_gopherstack/support/attachments/upload/"), url)

			req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(payload))
			require.NoError(t, err)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)
			_ = resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			done, err := client.CompleteAttachmentUpload(ctx, &supportsdk.CompleteAttachmentUploadInput{
				UploadId: links.UploadId,
				CompletedUploads: []supporttypes.CompletedUpload{
					{PartIndex: aws.Int32(1), ETag: aws.String(resp.Header.Get("ETag"))},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, supporttypes.UploadStatusAttachmentReady, done.UploadStatus)

			created, err := client.CreateCase(ctx, &supportsdk.CreateCaseInput{
				Subject: aws.String("s"), CommunicationBody: aws.String("b"),
				ServiceCode: aws.String("general-info"), CategoryCode: aws.String("other"),
				SeverityCode: aws.String("low"), UploadIds: []string{aws.ToString(links.UploadId)},
			})
			require.NoError(t, err)

			comms, err := client.DescribeCommunications(
				ctx,
				&supportsdk.DescribeCommunicationsInput{CaseId: created.CaseId},
			)
			require.NoError(t, err)
			require.Len(t, comms.Communications[0].Attachments, 1)

			dl, err := client.GetAttachmentDownloadLink(ctx, &supportsdk.GetAttachmentDownloadLinkInput{
				AttachmentId: comms.Communications[0].Attachments[0].AttachmentId,
			})
			require.NoError(t, err)

			code, body := devDo(t, http.MethodGet, aws.ToString(dl.DownloadUrl.Url), "")
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, payload, body)

			code, _ = devDo(t, http.MethodGet, srv.URL+"/_gopherstack/support/attachments/download/bogus", "")
			assert.Equal(t, http.StatusForbidden, code)
		})
	}
}
