package app

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	connectsdk "github.com/aws/aws-sdk-go-v2/service/connect"
	connecttypes "github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func connectDo(t *testing.T, method, target string, body []byte) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, target, bytes.NewReader(body))
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, data
}

// connectAttachedFileCase drives the attached-file upload through real S3 pre-signed URLs.
func connectAttachedFileCase(t *testing.T, e *connectWiringEnv) {
	t.Helper()

	content := []byte("wired attachment")

	task, err := e.connect.CreateContact(t.Context(), &connectsdk.CreateContactInput{
		InstanceId: e.instance.Id, Channel: connecttypes.ChannelTask,
		InitiationMethod: connecttypes.ContactInitiationMethodApi, Name: aws.String("wired task"),
	})
	require.NoError(t, err)

	desc, err := e.connect.DescribeContact(t.Context(),
		&connectsdk.DescribeContactInput{InstanceId: e.instance.Id, ContactId: task.ContactId})
	require.NoError(t, err)

	resource := desc.Contact.Arn

	start, err := e.connect.StartAttachedFileUpload(t.Context(), &connectsdk.StartAttachedFileUploadInput{
		InstanceId: e.instance.Id, AssociatedResourceArn: resource, FileName: aws.String("wired.txt"),
		FileSizeInBytes: aws.Int64(int64(len(content))), FileUseCaseType: connecttypes.FileUseCaseTypeAttachment,
	})
	require.NoError(t, err)

	upload := aws.ToString(start.UploadUrlMetadata.Url)
	parsed, err := url.Parse(upload)
	require.NoError(t, err)
	assert.Contains(t, parsed.Path, "/connect-attached-files-")
	assert.NotEmpty(t, parsed.Query().Get("X-Amz-Signature"), "a real SigV4 pre-signed S3 URL")

	complete := &connectsdk.CompleteAttachedFileUploadInput{
		InstanceId: e.instance.Id, FileId: start.FileId, AssociatedResourceArn: resource,
	}

	_, err = e.connect.CompleteAttachedFileUpload(t.Context(), complete)
	assert.Equal(t, "InvalidRequestException", connectErrCode(err), "nothing uploaded yet")

	status, _ := connectDo(t, http.MethodPut, upload, content)
	require.Equal(t, http.StatusOK, status)

	_, err = e.connect.CompleteAttachedFileUpload(t.Context(), complete)
	require.NoError(t, err)

	got, err := e.connect.GetAttachedFile(t.Context(), &connectsdk.GetAttachedFileInput{
		InstanceId: e.instance.Id, FileId: start.FileId, AssociatedResourceArn: resource,
	})
	require.NoError(t, err)
	assert.Equal(t, connecttypes.FileStatusTypeApproved, got.FileStatus)

	status, body := connectDo(t, http.MethodGet, aws.ToString(got.DownloadUrlMetadata.Url), nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, content, body)

	_, err = e.connect.DeleteAttachedFile(t.Context(), &connectsdk.DeleteAttachedFileInput{
		InstanceId: e.instance.Id, FileId: start.FileId, AssociatedResourceArn: resource,
	})
	require.NoError(t, err)

	loc := strings.SplitN(strings.TrimPrefix(parsed.Path, "/"), "/", 2)
	require.Len(t, loc, 2)

	_, err = e.objects.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: aws.String(loc[0]), Key: aws.String(loc[1])})
	require.Error(t, err, "the object is removed with the file")
}
