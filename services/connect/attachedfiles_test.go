package connect_test

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func (e *contactEnv) contactARN(t *testing.T, id string) string {
	t.Helper()

	return aws.ToString(e.describe(t, id).Arn)
}

func (e *contactEnv) startUpload(t *testing.T, resource, name string, size int64) *sdk.StartAttachedFileUploadOutput {
	t.Helper()

	out, err := e.f.client.StartAttachedFileUpload(t.Context(), &sdk.StartAttachedFileUploadInput{
		InstanceId: aws.String(e.inst), AssociatedResourceArn: aws.String(resource), FileName: aws.String(name),
		FileSizeInBytes: aws.Int64(size), FileUseCaseType: types.FileUseCaseTypeAttachment,
		Tags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	return out
}

func httpDo(t *testing.T, method, target string, body []byte) (int, []byte, http.Header) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, target, bytes.NewReader(body))
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, data, resp.Header
}

func TestAttachedFileUploadLifecycle(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	resource := e.contactARN(t, e.task(t))
	content := []byte("hello attachment")
	start := e.startUpload(t, resource, "notes.txt", int64(len(content)))

	require.NotNil(t, start.UploadUrlMetadata)
	assert.Equal(t, types.FileStatusTypeProcessing, start.FileStatus)
	assert.Contains(t, aws.ToString(start.FileArn), "/attached-file/"+aws.ToString(start.FileId))

	ref := func() (*string, *string, *string) {
		return aws.String(e.inst), start.FileId, aws.String(resource)
	}
	complete := func() error {
		inst, id, res := ref()
		_, err := e.f.client.CompleteAttachedFileUpload(t.Context(),
			&sdk.CompleteAttachedFileUploadInput{InstanceId: inst, FileId: id, AssociatedResourceArn: res})

		return err
	}

	assert.Equal(t, "InvalidRequestException", apiCode(complete()), "nothing uploaded yet")

	inst, id, res := ref()
	got, err := e.f.client.GetAttachedFile(t.Context(),
		&sdk.GetAttachedFileInput{InstanceId: inst, FileId: id, AssociatedResourceArn: res})
	require.NoError(t, err)
	assert.Nil(t, got.DownloadUrlMetadata, "only approved files can be downloaded")
	assert.Equal(t, types.FileStatusTypeProcessing, got.FileStatus)
	assert.Equal(t, map[string]string{"k": "v"}, got.Tags)

	status, _, _ := httpDo(t, http.MethodPut, aws.ToString(start.UploadUrlMetadata.Url)+"x", content)
	assert.Equal(t, http.StatusForbidden, status, "tampered signature")

	status, _, _ = httpDo(t, http.MethodPut, aws.ToString(start.UploadUrlMetadata.Url), content)
	require.Equal(t, http.StatusOK, status)

	require.NoError(t, complete())
	assert.Equal(t, "InvalidRequestException", apiCode(complete()), "already approved")

	got, err = e.f.client.GetAttachedFile(t.Context(),
		&sdk.GetAttachedFileInput{InstanceId: inst, FileId: id, AssociatedResourceArn: res})
	require.NoError(t, err)
	require.Equal(t, types.FileStatusTypeApproved, got.FileStatus)
	assert.Equal(t, "notes.txt", aws.ToString(got.FileName))
	assert.Equal(t, int64(len(content)), aws.ToInt64(got.FileSizeInBytes))
	assert.Equal(t, resource, aws.ToString(got.AssociatedResourceArn))

	status, body, hdr := httpDo(t, http.MethodGet, aws.ToString(got.DownloadUrlMetadata.Url), nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, content, body)
	assert.Contains(t, hdr.Get("Content-Disposition"), "notes.txt")

	status, _, _ = httpDo(t, http.MethodPut, aws.ToString(start.UploadUrlMetadata.Url), content)
	assert.Equal(t, http.StatusConflict, status, "approved files take no more uploads")

	e.f.clock.advance(10 * time.Minute)

	status, _, _ = httpDo(t, http.MethodGet, aws.ToString(got.DownloadUrlMetadata.Url), nil)
	assert.Equal(t, http.StatusForbidden, status, "expired")

	batch, err := e.f.client.BatchGetAttachedFileMetadata(t.Context(), &sdk.BatchGetAttachedFileMetadataInput{
		InstanceId: inst, AssociatedResourceArn: res, FileIds: []string{aws.ToString(id), "missing"},
	})
	require.NoError(t, err)
	require.Len(t, batch.Files, 1)
	require.Len(t, batch.Errors, 1)
	assert.Equal(t, "missing", aws.ToString(batch.Errors[0].FileId))

	_, err = e.f.client.DeleteAttachedFile(t.Context(),
		&sdk.DeleteAttachedFileInput{InstanceId: inst, FileId: id, AssociatedResourceArn: res})
	require.NoError(t, err)

	_, err = e.f.client.GetAttachedFile(t.Context(),
		&sdk.GetAttachedFileInput{InstanceId: inst, FileId: id, AssociatedResourceArn: res})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = e.f.client.DeleteAttachedFile(t.Context(),
		&sdk.DeleteAttachedFileInput{InstanceId: inst, FileId: id, AssociatedResourceArn: res})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestAttachedFileUploadSizeMustMatch(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	resource := e.contactARN(t, e.task(t))
	start := e.startUpload(t, resource, "short.txt", 10)

	status, _, _ := httpDo(t, http.MethodPut, aws.ToString(start.UploadUrlMetadata.Url), []byte("abc"))
	require.Equal(t, http.StatusOK, status)

	_, err := e.f.client.CompleteAttachedFileUpload(t.Context(), &sdk.CompleteAttachedFileUploadInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err))
}

func TestStartAttachedFileUploadValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(t *testing.T, e *contactEnv, in *sdk.StartAttachedFileUploadInput)
		name string
		code string
	}{
		{name: "bad extension", code: "InvalidParameterException", mut: func(_ *testing.T, _ *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.FileName = aws.String("run.exe")
		}},
		{name: "no extension", code: "InvalidParameterException", mut: func(_ *testing.T, _ *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.FileName = aws.String("README")
		}},
		{name: "too big", code: "InvalidParameterException", mut: func(_ *testing.T, _ *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.FileSizeInBytes = aws.Int64(30 * 1024 * 1024)
		}},
		{name: "zero size", code: "InvalidParameterException", mut: func(_ *testing.T, _ *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.FileSizeInBytes = aws.Int64(0)
		}},
		{name: "wrong use case", code: "InvalidParameterException", mut: func(_ *testing.T, _ *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.FileUseCaseType = types.FileUseCaseTypeVoiceRecording
		}},
		{name: "expiry out of range", code: "InvalidParameterException", mut: func(_ *testing.T, _ *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.UrlExpiryInSeconds = aws.Int32(3600)
		}},
		{name: "voice contact", code: "InvalidParameterException", mut: func(t *testing.T, e *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.AssociatedResourceArn = aws.String(e.contactARN(t, e.outbound(t, "+442071230001", true)))
		}},
		{name: "unknown contact", code: "ResourceNotFoundException", mut: func(_ *testing.T, e *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.AssociatedResourceArn = aws.String("arn:aws:connect:us-east-1:123456789012:instance/" + e.inst + "/contact/nope")
		}},
		{name: "not a resource", code: "InvalidParameterException", mut: func(_ *testing.T, _ *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.AssociatedResourceArn = aws.String("nonsense")
		}},
		{name: "both creators", code: "InvalidParameterException", mut: func(_ *testing.T, _ *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.CreatedBy = &types.CreatedByInfoMemberConnectUserArn{Value: "arn:aws:connect:us-east-1:123456789012:instance/x/agent/y"}
			in.FileName = aws.String("run.exe")
		}},
		{name: "unknown instance", code: "ResourceNotFoundException", mut: func(_ *testing.T, _ *contactEnv, in *sdk.StartAttachedFileUploadInput) {
			in.InstanceId = aws.String("missing")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			in := &sdk.StartAttachedFileUploadInput{
				InstanceId: aws.String(e.inst), AssociatedResourceArn: aws.String(e.contactARN(t, e.task(t))),
				FileName: aws.String("a.pdf"), FileSizeInBytes: aws.Int64(10), FileUseCaseType: types.FileUseCaseTypeAttachment,
			}
			tt.mut(t, e, in)

			_, err := e.f.client.StartAttachedFileUpload(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestStartAttachedFileUploadCases(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	out, err := e.f.client.StartAttachedFileUpload(t.Context(), &sdk.StartAttachedFileUploadInput{
		InstanceId:            aws.String(e.inst),
		AssociatedResourceArn: aws.String("arn:aws:cases:us-east-1:123456789012:domain/d1/case/c1"),
		FileName:              aws.String("photo.png"), FileSizeInBytes: aws.Int64(1), FileUseCaseType: types.FileUseCaseTypeAttachment,
		CreatedBy:          &types.CreatedByInfoMemberAWSIdentityArn{Value: "arn:aws:iam::123456789012:user/me"},
		UrlExpiryInSeconds: aws.Int32(60),
	})
	require.NoError(t, err)

	u, err := url.Parse(aws.ToString(out.UploadUrlMetadata.Url))
	require.NoError(t, err)
	assert.Equal(t, "put", u.Query().Get("op"))
	assert.Equal(t, e.f.clock.now().Add(time.Minute).UTC().Format("2006-01-02T15:04:05.000Z"), aws.ToString(out.UploadUrlMetadata.UrlExpiry))

	created, ok := out.CreatedBy.(*types.CreatedByInfoMemberAWSIdentityArn)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:iam::123456789012:user/me", created.Value)
}

func TestCreateAttachedFileFromS3(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	voice := e.outbound(t, "+442071230001", true)
	active := e.contactARN(t, voice)
	e.stop(t, voice)

	e.f.backend.SetResolvers(fakeResolvers{
		objects: map[string]bool{"rec/call.wav": true},
		data:    map[string][]byte{"rec/call.wav": []byte("RIFF")},
	})

	create := func(mut func(*sdk.CreateAttachedFileInput)) (*sdk.CreateAttachedFileOutput, error) {
		in := &sdk.CreateAttachedFileInput{
			InstanceId: aws.String(e.inst), AssociatedResourceArn: aws.String(active),
			FileSourceUri: aws.String("s3://rec/call.wav"), FileUseCaseType: types.FileUseCaseTypeVoiceRecording,
		}
		mut(in)

		return e.f.client.CreateAttachedFile(t.Context(), in)
	}

	out, err := create(func(*sdk.CreateAttachedFileInput) {})
	require.NoError(t, err)
	assert.Equal(t, types.FileStatusTypeApproved, out.FileStatus)

	got, err := e.f.client.GetAttachedFile(t.Context(), &sdk.GetAttachedFileInput{
		InstanceId: aws.String(e.inst), FileId: out.FileId, AssociatedResourceArn: aws.String(active),
	})
	require.NoError(t, err)
	assert.Equal(t, "call.wav", aws.ToString(got.FileName))
	assert.Equal(t, int64(4), aws.ToInt64(got.FileSizeInBytes))
	assert.Equal(t, types.FileUseCaseTypeVoiceRecording, got.FileUseCaseType)

	_, body, _ := httpDo(t, http.MethodGet, aws.ToString(got.DownloadUrlMetadata.Url), nil)
	assert.Equal(t, []byte("RIFF"), body)

	for name, mut := range map[string]func(*sdk.CreateAttachedFileInput){
		"not s3":       func(in *sdk.CreateAttachedFileInput) { in.FileSourceUri = aws.String("https://x/y.wav") },
		"use case":     func(in *sdk.CreateAttachedFileInput) { in.FileUseCaseType = types.FileUseCaseTypeAttachment },
		"active":       func(in *sdk.CreateAttachedFileInput) { in.AssociatedResourceArn = aws.String(e.contactARN(t, e.outbound(t, "+442071230002", true))) },
		"task contact": func(in *sdk.CreateAttachedFileInput) { in.AssociatedResourceArn = aws.String(e.contactARN(t, e.task(t))) },
	} {
		_, err = create(mut)
		assert.Contains(t, []string{"InvalidParameterException", "InvalidRequestException"}, apiCode(err), name)
	}

	_, err = create(func(in *sdk.CreateAttachedFileInput) { in.FileSourceUri = aws.String("s3://rec/missing.wav") })
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestAttachedFilesConfiguration(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)

	list, err := e.f.client.ListAttachedFilesConfigurations(t.Context(), &sdk.ListAttachedFilesConfigurationsInput{InstanceId: inst})
	require.NoError(t, err)
	require.Len(t, list.AttachedFilesConfigurations, 4)

	def, err := e.f.client.DescribeAttachedFilesConfiguration(t.Context(),
		&sdk.DescribeAttachedFilesConfigurationInput{InstanceId: inst, AttachmentScope: types.AttachmentScopeTask})
	require.NoError(t, err)
	assert.Equal(t, int64(20*1024*1024), aws.ToInt64(def.AttachedFilesConfiguration.MaximumSizeLimitInBytes))
	assert.Nil(t, def.AttachedFilesConfiguration.LastModifiedTime)

	upd, err := e.f.client.UpdateAttachedFilesConfiguration(t.Context(), &sdk.UpdateAttachedFilesConfigurationInput{
		InstanceId: inst, AttachmentScope: types.AttachmentScopeTask, MaximumSizeLimitInBytes: aws.Int64(100),
		ExtensionConfiguration: &types.ExtensionConfiguration{
			AllowedExtensions: []types.AllowedExtension{{Extension: aws.String("zip")}, {Extension: aws.String("ZIP")}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(100), aws.ToInt64(upd.MaximumSizeLimitInBytes))
	require.Len(t, upd.ExtensionConfiguration.AllowedExtensions, 1)
	assert.NotNil(t, upd.LastModifiedTime)

	again, err := e.f.client.DescribeAttachedFilesConfiguration(t.Context(),
		&sdk.DescribeAttachedFilesConfigurationInput{InstanceId: inst, AttachmentScope: types.AttachmentScopeTask})
	require.NoError(t, err)
	assert.Equal(t, "zip", aws.ToString(again.AttachedFilesConfiguration.ExtensionConfiguration.AllowedExtensions[0].Extension))

	resource := e.contactARN(t, e.task(t))
	_, err = e.f.client.StartAttachedFileUpload(t.Context(), &sdk.StartAttachedFileUploadInput{
		InstanceId: inst, AssociatedResourceArn: aws.String(resource), FileName: aws.String("a.pdf"),
		FileSizeInBytes: aws.Int64(10), FileUseCaseType: types.FileUseCaseTypeAttachment,
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err), "pdf is no longer allowed for tasks")

	e.startUpload(t, resource, "a.zip", 100)

	_, err = e.f.client.StartAttachedFileUpload(t.Context(), &sdk.StartAttachedFileUploadInput{
		InstanceId: inst, AssociatedResourceArn: aws.String(resource), FileName: aws.String("b.zip"),
		FileSizeInBytes: aws.Int64(101), FileUseCaseType: types.FileUseCaseTypeAttachment,
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	for name, in := range map[string]*sdk.UpdateAttachedFilesConfigurationInput{
		"empty":     {InstanceId: inst, AttachmentScope: types.AttachmentScopeChat},
		"big":       {InstanceId: inst, AttachmentScope: types.AttachmentScopeChat, MaximumSizeLimitInBytes: aws.Int64(104857601)},
		"zero":      {InstanceId: inst, AttachmentScope: types.AttachmentScopeChat, MaximumSizeLimitInBytes: aws.Int64(0)},
		"bad scope": {InstanceId: inst, AttachmentScope: "VOICE", MaximumSizeLimitInBytes: aws.Int64(1)},
		"bad ext": {InstanceId: inst, AttachmentScope: types.AttachmentScopeChat, ExtensionConfiguration: &types.ExtensionConfiguration{
			AllowedExtensions: []types.AllowedExtension{{Extension: aws.String(".pdf")}},
		}},
	} {
		_, err = e.f.client.UpdateAttachedFilesConfiguration(t.Context(), in)
		assert.Equal(t, "InvalidParameterException", apiCode(err), name)
	}

	_, err = e.f.client.DescribeAttachedFilesConfiguration(t.Context(),
		&sdk.DescribeAttachedFilesConfigurationInput{InstanceId: inst, AttachmentScope: "VOICE"})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func TestAttachedFilesSurviveRestore(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	resource := e.contactARN(t, e.task(t))
	start := e.startUpload(t, resource, "keep.txt", 4)

	status, _, _ := httpDo(t, http.MethodPut, aws.ToString(start.UploadUrlMetadata.Url), []byte("keep"))
	require.Equal(t, http.StatusOK, status)

	_, err := e.f.client.CompleteAttachedFileUpload(t.Context(), &sdk.CompleteAttachedFileUploadInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	require.NoError(t, err)

	_, err = e.f.client.UpdateAttachedFilesConfiguration(t.Context(), &sdk.UpdateAttachedFilesConfigurationInput{
		InstanceId: aws.String(e.inst), AttachmentScope: types.AttachmentScopeCase, MaximumSizeLimitInBytes: aws.Int64(7),
	})
	require.NoError(t, err)

	r := e.f.restored(t)
	got, err := r.client.GetAttachedFile(t.Context(), &sdk.GetAttachedFileInput{
		InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
	})
	require.NoError(t, err)
	assert.Equal(t, types.FileStatusTypeApproved, got.FileStatus)

	cfg, err := r.client.DescribeAttachedFilesConfiguration(t.Context(),
		&sdk.DescribeAttachedFilesConfigurationInput{InstanceId: aws.String(e.inst), AttachmentScope: types.AttachmentScopeCase})
	require.NoError(t, err)
	assert.Equal(t, int64(7), aws.ToInt64(cfg.AttachedFilesConfiguration.MaximumSizeLimitInBytes))

	tags, err := r.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: start.FileArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, tags.Tags)
}
