package connect_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	cn "github.com/blackbirdworks/gopherstack/services/connect"
)

func TestAttachedFileApprovalLogsWouldScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resolver fakeResolvers
		name     string
		wantErr  bool
	}{
		{
			name: "approved",
			resolver: fakeResolvers{
				objects: map[string]bool{"rec/call.wav": true},
				data:    map[string][]byte{"rec/call.wav": []byte("RIFF")},
			},
		},
		{name: "missing object", resolver: fakeResolvers{}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			voice := e.outbound(t, "+442071230001", true)
			active := e.contactARN(t, voice)
			e.stop(t, voice)
			e.f.backend.SetResolvers(tt.resolver)

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))
			in := &cn.CreateAttachedFileInput{
				FileSourceURI: "s3://rec/call.wav", FileUseCaseType: "VOICE_RECORDING", AssociatedResourceArn: active,
			}
			in.InstanceID = e.inst

			out, err := e.f.backend.CreateAttachedFile(ctx, in)
			if tt.wantErr {
				require.Error(t, err)
				assert.NotContains(t, buf.String(), "would scan")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "APPROVED", out.FileStatus)
			assert.Contains(t, buf.String(), "would scan attached file for viruses")
			assert.Contains(t, buf.String(), out.FileID)
		})
	}
}

type lockedBuf struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.String()
}

func TestCompleteAttachedFileUploadLogsWouldScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		s3      bool
		upload  bool
		wantLog bool
	}{
		{name: "local approved", upload: true, wantLog: true},
		{name: "local not uploaded"},
		{name: "s3 approved", s3: true, upload: true, wantLog: true},
		{name: "s3 not uploaded", s3: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logs := &lockedBuf{}
			e := newContactEnvOn(t, newLoggedFixture(t, logs))
			store := newFakeAttachmentStore()

			if tt.s3 {
				e.f.backend.SetResolvers(store)
			}

			resource := e.contactARN(t, e.task(t))
			start := e.startUpload(t, resource, "a.txt", int64(len(attachedText)))

			if tt.upload && tt.s3 {
				c := store.last()
				require.NoError(t, store.PutObject(t.Context(), c.bucket, c.key, []byte(attachedText)))
			}

			if tt.upload && !tt.s3 {
				url := aws.ToString(start.UploadUrlMetadata.Url)
				status, _, _ := httpDo(t, http.MethodPut, url, []byte(attachedText))
				require.Equal(t, http.StatusOK, status)
			}

			_, err := e.f.client.CompleteAttachedFileUpload(t.Context(), &sdk.CompleteAttachedFileUploadInput{
				InstanceId: aws.String(e.inst), FileId: start.FileId, AssociatedResourceArn: aws.String(resource),
			})

			out := logs.String()
			if !tt.wantLog {
				require.Error(t, err)
				assert.NotContains(t, out, "would scan")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, strings.Count(out, "would scan attached file for viruses"))
			assert.Contains(t, out, aws.ToString(start.FileId))
		})
	}
}

func TestStartAttachedFileUploadLogsWouldAttachToCase(t *testing.T) {
	t.Parallel()

	const caseARN = "arn:aws:cases:us-east-1:" + testAccount + ":domain/d1/case/c1"

	tests := []struct {
		name    string
		cases   bool
		wantLog bool
	}{
		{name: "case", cases: true, wantLog: true},
		{name: "contact"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logs := &lockedBuf{}
			e := newContactEnvOn(t, newLoggedFixture(t, logs))
			resource := caseARN

			if !tt.cases {
				resource = e.contactARN(t, e.task(t))
			}

			start := e.startUpload(t, resource, "a.txt", int64(len(attachedText)))
			out := logs.String()

			if !tt.wantLog {
				assert.NotContains(t, out, "would attach the file")

				return
			}

			assert.Equal(t, 1, strings.Count(out, "would attach the file to the Cases case"))
			assert.Contains(t, out, aws.ToString(start.FileId))
			assert.Contains(t, out, caseARN)
		})
	}
}
