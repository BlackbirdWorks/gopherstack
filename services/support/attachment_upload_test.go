package support_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	supportsdk "github.com/aws/aws-sdk-go-v2/service/support"
	supporttypes "github.com/aws/aws-sdk-go-v2/service/support/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/support"
)

const (
	uploadPathPrefix   = "/_gopherstack/support/attachments/upload/"
	downloadPathPrefix = "/_gopherstack/support/attachments/download/"
)

type fakeClock struct{ offset atomic.Int64 }

func (c *fakeClock) now() time.Time {
	return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(c.offset.Load()))
}

func (c *fakeClock) advance(d time.Duration) { c.offset.Add(int64(d)) }

type uploadStack struct {
	backend *support.InMemoryBackend
	client  *supportsdk.Client
	clock   *fakeClock
	url     string
}

func newUploadStack(t *testing.T, partSize int64, delay time.Duration) *uploadStack {
	t.Helper()

	clock := &fakeClock{}
	backend := support.NewInMemoryBackend()
	backend.SetClock(clock.now)
	backend.SetUploadPartSize(partSize)
	backend.SetUploadProcessingDelay(delay)

	h := support.NewHandler(backend)
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	h.RegisterInspectionRoutes(e)

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return &uploadStack{
		backend: backend,
		clock:   clock,
		url:     srv.URL,
		client: supportsdk.NewFromConfig(cfg, func(o *supportsdk.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
		}),
	}
}

func doRaw(t *testing.T, method, url string, body []byte) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, bytes.NewReader(body))
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, data
}

func startUpload(
	ctx context.Context, t *testing.T, s *uploadStack, name string, size int64,
) *supportsdk.GetAttachmentUploadLinksOutput {
	t.Helper()

	out, err := s.client.GetAttachmentUploadLinks(ctx, &supportsdk.GetAttachmentUploadLinksInput{
		FileName: aws.String(name), FileSizeBytes: aws.Int64(size),
	})
	require.NoError(t, err)

	return out
}

func putParts(
	t *testing.T, links []supporttypes.UploadUrl, payload []byte, partSize int,
) []supporttypes.CompletedUpload {
	t.Helper()

	completed := make([]supporttypes.CompletedUpload, 0, len(links))

	for _, link := range links {
		start := (int(link.PartIndex) - 1) * partSize
		end := min(start+partSize, len(payload))

		resp, _ := doRaw(t, http.MethodPut, aws.ToString(link.Url), payload[start:end])
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NotEmpty(t, resp.Header.Get("ETag"))

		completed = append(completed, supporttypes.CompletedUpload{
			PartIndex: aws.Int32(link.PartIndex), ETag: aws.String(resp.Header.Get("ETag")),
		})
	}

	return completed
}

func TestAttachmentUpload_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		payload    string
		partSize   int64
		totalParts int32
	}{
		{name: "single part", payload: "hello", partSize: 8, totalParts: 1},
		{name: "exact multiple", payload: "abcdefgh", partSize: 4, totalParts: 2},
		{name: "ragged last part", payload: "0123456789", partSize: 4, totalParts: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newUploadStack(t, tt.partSize, 0)
			ctx := t.Context()
			payload := []byte(tt.payload)

			links := startUpload(ctx, t, s, "log.txt", int64(len(payload)))
			assert.Equal(t, tt.partSize, aws.ToInt64(links.PartSizeBytes))
			assert.Equal(t, tt.totalParts, links.TotalParts)
			assert.NotEmpty(t, aws.ToString(links.UploadId))
			assert.Zero(t, links.NextIndex)
			require.Len(t, links.UploadUrls, int(tt.totalParts))

			for i, u := range links.UploadUrls {
				assert.Equal(t, int32(i+1), u.PartIndex)
				assert.True(t, strings.HasPrefix(aws.ToString(u.Url), s.url+uploadPathPrefix), aws.ToString(u.Url))
				expiry, err := time.Parse(time.RFC3339, aws.ToString(u.ExpiryDate))
				require.NoError(t, err)
				assert.Equal(t, s.clock.now().Add(time.Hour), expiry)
			}

			status, err := s.client.DescribeAttachmentUploadStatus(ctx, &supportsdk.DescribeAttachmentUploadStatusInput{
				UploadId: links.UploadId,
			})
			require.NoError(t, err)
			assert.Equal(t, "log.txt", aws.ToString(status.FileName))
			assert.Equal(t, supporttypes.UploadStatusAttachmentNotReady, status.UploadStatus)
			assert.Equal(t, tt.totalParts, aws.ToInt32(status.UploadProgress.TotalParts))
			assert.Zero(t, aws.ToInt32(status.UploadProgress.CompletedPartsCount))

			completed := putParts(t, links.UploadUrls, payload, int(tt.partSize))
			done, err := s.client.CompleteAttachmentUpload(ctx, &supportsdk.CompleteAttachmentUploadInput{
				UploadId: links.UploadId, CompletedUploads: completed,
			})
			require.NoError(t, err)
			assert.Equal(t, supporttypes.UploadStatusAttachmentReady, done.UploadStatus)

			status, err = s.client.DescribeAttachmentUploadStatus(ctx, &supportsdk.DescribeAttachmentUploadStatusInput{
				UploadId: links.UploadId,
			})
			require.NoError(t, err)
			assert.Equal(t, supporttypes.UploadStatusAttachmentReady, status.UploadStatus)
			assert.Equal(t, tt.totalParts, aws.ToInt32(status.UploadProgress.CompletedPartsCount))

			created, err := s.client.CreateCase(ctx, &supportsdk.CreateCaseInput{
				Subject: aws.String("s"), CommunicationBody: aws.String("b"),
				ServiceCode: aws.String("general-info"), CategoryCode: aws.String("other"),
				SeverityCode: aws.String("low"), UploadIds: []string{aws.ToString(links.UploadId)},
			})
			require.NoError(t, err)

			comms, err := s.client.DescribeCommunications(ctx, &supportsdk.DescribeCommunicationsInput{
				CaseId: created.CaseId,
			})
			require.NoError(t, err)
			require.Len(t, comms.Communications, 1)
			require.Len(t, comms.Communications[0].Attachments, 1)
			assert.Len(t, comms.Communications[0].AttachmentSet, 1)

			att := comms.Communications[0].Attachments[0]
			assert.Equal(t, "log.txt", aws.ToString(att.FileName))

			dl, err := s.client.GetAttachmentDownloadLink(ctx, &supportsdk.GetAttachmentDownloadLinkInput{
				AttachmentId: att.AttachmentId,
			})
			require.NoError(t, err)
			assert.Equal(t, "log.txt", aws.ToString(dl.FileName))
			assert.True(t, strings.HasPrefix(aws.ToString(dl.DownloadUrl.Url), s.url+downloadPathPrefix))
			expiry, err := time.Parse(time.RFC3339, aws.ToString(dl.DownloadUrl.ExpiryDate))
			require.NoError(t, err)
			assert.Equal(t, s.clock.now().Add(time.Hour), expiry)

			resp, body := doRaw(t, http.MethodGet, aws.ToString(dl.DownloadUrl.Url), nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, payload, body)
			assert.Contains(t, resp.Header.Get("Content-Disposition"), "log.txt")

			desc, err := s.client.DescribeAttachment(
				ctx,
				&supportsdk.DescribeAttachmentInput{AttachmentId: att.AttachmentId},
			)
			require.NoError(t, err)
			assert.Equal(t, payload, desc.Attachment.Data)

			_, err = s.client.AddCommunicationToCase(ctx, &supportsdk.AddCommunicationToCaseInput{
				CaseId: created.CaseId, CommunicationBody: aws.String("again"),
				UploadIds: []string{aws.ToString(links.UploadId)},
			})
			var notFound *supporttypes.AttachmentSetIdNotFound
			require.ErrorAs(t, err, &notFound, "an upload attaches once")
		})
	}
}

func TestAttachmentUpload_LinkPagination(t *testing.T) {
	t.Parallel()

	s := newUploadStack(t, 1, 0)
	ctx := t.Context()

	first := startUpload(ctx, t, s, "big.bin", 25)
	assert.Equal(t, int32(25), first.TotalParts)
	require.Len(t, first.UploadUrls, 10)
	assert.Equal(t, int32(11), first.NextIndex)

	tests := []struct {
		rng       *supporttypes.UploadRange
		name      string
		wantCount int
		wantFirst int32
		wantNext  int32
	}{
		{nil, "default range", 10, 1, 11},
		{&supporttypes.UploadRange{StartIndex: aws.Int32(11), EndIndex: aws.Int32(21)}, "second page", 10, 11, 21},
		{&supporttypes.UploadRange{StartIndex: aws.Int32(21)}, "last page", 5, 21, 0},
		{&supporttypes.UploadRange{StartIndex: aws.Int32(3), EndIndex: aws.Int32(5)}, "small range", 2, 3, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := s.client.GetAttachmentUploadLinks(ctx, &supportsdk.GetAttachmentUploadLinksInput{
				FileName: aws.String("big.bin"), UploadId: first.UploadId, UploadRange: tt.rng,
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.UploadId), aws.ToString(out.UploadId))
			require.Len(t, out.UploadUrls, tt.wantCount)
			assert.Equal(t, tt.wantFirst, out.UploadUrls[0].PartIndex)
			assert.Equal(t, tt.wantNext, out.NextIndex)
		})
	}
}

func TestAttachmentUpload_APIErrors(t *testing.T) {
	t.Parallel()

	s := newUploadStack(t, 4, 0)
	ctx := t.Context()
	links := startUpload(ctx, t, s, "a.txt", 8)

	tests := []struct {
		wantErr any
		call    func() error
		name    string
	}{
		{
			name: "links unknown upload", wantErr: new(*supporttypes.UploadIdNotFound),
			call: func() error {
				_, err := s.client.GetAttachmentUploadLinks(ctx, &supportsdk.GetAttachmentUploadLinksInput{
					FileName: aws.String("a"), UploadId: aws.String("nope"),
				})

				return err
			},
		},
		{
			name: "status unknown upload", wantErr: new(*supporttypes.UploadIdNotFound),
			call: func() error {
				_, err := s.client.DescribeAttachmentUploadStatus(ctx, &supportsdk.DescribeAttachmentUploadStatusInput{
					UploadId: aws.String("nope"),
				})

				return err
			},
		},
		{
			name: "complete unknown upload", wantErr: new(*supporttypes.UploadIdNotFound),
			call: func() error {
				_, err := s.client.CompleteAttachmentUpload(ctx, &supportsdk.CompleteAttachmentUploadInput{
					UploadId: aws.String("nope"),
					CompletedUploads: []supporttypes.CompletedUpload{
						{PartIndex: aws.Int32(1), ETag: aws.String("x")},
					},
				})

				return err
			},
		},
		{
			name: "download unknown attachment", wantErr: new(*supporttypes.AttachmentIdNotFound),
			call: func() error {
				_, err := s.client.GetAttachmentDownloadLink(ctx, &supportsdk.GetAttachmentDownloadLinkInput{
					AttachmentId: aws.String("att-missing"),
				})

				return err
			},
		},
		{
			name: "links dry run", wantErr: new(*supporttypes.DryRunOperationException),
			call: func() error {
				_, err := s.client.GetAttachmentUploadLinks(ctx, &supportsdk.GetAttachmentUploadLinksInput{
					FileName: aws.String("a"), FileSizeBytes: aws.Int64(3), DryRun: aws.Bool(true),
				})

				return err
			},
		},
		{
			name: "status dry run", wantErr: new(*supporttypes.DryRunOperationException),
			call: func() error {
				_, err := s.client.DescribeAttachmentUploadStatus(ctx, &supportsdk.DescribeAttachmentUploadStatusInput{
					UploadId: links.UploadId, DryRun: aws.Bool(true),
				})

				return err
			},
		},
		{
			name: "complete part never uploaded", wantErr: nil,
			call: func() error {
				_, err := s.client.CompleteAttachmentUpload(ctx, &supportsdk.CompleteAttachmentUploadInput{
					UploadId: links.UploadId,
					CompletedUploads: []supporttypes.CompletedUpload{
						{PartIndex: aws.Int32(1), ETag: aws.String("x")},
					},
				})

				return err
			},
		},
		{
			name: "links size too large", wantErr: nil,
			call: func() error {
				_, err := s.client.GetAttachmentUploadLinks(ctx, &supportsdk.GetAttachmentUploadLinksInput{
					FileName: aws.String("a"), FileSizeBytes: aws.Int64(157286401),
				})

				return err
			},
		},
		{
			name: "links size zero", wantErr: nil,
			call: func() error {
				_, err := s.client.GetAttachmentUploadLinks(ctx, &supportsdk.GetAttachmentUploadLinksInput{
					FileName: aws.String("a"), FileSizeBytes: aws.Int64(0),
				})

				return err
			},
		},
		{
			name: "links range too wide", wantErr: nil,
			call: func() error {
				_, err := s.client.GetAttachmentUploadLinks(ctx, &supportsdk.GetAttachmentUploadLinksInput{
					FileName: aws.String("a"), UploadId: links.UploadId,
					UploadRange: &supporttypes.UploadRange{StartIndex: aws.Int32(1), EndIndex: aws.Int32(13)},
				})

				return err
			},
		},
		{
			name: "links range past end", wantErr: nil,
			call: func() error {
				_, err := s.client.GetAttachmentUploadLinks(ctx, &supportsdk.GetAttachmentUploadLinksInput{
					FileName: aws.String("a"), UploadId: links.UploadId,
					UploadRange: &supporttypes.UploadRange{StartIndex: aws.Int32(9)},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call()
			require.Error(t, err)

			switch want := tt.wantErr.(type) {
			case **supporttypes.UploadIdNotFound:
				require.ErrorAs(t, err, want)
			case **supporttypes.AttachmentIdNotFound:
				require.ErrorAs(t, err, want)
			case **supporttypes.DryRunOperationException:
				require.ErrorAs(t, err, want)
			default:
				assert.Contains(t, err.Error(), "ValidationException")
			}
		})
	}
}

func TestAttachmentUpload_CompleteValidation(t *testing.T) {
	t.Parallel()

	s := newUploadStack(t, 4, 0)
	ctx := t.Context()
	links := startUpload(ctx, t, s, "a.txt", 8)
	completed := putParts(t, links.UploadUrls[:1], []byte("abcdefgh"), 4)

	tests := []struct {
		name  string
		parts []supporttypes.CompletedUpload
	}{
		{"wrong etag", []supporttypes.CompletedUpload{{PartIndex: aws.Int32(1), ETag: aws.String("\"bad\"")}}},
		{"part out of range", []supporttypes.CompletedUpload{{PartIndex: aws.Int32(9), ETag: completed[0].ETag}}},
		{"part not uploaded", []supporttypes.CompletedUpload{{PartIndex: aws.Int32(2), ETag: completed[0].ETag}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := s.client.CompleteAttachmentUpload(ctx, &supportsdk.CompleteAttachmentUploadInput{
				UploadId: links.UploadId, CompletedUploads: tt.parts,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
		})
	}

	t.Run("partial report stays not ready", func(t *testing.T) {
		t.Parallel()

		out, err := s.client.CompleteAttachmentUpload(ctx, &supportsdk.CompleteAttachmentUploadInput{
			UploadId: links.UploadId, CompletedUploads: completed,
		})
		require.NoError(t, err)
		assert.Equal(t, supporttypes.UploadStatusAttachmentNotReady, out.UploadStatus)

		status, err := s.client.DescribeAttachmentUploadStatus(ctx, &supportsdk.DescribeAttachmentUploadStatusInput{
			UploadId: links.UploadId,
		})
		require.NoError(t, err)
		assert.Equal(t, int32(1), aws.ToInt32(status.UploadProgress.CompletedPartsCount))
	})
}

func TestAttachmentUpload_HTTPLinks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run        func(t *testing.T, s *uploadStack, put, get string) *http.Response
		name       string
		wantStatus int
	}{
		{
			name: "put ok", wantStatus: http.StatusOK,
			run: func(t *testing.T, _ *uploadStack, put, _ string) *http.Response {
				t.Helper()
				resp, _ := doRaw(t, http.MethodPut, put, []byte("abcd"))

				return resp
			},
		},
		{
			name: "put wrong size", wantStatus: http.StatusBadRequest,
			run: func(t *testing.T, _ *uploadStack, put, _ string) *http.Response {
				t.Helper()
				resp, _ := doRaw(t, http.MethodPut, put, []byte("abc"))

				return resp
			},
		},
		{
			name: "put oversize", wantStatus: http.StatusBadRequest,
			run: func(t *testing.T, _ *uploadStack, put, _ string) *http.Response {
				t.Helper()
				resp, _ := doRaw(t, http.MethodPut, put, []byte("abcde"))

				return resp
			},
		},
		{
			name: "put unknown token", wantStatus: http.StatusForbidden,
			run: func(t *testing.T, s *uploadStack, _, _ string) *http.Response {
				t.Helper()
				resp, _ := doRaw(t, http.MethodPut, s.url+uploadPathPrefix+"nope", []byte("abcd"))

				return resp
			},
		},
		{
			name: "put expired", wantStatus: http.StatusForbidden,
			run: func(t *testing.T, s *uploadStack, put, _ string) *http.Response {
				t.Helper()
				s.clock.advance(time.Hour + time.Second)
				resp, _ := doRaw(t, http.MethodPut, put, []byte("abcd"))

				return resp
			},
		},
		{
			name: "put to download token", wantStatus: http.StatusForbidden,
			run: func(t *testing.T, _ *uploadStack, _, get string) *http.Response {
				t.Helper()
				resp, _ := doRaw(
					t,
					http.MethodPut,
					strings.Replace(get, downloadPathPrefix, uploadPathPrefix, 1),
					[]byte("x"),
				)

				return resp
			},
		},
		{
			name: "get ok", wantStatus: http.StatusOK,
			run: func(t *testing.T, _ *uploadStack, _, get string) *http.Response {
				t.Helper()
				resp, _ := doRaw(t, http.MethodGet, get, nil)

				return resp
			},
		},
		{
			name: "get expired", wantStatus: http.StatusForbidden,
			run: func(t *testing.T, s *uploadStack, _, get string) *http.Response {
				t.Helper()
				s.clock.advance(time.Hour + time.Second)
				resp, _ := doRaw(t, http.MethodGet, get, nil)

				return resp
			},
		},
		{
			name: "get unknown token", wantStatus: http.StatusForbidden,
			run: func(t *testing.T, s *uploadStack, _, _ string) *http.Response {
				t.Helper()
				resp, _ := doRaw(t, http.MethodGet, s.url+downloadPathPrefix+"nope", nil)

				return resp
			},
		},
		{
			name: "get upload token", wantStatus: http.StatusForbidden,
			run: func(t *testing.T, _ *uploadStack, put, _ string) *http.Response {
				t.Helper()
				resp, _ := doRaw(t, http.MethodGet, strings.Replace(put, uploadPathPrefix, downloadPathPrefix, 1), nil)

				return resp
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newUploadStack(t, 4, 0)
			ctx := t.Context()
			links := startUpload(ctx, t, s, "a.txt", 4)
			s.backend.AddAttachmentInternal(
				&support.Attachment{AttachmentID: "att-1", FileName: "k.txt", Data: []byte("data")},
			)

			dl, err := s.client.GetAttachmentDownloadLink(ctx, &supportsdk.GetAttachmentDownloadLinkInput{
				AttachmentId: aws.String("att-1"),
			})
			require.NoError(t, err)

			resp := tt.run(t, s, aws.ToString(links.UploadUrls[0].Url), aws.ToString(dl.DownloadUrl.Url))
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}

func TestAttachmentUpload_ProcessingDelay(t *testing.T) {
	t.Parallel()

	s := newUploadStack(t, 4, time.Minute)
	ctx := t.Context()
	links := startUpload(ctx, t, s, "a.txt", 4)
	completed := putParts(t, links.UploadUrls, []byte("abcd"), 4)

	out, err := s.client.CompleteAttachmentUpload(ctx, &supportsdk.CompleteAttachmentUploadInput{
		UploadId: links.UploadId, CompletedUploads: completed,
	})
	require.NoError(t, err)
	assert.Equal(t, supporttypes.UploadStatusAttachmentNotReady, out.UploadStatus)

	createInput := &supportsdk.CreateCaseInput{
		Subject: aws.String("s"), CommunicationBody: aws.String("b"),
		ServiceCode: aws.String("general-info"), CategoryCode: aws.String("other"),
		SeverityCode: aws.String("low"), UploadIds: []string{aws.ToString(links.UploadId)},
	}
	_, err = s.client.CreateCase(ctx, createInput)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ValidationException")

	s.clock.advance(time.Minute)

	status, err := s.client.DescribeAttachmentUploadStatus(ctx, &supportsdk.DescribeAttachmentUploadStatusInput{
		UploadId: links.UploadId,
	})
	require.NoError(t, err)
	assert.Equal(t, supporttypes.UploadStatusAttachmentReady, status.UploadStatus)

	_, err = s.client.CreateCase(ctx, createInput)
	require.NoError(t, err)
}

func TestAttachmentUpload_Persistence(t *testing.T) {
	t.Parallel()

	s := newUploadStack(t, 4, 0)
	ctx := t.Context()
	links := startUpload(ctx, t, s, "a.txt", 8)

	resp, _ := doRaw(t, http.MethodPut, aws.ToString(links.UploadUrls[0].Url), []byte("abcd"))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	snap := s.backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	restored := newUploadStack(t, 4, 0)
	require.NoError(t, restored.backend.Restore(ctx, snap))

	status, err := restored.client.DescribeAttachmentUploadStatus(ctx, &supportsdk.DescribeAttachmentUploadStatusInput{
		UploadId: links.UploadId,
	})
	require.NoError(t, err)
	assert.Equal(t, "a.txt", aws.ToString(status.FileName))

	// the pending link survives the round trip (path only: the host differs per server)
	path := strings.TrimPrefix(aws.ToString(links.UploadUrls[1].Url), s.url)
	resp, _ = doRaw(t, http.MethodPut, restored.url+path, []byte("efgh"))
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
