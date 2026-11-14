package neptune_test

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
	"github.com/blackbirdworks/gopherstack/services/neptune"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

type authCall struct{ principal, role, action, resource string }

type scriptedAuth struct {
	deny  map[string]error
	calls []authCall
	mu    sync.Mutex
}

func (a *scriptedAuth) AuthorizeRole(principal, role, action, resource string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.calls = append(a.calls, authCall{principal, role, action, resource})

	return a.deny[role+"|"+action]
}

func (a *scriptedAuth) seen() []authCall {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]authCall(nil), a.calls...)
}

const (
	roleA = "arn:aws:iam::000000000000:role/a"
	roleB = "arn:aws:iam::000000000000:role/b"
)

func TestBulkLoaderRoleAssumption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		deny        map[string]error
		name        string
		role        string
		wantStatus  string
		wantFeed    string
		wantErrCode string
		wantCall    authCall
		wantTriples string
	}{
		{name: "allowed", role: roleA, wantStatus: "LOAD_COMPLETED", wantFeed: "LOAD_COMPLETED", wantTriples: "1",
			wantCall: authCall{roleauth.PrincipalRDS, roleA, "s3:GetObject", "arn:aws:s3:::lake/d/a.nt"}},
		{name: "not assumable", role: roleA, wantStatus: "LOAD_FAILED", wantFeed: "LOAD_S3_ACCESS_DENIED_ERROR",
			wantErrCode: "S3_ACCESS_DENIED_ERROR", wantTriples: "0",
			deny:     map[string]error{roleA + "|s3:GetObject": roleauth.ErrNotAssumable},
			wantCall: authCall{roleauth.PrincipalRDS, roleA, "s3:GetObject", "arn:aws:s3:::lake/d/a.nt"}},
		{name: "policy denies read", role: roleA, wantStatus: "LOAD_FAILED", wantFeed: "LOAD_S3_ACCESS_DENIED_ERROR",
			wantErrCode: "S3_ACCESS_DENIED_ERROR", wantTriples: "0",
			deny:     map[string]error{roleA + "|s3:GetObject": roleauth.ErrAccessDenied},
			wantCall: authCall{roleauth.PrincipalRDS, roleA, "s3:GetObject", "arn:aws:s3:::lake/d/a.nt"}},
		{name: "chain checks the first hop", role: roleA + "," + roleB, wantStatus: "LOAD_FAILED",
			wantFeed: "LOAD_S3_ACCESS_DENIED_ERROR", wantErrCode: "S3_ACCESS_DENIED_ERROR", wantTriples: "0",
			deny:     map[string]error{roleA + "|sts:AssumeRole": roleauth.ErrAccessDenied},
			wantCall: authCall{roleauth.PrincipalRDS, roleA, "sts:AssumeRole", roleB}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s3b := s3backend.NewInMemoryBackend(nil)
			_, err := s3b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("lake")})
			require.NoError(t, err)
			_, err = s3b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("lake"), Key: aws.String("d/a.nt"),
				Body: strings.NewReader("<urn:s> <urn:p> <urn:o> .\n"),
			})
			require.NoError(t, err)

			b, c := newEmbeddedBackend(t)
			b.SetS3Backend(s3b)

			auth := &scriptedAuth{deny: tt.deny}
			b.SetRoleAuthorizer(auth)

			status, env, raw := loaderStartRaw(t, c,
				`{"source":"s3://lake/d/","format":"ntriples","iamRoleArn":"`+tt.role+`"}`)
			require.Equal(t, http.StatusOK, status, raw)

			id, _ := env.Payload["loadId"].(string)
			done := loaderWait(t, c, id, "?errors=true&details=true")
			assert.Equal(t, tt.wantStatus, overall(done)["status"])
			assert.Contains(t, auth.seen(), tt.wantCall)
			assert.Equal(t, tt.wantTriples, countTriples(t, c, "?s ?p ?o"))

			if tt.wantErrCode != "" {
				failed, _ := done.Payload["failedFeeds"].([]any)
				require.Len(t, failed, 1)
				assert.Equal(t, tt.wantFeed, failed[0].(map[string]any)["status"])

				errs, _ := done.Payload["errors"].(map[string]any)
				logs, _ := errs["errorLogs"].([]any)
				require.NotEmpty(t, logs)
				assert.Equal(t, tt.wantErrCode, logs[0].(map[string]any)["errorCode"])
			}
		})
	}
}

type kmsS3 struct {
	neptune.S3Reader
	key string
}

func (k *kmsS3) GetObject(ctx context.Context, in *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error) {
	out, err := k.S3Reader.GetObject(ctx, in)
	if err == nil {
		out.ServerSideEncryption, out.SSEKMSKeyId = s3types.ServerSideEncryptionAwsKms, aws.String(k.key)
	}

	return out, err
}

func TestBulkLoaderSSEKMS(t *testing.T) {
	t.Parallel()

	const key = "arn:aws:kms:us-east-1:000000000000:key/k1"

	tests := []struct {
		deny       map[string]error
		name       string
		wantStatus string
		wantTriple string
	}{
		{name: "decrypt allowed", wantStatus: "LOAD_COMPLETED", wantTriple: "1"},
		{name: "decrypt denied", wantStatus: "LOAD_FAILED", wantTriple: "0",
			deny: map[string]error{roleA + "|kms:Decrypt": roleauth.ErrAccessDenied}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s3b := s3backend.NewInMemoryBackend(nil)
			_, err := s3b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("lake")})
			require.NoError(t, err)
			_, err = s3b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("lake"), Key: aws.String("d/a.nt"),
				Body: strings.NewReader("<urn:s> <urn:p> <urn:o> .\n"),
			})
			require.NoError(t, err)

			b, c := newEmbeddedBackend(t)
			b.SetS3Backend(&kmsS3{S3Reader: s3b, key: key})

			auth := &scriptedAuth{deny: tt.deny}
			b.SetRoleAuthorizer(auth)

			status, env, raw := loaderStartRaw(t, c,
				`{"source":"s3://lake/d/","format":"ntriples","iamRoleArn":"`+roleA+`"}`)
			require.Equal(t, http.StatusOK, status, raw)

			id, _ := env.Payload["loadId"].(string)
			done := loaderWait(t, c, id, "")
			assert.Equal(t, tt.wantStatus, overall(done)["status"])
			assert.Contains(t, auth.seen(), authCall{roleauth.PrincipalRDS, roleA, "kms:Decrypt", key})
			assert.Equal(t, tt.wantTriple, countTriples(t, c, "?s ?p ?o"))
		})
	}
}

func TestBulkLoaderResumeModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mode        string
		wantTriples string
		wantMessage string
		wantRun     float64
		wantStatus  int
		prepare     bool
	}{
		{name: "resume reloads only failed feeds", mode: "RESUME", prepare: true, wantStatus: 200, wantRun: 2,
			wantTriples: "1"},
		{name: "auto resumes", mode: "AUTO", prepare: true, wantStatus: 200, wantRun: 2, wantTriples: "1"},
		{name: "new reloads everything", mode: "NEW", prepare: true, wantStatus: 200, wantRun: 1, wantTriples: "2"},
		{name: "resume without history", mode: "RESUME", prepare: false, wantStatus: 400,
			wantMessage: "no previous load job"},
		{name: "auto without history", mode: "AUTO", prepare: false, wantStatus: 200, wantRun: 1, wantTriples: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s3b := s3backend.NewInMemoryBackend(nil)
			_, err := s3b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("lake")})
			require.NoError(t, err)

			put := func(key, body string) {
				_, perr := s3b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
					Bucket: aws.String("lake"), Key: aws.String(key), Body: strings.NewReader(body),
				})
				require.NoError(t, perr)
			}

			const (
				src  = `"source":"s3://lake/d/","format":"ntriples"`
				good = "<urn:b> <urn:p> <urn:o> .\n"
			)

			put("d/a.nt", "<urn:a> <urn:p> <urn:o> .\n")
			put("d/b.nt", map[bool]string{true: "<urn:b> <urn:p> broken\n", false: good}[tt.prepare])

			b, c := newEmbeddedBackend(t)
			b.SetS3Backend(s3b)

			if tt.prepare {
				_, env, _ := loaderStart(t, c, `{`+src+`}`)
				id, _ := env.Payload["loadId"].(string)
				first := loaderWait(t, c, id, "")
				require.Equal(t, "LOAD_FAILED", overall(first)["status"])
				require.Equal(t, "1", countTriples(t, c, "<urn:a> ?p ?o"))

				res := sparqlDo(t, c, http.MethodPost, "/sparql", formCT, "",
					url.Values{"update": {"DROP ALL"}}.Encode())
				require.Equal(t, http.StatusOK, res.status, res.body)

				put("d/b.nt", good)
			}

			status, env, raw := loaderStart(t, c, `{`+src+`,"mode":"`+tt.mode+`","failOnError":"FALSE"}`)
			require.Equal(t, tt.wantStatus, status, raw)

			if tt.wantMessage != "" {
				assert.Contains(t, raw, tt.wantMessage)

				return
			}

			id, _ := env.Payload["loadId"].(string)
			done := loaderWait(t, c, id, "")
			assert.InDelta(t, tt.wantRun, overall(done)["runNumber"], 0)
			assert.Equal(t, tt.wantTriples, countTriples(t, c, "?s ?p ?o"))
		})
	}
}

type countingS3 struct {
	neptune.S3Reader
	barrier  chan struct{}
	arrivals atomic.Int32
	inflight atomic.Int32
	peak     atomic.Int32
	want     int32
	once     sync.Once
}

func (g *countingS3) GetObject(ctx context.Context, in *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error) {
	cur := g.inflight.Add(1)
	defer g.inflight.Add(-1)

	for {
		old := g.peak.Load()
		if cur <= old || g.peak.CompareAndSwap(old, cur) {
			break
		}
	}

	if g.barrier != nil {
		if g.arrivals.Add(1) >= g.want {
			g.once.Do(func() { close(g.barrier) })
		}

		select {
		case <-g.barrier:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return g.S3Reader.GetObject(ctx, in)
}

func TestBulkLoaderParallelism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		level     string
		cpus      int
		wantPeak  int32
		wantBlock bool
	}{
		{name: "oversubscribe runs feeds concurrently", level: "OVERSUBSCRIBE", cpus: 2, wantPeak: 4, wantBlock: true},
		{name: "low is a single thread", level: "LOW", cpus: 8, wantPeak: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s3b := s3backend.NewInMemoryBackend(nil)
			_, err := s3b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("lake")})
			require.NoError(t, err)

			for _, k := range []string{"a", "b", "c", "d"} {
				_, err = s3b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
					Bucket: aws.String("lake"), Key: aws.String("d/" + k + ".nt"),
					Body: strings.NewReader("<urn:" + k + "> <urn:p> <urn:o> .\n"),
				})
				require.NoError(t, err)
			}

			reader := &countingS3{S3Reader: s3b, want: tt.wantPeak}
			if tt.wantBlock {
				reader.barrier = make(chan struct{})
			}

			b, c := newEmbeddedBackend(t)
			b.SetS3Backend(reader)
			neptune.SetLoaderCPUs(b, tt.cpus)

			status, env, raw := loaderStart(t, c,
				`{"source":"s3://lake/d/","format":"ntriples","parallelism":"`+tt.level+`"}`)
			require.Equal(t, http.StatusOK, status, raw)

			id, _ := env.Payload["loadId"].(string)
			done := loaderWait(t, c, id, "")
			assert.Equal(t, "LOAD_COMPLETED", overall(done)["status"])
			assert.Equal(t, tt.wantPeak, reader.peak.Load())
			assert.Equal(t, "4", countTriples(t, c, "?s ?p ?o"))
		})
	}
}

func TestLoaderThreads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level string
		cpus  int
		want  int
	}{
		{"LOW", 16, 2}, {"LOW", 4, 1}, {"MEDIUM", 8, 4}, {"MEDIUM", 1, 1}, {"HIGH", 8, 8}, {"", 8, 8},
		{"OVERSUBSCRIBE", 8, 16}, {"low", 16, 2},
	}

	for _, tt := range tests {
		t.Run(tt.level+"_"+strings.Repeat("c", tt.cpus%5), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, neptune.LoaderThreads(tt.level, tt.cpus))
		})
	}
}

func TestBulkLoaderJobHistoryPersistence(t *testing.T) {
	t.Parallel()

	s3b := s3backend.NewInMemoryBackend(nil)
	_, err := s3b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("lake")})
	require.NoError(t, err)

	for k, v := range map[string]string{"d/a.nt": "<urn:a> <urn:p> <urn:o> .\n", "d/b.nt": "<urn:b> <urn:p> broken\n"} {
		_, err = s3b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
			Bucket: aws.String("lake"), Key: aws.String(k), Body: strings.NewReader(v),
		})
		require.NoError(t, err)
	}

	b, c := newEmbeddedBackend(t)
	b.SetS3Backend(s3b)

	_, env, raw := loaderStart(t, c, `{"source":"s3://lake/d/","format":"ntriples"}`)
	id, _ := env.Payload["loadId"].(string)
	require.NotEmpty(t, id, raw)

	before := loaderWait(t, c, id, "?errors=true&details=true")
	require.Equal(t, "LOAD_FAILED", overall(before)["status"])

	b2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b2.EnableEngine(neptune.EngineConfig{InProcessGremlin: true, OpenCypher: true})
	t.Cleanup(b2.Close)
	b2.SetS3Backend(s3b)
	require.NoError(t, b2.Restore(t.Context(), b.Snapshot(t.Context())))

	c2 := waitCluster(t, b2, "c1", "available")
	after := loaderWait(t, c2, id, "?errors=true&details=true")
	assert.Equal(t, overall(before), overall(after))
	assert.Equal(t, before.Payload["failedFeeds"], after.Payload["failedFeeds"])
	assert.Equal(t, before.Payload["errors"], after.Payload["errors"])

	list := sparqlDo(t, c2, http.MethodGet, "/loader", "", "", "")
	assert.Contains(t, list.body, id)

	_, resumed, rraw := loaderStart(
		t,
		c2,
		`{"source":"s3://lake/d/","format":"ntriples","mode":"RESUME","failOnError":"FALSE"}`,
	)
	rid, _ := resumed.Payload["loadId"].(string)
	require.NotEmpty(t, rid, rraw)
	assert.InDelta(t, 2, overall(loaderWait(t, c2, rid, ""))["runNumber"], 0)
}
