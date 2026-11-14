package neptune

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

const (
	loadS3AccessDenied = "LOAD_S3_ACCESS_DENIED_ERROR"
	errCodeS3Denied    = "S3_ACCESS_DENIED_ERROR"
	loadModeNew        = "NEW"
	loadModeResume     = "RESUME"
	loadModeAuto       = "AUTO"
	sseKMS             = "aws:kms"

	lowDivisor    = 8
	mediumDivisor = 2
	oversubscribe = 2
)

var errNoPreviousLoad = errors.New("no previous load job found for this source to resume")

// objectData is a fetched feed with the SSE-KMS key that encrypted it, if any.
type objectData struct {
	kmsKey string
	data   []byte
}

type feedCache map[string]objectData

// SetRoleAuthorizer makes bulk loads run under the iamRoleArn policies when IAM enforcement is on.
func (b *InMemoryBackend) SetRoleAuthorizer(a roleauth.Authorizer) {
	b.mu.Lock("SetRoleAuthorizer")
	defer b.mu.Unlock()

	b.roleAuth = a
}

func (b *InMemoryBackend) roleAuthorizer() roleauth.Authorizer {
	b.mu.RLock("roleAuthorizer")
	defer b.mu.RUnlock()

	return b.roleAuth
}

func (b *InMemoryBackend) loaderCPUCount() int {
	b.mu.RLock("loaderCPUCount")
	defer b.mu.RUnlock()

	if b.loaderCPUs > 0 {
		return b.loaderCPUs
	}

	return runtime.NumCPU()
}

func splitRoleARNs(s string) []string {
	var out []string

	for part := range strings.SplitSeq(s, ",") {
		if r := strings.TrimSpace(part); r != "" {
			out = append(out, r)
		}
	}

	return out
}

// loaderThreads sizes the worker pool from the documented parallelism levels (load-api-reference-load.html).
func loaderThreads(level string, cpus int) int {
	cpus = max(cpus, 1)

	switch strings.ToUpper(level) {
	case "LOW":
		return max(cpus/lowDivisor, 1)
	case "MEDIUM":
		return max(cpus/mediumDivisor, 1)
	case "OVERSUBSCRIBE":
		return cpus * oversubscribe
	default:
		return cpus
	}
}

// feedFailure describes a feed that could not be read.
type feedFailure struct {
	status string
	msg    string
}

func (f *feedFailure) result(uri string) feedResult {
	return feedResult{status: f.status, errs: []loadError{{Code: errCodeS3Denied, Message: f.msg, File: uri}}}
}

// authorizeRole checks that Neptune (rds.amazonaws.com) can assume the first role and that the role at the end of
// the chain may perform action on resource.
func (rs *rdfService) authorizeRole(job *loadJob, action, resource string) *feedFailure {
	var auth roleauth.Authorizer
	if rs.roleAuth != nil {
		auth = rs.roleAuth()
	}

	if auth == nil || len(job.roles) == 0 {
		return nil
	}

	var err error

	if len(job.roles) == 1 {
		err = auth.AuthorizeRole(roleauth.PrincipalRDS, job.roles[0], action, resource)
	} else {
		err = auth.AuthorizeRole(roleauth.PrincipalRDS, job.roles[0], "sts:AssumeRole", job.roles[1])
	}

	switch {
	case err == nil:
		return nil
	case errors.Is(err, roleauth.ErrNotAssumable):
		return &feedFailure{
			status: loadS3AccessDenied,
			msg:    "Unable to assume the IAM role " + job.roles[0] + ": " + err.Error(),
		}
	default:
		return &feedFailure{status: loadS3AccessDenied, msg: err.Error()}
	}
}

// fetchFeed reads one feed under the load's role: role assumption, s3:GetObject and, for SSE-KMS objects, kms:Decrypt.
func (rs *rdfService) fetchFeed(
	ctx context.Context,
	job *loadJob,
	cache feedCache,
	key string,
) (objectData, *feedFailure) {
	if fail := rs.authorizeRole(job, "s3:GetObject", arn.BuildS3(job.bucket+"/"+key)); fail != nil {
		return objectData{}, fail
	}

	obj, ok := cache[key]
	if !ok {
		var err error
		if obj, err = rs.readObjectData(ctx, job.bucket, key); err != nil {
			return objectData{}, &feedFailure{status: loadS3ReadError, msg: err.Error()}
		}
	}

	if obj.kmsKey != "" && len(job.roles) == 1 {
		if fail := rs.authorizeRole(job, "kms:Decrypt", obj.kmsKey); fail != nil {
			return objectData{}, &feedFailure{status: loadFailed, msg: fail.msg}
		}
	}

	return obj, nil
}

func (rs *rdfService) readObjectData(ctx context.Context, bucket, key string) (objectData, error) {
	var s3 S3Reader
	if rs.s3 != nil {
		s3 = rs.s3()
	}

	if s3 == nil {
		return objectData{}, errNoS3
	}

	out, err := s3.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return objectData{}, fmt.Errorf("read s3://%s/%s: %w", bucket, key, err)
	}

	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return objectData{}, err
	}

	obj := objectData{data: data}
	if string(out.ServerSideEncryption) == sseKMS {
		obj.kmsKey = aws.ToString(out.SSEKMSKeyId)
	}

	return obj, nil
}

// previousJob returns the newest finished job loaded from source.
func (ls *loaderState) previousJob(source string) *loadJob {
	ls.mu.RLock("previousJob")
	defer ls.mu.RUnlock()

	for _, id := range slices.Backward(ls.order) {
		if j := ls.jobs[id]; j != nil && j.source == source && j.finished {
			cp, _ := ls.snapshotJob(j.id)

			return cp
		}
	}

	return nil
}

// applyMode implements NEW, RESUME and AUTO: a resumed job skips the feeds the previous run completed.
func (ls *loaderState) applyMode(job *loadJob, mode string) error {
	job.run = 1

	if mode == loadModeNew {
		return nil
	}

	prev := ls.previousJob(job.source)
	if prev == nil {
		if mode == loadModeResume {
			return errNoPreviousLoad
		}

		return nil
	}

	job.run = prev.run + 1
	done := map[string]*loadFeed{}

	for _, f := range prev.feeds {
		if f.Status == loadCompleted {
			done[f.URI] = f
		}
	}

	job.skip = map[string]bool{}

	for _, feed := range job.feeds {
		feed.Run = job.run

		if old, ok := done[feed.URI]; ok {
			*feed = *old
			job.skip[feed.URI] = true
		}
	}

	return nil
}

// runWave loads the feeds of one wave on up to workers goroutines, writing each feed's final status to results.
func (rs *rdfService) runWave(
	ctx context.Context, job *loadJob, cache feedCache, wave []int, workers int, results []string,
) {
	sem := make(chan struct{}, max(workers, 1))

	var (
		wg   sync.WaitGroup
		halt atomic.Bool
	)

	for _, i := range wave {
		if job.skip[job.feeds[i].URI] {
			results[i] = loadCompleted

			continue
		}

		if halt.Load() && job.failOnErr {
			break
		}

		sem <- struct{}{}

		wg.Go(func() {
			defer func() { <-sem }()

			st := rs.loadFeed(ctx, job, cache, job.feeds[i], job.keys[i])
			results[i] = st

			if st != loadCompleted {
				halt.Store(true)
			}
		})
	}

	wg.Wait()
}
