package codepipeline

import (
	"context"
	"maps"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	sourcePollComponent       = "SourceChangePoller"
	sourcePollInterval        = time.Minute
	configKeyPollForChanges   = "PollForSourceChanges"
	triggerTypePollForChanges = "PollForSourceChanges"
)

type polledSource struct {
	key, region, pipeline string
	action                Action
}

// pollsSource reports whether a Source/S3 or Source/CodeCommit action polls; omitting the key means true.
func pollsSource(a Action) bool {
	if a.ActionTypeID.Category != actionCategorySource ||
		!isBuiltinAction(a, actionProviderS3) && !isBuiltinAction(a, actionProviderCodeCommit) {
		return false
	}

	return !strings.EqualFold(a.Configuration[configKeyPollForChanges], "false")
}

func (b *InMemoryBackend) pollCandidatesLocked() []polledSource {
	var out []polledSource

	for _, p := range b.pipelines.All() {
		for _, st := range p.Declaration.Stages {
			for _, a := range st.Actions {
				if pollsSource(a) {
					out = append(out, polledSource{
						key: p.region + "|" + p.Declaration.Name + "|" + a.Name, region: p.region,
						pipeline: p.Declaration.Name, action: a,
					})
				}
			}
		}
	}

	return out
}

// sourceRevision returns the current revision of the source (S3 ETag/VersionId, CodeCommit commit ID).
func (b *InMemoryBackend) sourceRevision(ctx context.Context, a Action) (string, bool) {
	if isBuiltinAction(a, actionProviderCodeCommit) {
		if b.repositoryReader == nil {
			return "", false
		}

		snap, err := b.repositoryReader.Snapshot(ctx, a.Configuration["RepositoryName"], a.Configuration["BranchName"])
		if err != nil {
			return "", false
		}

		return snap.CommitID, true
	}

	if b.artifactS3 == nil {
		return "", false
	}

	out, err := b.artifactS3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.Configuration[configKeyS3Bucket]), Key: aws.String(a.Configuration["S3ObjectKey"]),
	})
	if err != nil {
		return "", false
	}

	_ = out.Body.Close()

	return aws.ToString(out.ETag) + "|" + aws.ToString(out.VersionId), true
}

// PollSourceChanges starts an execution for every polling source whose revision moved since the previous poll.
// The first poll of a source only records its revision: CreatePipeline does not start an execution here.
func (b *InMemoryBackend) PollSourceChanges(_ context.Context) {
	b.mu.RLock("PollSourceChanges")
	cands := b.pollCandidatesLocked()
	b.mu.RUnlock()

	seen := make(map[string]string, len(cands))
	changed := make(map[string]string)

	for _, c := range cands {
		rev, ok := b.sourceRevision(b.regionContext(c.region), c.action)
		if !ok {
			continue
		}

		seen[c.key] = rev

		b.mu.RLock("PollSourceChanges")
		prev, known := b.polledRevisions[c.key]
		b.mu.RUnlock()

		if known && prev != rev {
			changed[c.key] = rev
		}
	}

	b.mu.Lock("PollSourceChanges")
	live := make(map[string]struct{}, len(cands))

	for _, c := range cands {
		live[c.key] = struct{}{}
	}

	for k := range b.polledRevisions {
		if _, ok := live[k]; !ok {
			delete(b.polledRevisions, k)
		}
	}

	maps.Copy(b.polledRevisions, seen)
	b.mu.Unlock()

	started := map[string]struct{}{}

	for _, c := range cands {
		if _, ok := changed[c.key]; !ok {
			continue
		}

		if _, dup := started[c.region+"|"+c.pipeline]; dup {
			continue
		}

		started[c.region+"|"+c.pipeline] = struct{}{}

		_, _ = b.StartPipelineExecutionWith(b.regionContext(c.region), c.pipeline,
			StartExecutionOptions{TriggerType: triggerTypePollForChanges})
	}
}
