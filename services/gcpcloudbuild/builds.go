package gcpcloudbuild

import (
	"cmp"
	"maps"
	"slices"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultTimeout  = 10 * time.Minute
	defaultQueueTTL = time.Hour
)

// buildInput is everything needed to queue a build.
type buildInput struct {
	build    *cloudbuildpb.Build
	builtin  map[string]string
	project  string
	location string
	trigger  string
}

func (b *InMemoryBackend) logsBucketDefault(project string) string {
	return "gs://" + project + "_cloudbuild"
}

func logURL(r ref) string {
	base := "https://console.cloud.google.com/cloud-build/builds"
	if r.location != globalLocation {
		base += ";region=" + r.location
	}

	return base + "/" + r.id + "?project=" + projectNumber(r.project)
}

func builtinSubstitutions(r ref) map[string]string {
	return map[string]string{
		"PROJECT_ID": r.project, "BUILD_ID": r.id, "PROJECT_NUMBER": projectNumber(r.project), "LOCATION": r.location,
	}
}

// newBuild validates, resolves and stores a build, queues it and returns its operation.
func (b *InMemoryBackend) newBuild(in buildInput) (*longrunningpb.Operation, error) {
	if in.build == nil {
		return nil, errInvalid("build is required.")
	}

	if err := validateBuild(in.build); err != nil {
		return nil, err
	}

	if err := validateSource(in.build.GetSource()); err != nil {
		return nil, err
	}

	r := ref{project: in.project, location: in.location, id: uuid.NewString()}
	orig := cloneBuild(in.build)
	pb := cloneBuild(in.build)

	if err := substituteBuild(
		pb,
		mergeSubs(builtinSubstitutions(r), in.builtin, in.build.GetSubstitutions()),
	); err != nil {
		return nil, err
	}

	b.initBuild(pb, r, in.trigger)
	holdForApproval(pb, in)

	rec := &buildRec{pb: pb, ref: r, opName: buildOpName(r), orig: orig}

	b.mu.Lock("newBuild")

	if err := b.checkBuildLocked(pb); err != nil {
		b.mu.Unlock()

		return nil, err
	}

	b.builds[r.key()] = rec
	op := b.putBuildOp(rec)
	snap := cloneBuild(pb)

	b.mu.Unlock()

	b.notify(snap)

	if snap.GetStatus() == cloudbuildpb.Build_QUEUED {
		b.launch(r)
	}

	return op, nil
}

// holdForApproval parks a trigger-started build in PENDING when its template asks for approval. v1.30's
// BuildTrigger has no approval_config, so the request is read from build.approval.config.
func holdForApproval(pb *cloudbuildpb.Build, in buildInput) {
	cfg := in.build.GetApproval().GetConfig()
	if in.trigger == "" || !cfg.GetApprovalRequired() {
		return
	}

	pb.Status = cloudbuildpb.Build_PENDING
	pb.Approval = &cloudbuildpb.BuildApproval{
		State:  cloudbuildpb.BuildApproval_PENDING,
		Config: &cloudbuildpb.ApprovalConfig{ApprovalRequired: true},
	}
}

func mergeSubs(layers ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, l := range layers {
		maps.Copy(out, l)
	}

	return out
}

func (b *InMemoryBackend) checkBuildLocked(pb *cloudbuildpb.Build) error {
	if len(b.builds) >= maxBuilds {
		return errPrecondition("Too many builds; delete state or reset the emulator.")
	}

	if pool := pb.GetOptions().GetPool().GetName(); pool != "" {
		if _, ok := b.pools[pool]; !ok {
			return errInvalid("Invalid build: worker pool %q was not found.", pool)
		}
	}

	return nil
}

func (b *InMemoryBackend) initBuild(pb *cloudbuildpb.Build, r ref, trigger string) {
	pb.Name, pb.Id, pb.ProjectId = r.name("builds"), r.id, r.project
	pb.Status, pb.StatusDetail = cloudbuildpb.Build_QUEUED, ""
	pb.CreateTime, pb.StartTime, pb.FinishTime = b.ts(), nil, nil
	pb.Results, pb.Timing, pb.FailureInfo, pb.Approval = nil, nil, nil, nil
	pb.LogUrl = logURL(r)
	pb.BuildTriggerId = trigger

	if pb.GetTimeout() == nil || pb.GetTimeout().AsDuration() == 0 {
		pb.Timeout = durationpb.New(defaultTimeout)
	}

	if pb.GetQueueTtl() == nil {
		pb.QueueTtl = durationpb.New(defaultQueueTTL)
	}

	if pb.GetLogsBucket() == "" {
		pb.LogsBucket = b.logsBucketDefault(r.project)
	}

	for _, st := range pb.GetSteps() {
		st.Status, st.Timing, st.PullTiming, st.ExitCode = cloudbuildpb.Build_STATUS_UNKNOWN, nil, nil, 0
	}
}

// CreateBuild queues a build.
func (b *InMemoryBackend) CreateBuild(
	in *cloudbuildpb.CreateBuildRequest,
) (*longrunningpb.Operation, error) {
	project, location, err := parseParent(in.GetParent(), in.GetProjectId())
	if err != nil {
		return nil, err
	}

	return b.newBuild(buildInput{build: in.GetBuild(), project: project, location: location})
}

func (b *InMemoryBackend) findBuildLocked(r ref) (*buildRec, error) {
	rec, ok := b.builds[r.key()]
	if !ok || rec.ref.location != r.location {
		return nil, errNotFound("Build %q not found.", r.name("builds"))
	}

	return rec, nil
}

// GetBuild returns a build by name or by project_id and id.
func (b *InMemoryBackend) GetBuild(in *cloudbuildpb.GetBuildRequest) (*cloudbuildpb.Build, error) {
	r, err := resolveRef(in.GetName(), in.GetProjectId(), in.GetId(), "builds")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetBuild")
	defer b.mu.RUnlock()

	rec, err := b.findBuildLocked(r)
	if err != nil {
		return nil, err
	}

	return cloneBuild(rec.pb), nil
}

// ListBuilds lists builds of a project and location, newest first.
func (b *InMemoryBackend) ListBuilds(
	in *cloudbuildpb.ListBuildsRequest,
) ([]*cloudbuildpb.Build, string, error) {
	project, location, err := parseParent(in.GetParent(), in.GetProjectId())
	if err != nil {
		return nil, "", err
	}

	if in.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	match, err := parseBuildFilter(in.GetFilter())
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListBuilds")

	var all []*cloudbuildpb.Build

	for _, rec := range b.builds {
		if rec.ref.project == project && rec.ref.location == location && match(rec.pb) {
			all = append(all, cloneBuild(rec.pb))
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(x, y *cloudbuildpb.Build) int {
		return cmp.Or(
			y.GetCreateTime().AsTime().Compare(x.GetCreateTime().AsTime()),
			cmp.Compare(x.GetId(), y.GetId()),
		)
	})

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// CancelBuild cancels a queued, pending or running build.
func (b *InMemoryBackend) CancelBuild(
	in *cloudbuildpb.CancelBuildRequest,
) (*cloudbuildpb.Build, error) {
	r, err := resolveRef(in.GetName(), in.GetProjectId(), in.GetId(), "builds")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CancelBuild")

	rec, err := b.findBuildLocked(r)
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if terminal(rec.pb.GetStatus()) {
		st := rec.pb.GetStatus()
		b.mu.Unlock()

		return nil, errPrecondition("Cannot cancel build: it already finished with status %s.", st)
	}

	b.finishLocked(rec, cloudbuildpb.Build_CANCELLED, "Build was cancelled.", nil)

	if cancel := b.cancels[rec.ref.key()]; cancel != nil {
		cancel()
	}

	snap := cloneBuild(rec.pb)

	b.mu.Unlock()

	b.notify(snap)

	return snap, nil
}

// RetryBuild queues a new build from the request that created the original.
func (b *InMemoryBackend) RetryBuild(
	in *cloudbuildpb.RetryBuildRequest,
) (*longrunningpb.Operation, error) {
	r, err := resolveRef(in.GetName(), in.GetProjectId(), in.GetId(), "builds")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("RetryBuild")

	rec, err := b.findBuildLocked(r)
	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	orig, trigger := cloneBuild(rec.orig), rec.pb.GetBuildTriggerId()

	b.mu.RUnlock()

	return b.newBuild(
		buildInput{build: orig, project: r.project, location: r.location, trigger: trigger},
	)
}

// ApproveBuild records an approval decision on a build that awaits one.
func (b *InMemoryBackend) ApproveBuild(
	in *cloudbuildpb.ApproveBuildRequest,
) (*longrunningpb.Operation, error) {
	r, err := parseName(in.GetName(), "builds")
	if err != nil {
		return nil, err
	}

	if in.GetApprovalResult() == nil ||
		in.GetApprovalResult().GetDecision() == cloudbuildpb.ApprovalResult_DECISION_UNSPECIFIED {
		return nil, errInvalid("approval_result.decision is required.")
	}

	b.mu.Lock("ApproveBuild")

	rec, err := b.findBuildLocked(r)
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if rec.pb.GetStatus() != cloudbuildpb.Build_PENDING ||
		rec.pb.GetApproval().GetState() != cloudbuildpb.BuildApproval_PENDING {
		b.mu.Unlock()

		return nil, errPrecondition("Build %q is not awaiting approval.", in.GetName())
	}

	res, _ := proto.Clone(in.GetApprovalResult()).(*cloudbuildpb.ApprovalResult)
	res.ApprovalTime = b.ts()
	rec.pb.Approval.Result = res

	approved := res.GetDecision() == cloudbuildpb.ApprovalResult_APPROVED
	if approved {
		rec.pb.Approval.State, rec.pb.Status = cloudbuildpb.BuildApproval_APPROVED, cloudbuildpb.Build_QUEUED
	} else {
		rec.pb.Approval.State = cloudbuildpb.BuildApproval_REJECTED
		b.finishLocked(rec, cloudbuildpb.Build_CANCELLED, "Build was rejected.", nil)
	}

	op := b.putBuildOp(rec)
	snap := cloneBuild(rec.pb)

	b.mu.Unlock()

	b.notify(snap)

	if approved {
		b.launch(r)
	}

	return op, nil
}
