package gcpcloudbuild

import (
	"context"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// update applies fn to a running build and refreshes its operation; it does nothing once the build is terminal.
func (b *InMemoryBackend) update(r ref, fn func(*cloudbuildpb.Build)) {
	b.mu.Lock("update")
	defer b.mu.Unlock()

	rec, ok := b.builds[r.key()]
	if !ok || terminal(rec.pb.GetStatus()) {
		return
	}

	fn(rec.pb)
	b.putBuildOp(rec)
}

// finishLocked moves a build to a terminal status unless it already is terminal. Callers hold b.mu.
func (b *InMemoryBackend) finishLocked(
	rec *buildRec, st cloudbuildpb.Build_Status, detail string, fi *cloudbuildpb.Build_FailureInfo,
) {
	if terminal(rec.pb.GetStatus()) {
		return
	}

	now := b.ts()
	rec.pb.Status, rec.pb.StatusDetail, rec.pb.FailureInfo, rec.pb.FinishTime = st, detail, fi, now

	if rec.pb.GetStartTime() != nil {
		if rec.pb.Timing == nil {
			rec.pb.Timing = map[string]*cloudbuildpb.TimeSpan{}
		}

		rec.pb.Timing["BUILD"] = &cloudbuildpb.TimeSpan{
			StartTime: rec.pb.GetStartTime(),
			EndTime:   now,
		}
	}

	b.putBuildOp(rec)
}

// finish ends a build and publishes the new status.
func (b *InMemoryBackend) finish(
	r ref, st cloudbuildpb.Build_Status, detail string, fi *cloudbuildpb.Build_FailureInfo,
) {
	b.mu.Lock("finish")

	rec, ok := b.builds[r.key()]
	if !ok || terminal(rec.pb.GetStatus()) {
		b.mu.Unlock()

		return
	}

	b.finishLocked(rec, st, detail, fi)

	snap := cloneBuild(rec.pb)

	b.mu.Unlock()

	b.notify(snap)
}

// begin moves a queued build to WORKING and returns a snapshot to run.
func (b *InMemoryBackend) begin(r ref) (*cloudbuildpb.Build, bool) {
	b.mu.Lock("begin")

	rec, ok := b.builds[r.key()]
	if !ok || rec.pb.GetStatus() != cloudbuildpb.Build_QUEUED {
		b.mu.Unlock()

		return nil, false
	}

	rec.pb.Status, rec.pb.StartTime = cloudbuildpb.Build_WORKING, b.ts()
	b.putBuildOp(rec)

	snap := cloneBuild(rec.pb)

	b.mu.Unlock()

	b.notify(snap)

	return snap, true
}

func (b *InMemoryBackend) services() (Runtime, Services, string) {
	b.mu.RLock("services")
	defer b.mu.RUnlock()

	return b.servicesLocked()
}

func (b *InMemoryBackend) servicesLocked() (Runtime, Services, string) {
	return b.rt, b.svc, b.noEngine
}

// notify publishes the build to the cloud-builds Pub/Sub topic, as Cloud Build does on every status change.
func (b *InMemoryBackend) notify(pb *cloudbuildpb.Build) {
	_, svc, _ := b.services()
	if svc.Pub == nil {
		return
	}

	data, err := protojson.Marshal(pb)
	if err != nil {
		return
	}

	attrs := map[string]string{"buildId": pb.GetId(), "status": pb.GetStatus().String()}

	if err = svc.Pub.Publish(pb.GetProjectId(), buildsTopic, data, attrs); err != nil {
		logger.Load(b.ctx).WarnContext(b.ctx, "gcpcloudbuild: publish build status failed",
			"build", pb.GetId(), "error", err)
	}
}

// launch starts the goroutine that executes build r.
func (b *InMemoryBackend) launch(r ref) {
	b.mu.Lock("launch")

	ctx, cancel := context.WithCancel(b.ctx)
	b.cancels[r.key()] = cancel

	if b.running == 0 {
		b.idle = make(chan struct{})
	}

	b.running++

	b.mu.Unlock()

	go func() {
		defer func() {
			cancel()
			b.mu.Lock("launch.done")
			delete(b.cancels, r.key())

			if b.running--; b.running == 0 {
				close(b.idle)
				b.idle = nil
			}

			b.mu.Unlock()
		}()

		b.execute(ctx, r)
	}()
}
