package gcprun

import (
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// softDeleteRetention is how long a deleted service, job or execution stays listable with show_deleted.
// The API documents expire_time but not the window, so the length is an emulator choice.
const softDeleteRetention = 7 * 24 * time.Hour

type tombstones struct {
	services   map[string]*runpb.Service
	jobs       map[string]*runpb.Job
	executions map[string]*runpb.Execution
}

func newTombstones() *tombstones {
	return &tombstones{
		services:   map[string]*runpb.Service{},
		jobs:       map[string]*runpb.Job{},
		executions: map[string]*runpb.Execution{},
	}
}

func tombKey(name, uid string) string { return name + "#" + uid }

// stampExpiry sets expire_time on a deleted resource's view from its delete_time.
func stampExpiry(del *timestamppb.Timestamp) *timestamppb.Timestamp {
	return timestamppb.New(del.AsTime().Add(softDeleteRetention))
}

func (b *InMemoryBackend) liveTomb(expire *timestamppb.Timestamp) bool {
	return expire.AsTime().After(b.clk().Now())
}

func (b *InMemoryBackend) entombServiceLocked(view *runpb.Service) {
	view.ExpireTime = stampExpiry(view.GetDeleteTime())
	b.sweepLocked()
	b.deleted.services[tombKey(view.GetName(), view.GetUid())] = clone(view)
}

func (b *InMemoryBackend) entombJobLocked(view *runpb.Job) {
	view.ExpireTime = stampExpiry(view.GetDeleteTime())
	b.sweepLocked()
	b.deleted.jobs[tombKey(view.GetName(), view.GetUid())] = clone(view)
}

func (b *InMemoryBackend) entombExecutionLocked(view *runpb.Execution) {
	view.ExpireTime = stampExpiry(view.GetDeleteTime())
	b.sweepLocked()
	b.deleted.executions[tombKey(view.GetName(), view.GetUid())] = clone(view)
}

// sweepLocked drops tombstones whose expire_time has passed.
func (b *InMemoryBackend) sweepLocked() {
	for k, v := range b.deleted.services {
		if !b.liveTomb(v.GetExpireTime()) {
			delete(b.deleted.services, k)
		}
	}

	for k, v := range b.deleted.jobs {
		if !b.liveTomb(v.GetExpireTime()) {
			delete(b.deleted.jobs, k)
		}
	}

	for k, v := range b.deleted.executions {
		if !b.liveTomb(v.GetExpireTime()) {
			delete(b.deleted.executions, k)
		}
	}
}

// mergeDeleted appends the unexpired tombstones under prefix to live and orders the result by name, live first.
func mergeDeleted[T interface {
	GetName() string
	GetExpireTime() *timestamppb.Timestamp
}](b *InMemoryBackend, live []T, tombs map[string]T, prefix string, cp func(T) T) []T {
	for _, t := range tombs {
		if strings.HasPrefix(t.GetName(), prefix) && b.liveTomb(t.GetExpireTime()) {
			live = append(live, cp(t))
		}
	}

	slices.SortStableFunc(live, func(x, y T) int { return strings.Compare(x.GetName(), y.GetName()) })

	return live
}
