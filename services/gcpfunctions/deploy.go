package gcpfunctions

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const eventsPrefix = "/_gcf/events/"

var errFunctionGone = errors.New("function was deleted")

// run executes a background job and resolves its operation.
func (b *InMemoryBackend) run(j job) {
	if j.kind == opDelete {
		b.runDelete(j)

		return
	}

	err := b.prepare(j)
	if err == nil {
		err = b.warm(j.name)
	}

	b.finishDeploy(j, err)
}

// warm replaces the instance pool with one for the new revision and brings it to its minimum instances (at least
// one) so a bad container fails the deploy. Without a container runtime the function is still deployed;
// invocations then return 503.
func (b *InMemoryBackend) warm(name string) error {
	b.mu.Lock("warm")

	rec, ok := b.functions[name]
	if !ok {
		b.mu.Unlock()

		return errFunctionGone
	}

	old := b.takePoolLocked(rec)
	b.mu.Unlock()

	if old != nil {
		old.Close()
	}

	pool, err := b.poolFor(name)
	if err != nil {
		return errFunctionGone
	}

	want := 1

	b.mu.RLock("warm.min")
	if r := b.functions[name]; r != nil {
		want = max(int(r.pb.GetServiceConfig().GetMinInstanceCount()), 1)
	}
	b.mu.RUnlock()

	err = pool.Ensure(b.baseCtx(), want)
	if errors.Is(err, errNoRuntime) || errors.Is(err, gcpscale.ErrClosed) {
		return nil
	}

	return err
}

// prepare validates the archive and wires the event trigger for the job's function.
func (b *InMemoryBackend) prepare(j job) error {
	b.mu.RLock("prepare")
	rec, ok := b.functions[j.name]

	if !ok {
		b.mu.RUnlock()

		return errFunctionGone
	}

	data, bc, et, oldSub := rec.zip, rec.pb.GetBuildConfig(), clone(rec.pb.GetEventTrigger()), rec.subName
	fn := clone(rec.pb)
	b.mu.RUnlock()

	rt, err := lookupRuntime(bc.GetRuntime())
	if err != nil {
		return err
	}

	if err = checkArchive(data, rt); err != nil {
		return err
	}

	if err = b.checkSecrets(fn); err != nil {
		return err
	}

	if et.GetEventType() != EventTypePubSub {
		return b.dropSubscription(j.name, oldSub)
	}

	sub, err := b.ensureTrigger(j.name, et, oldSub)
	if err != nil {
		return err
	}

	b.mu.Lock("prepare.sub")
	if cur := b.functions[j.name]; cur != nil {
		cur.subName = sub
	}
	b.mu.Unlock()

	return nil
}

func subscriptionName(n resName, topic string) string {
	return fmt.Sprintf("projects/%s/subscriptions/gcf-%s-%s-%s", n.project, n.id, n.location, lastSegment(topic))
}

// ensureTrigger (re)creates the push subscription that delivers topic messages to the function.
func (b *InMemoryBackend) ensureTrigger(name string, et *functionspb.EventTrigger, oldSub string) (string, error) {
	n, err := parseName(name)
	if err != nil {
		return "", err
	}

	base, err := b.ingressBase()
	if err != nil {
		return "", err
	}

	b.mu.RLock("ensureTrigger")
	bus := b.events
	b.mu.RUnlock()

	if bus == nil {
		return "", errPrecondition("Event triggers need the Pub/Sub emulator, which is not available.")
	}

	if oldSub != "" {
		_ = bus.DeleteSubscription(oldSub)
	}

	sub := subscriptionName(n, et.GetPubsubTopic())
	_ = bus.DeleteSubscription(sub)

	_, err = bus.CreateSubscription(&pubsubpb.Subscription{
		Name:  sub,
		Topic: et.GetPubsubTopic(),
		PushConfig: &pubsubpb.PushConfig{
			PushEndpoint: base + eventsPrefix + name,
		},
	})
	if err != nil {
		return "", errPrecondition("Could not subscribe to %q: %s", et.GetPubsubTopic(), status.Convert(err).Message())
	}

	return sub, nil
}

// finishDeploy records the outcome of a create or update job.
func (b *InMemoryBackend) finishDeploy(j job, deployErr error) {
	b.mu.Lock("finishDeploy")

	rec, ok := b.functions[j.name]
	if !ok {
		b.mu.Unlock()

		return
	}

	rec.pending = 0
	rec.pb.UpdateTime = b.ts()

	if deployErr != nil {
		rec.failure = status.Convert(deployErr).Message()
		rec.pb.State = functionspb.Function_FAILED
		rec.pb.StateMessages = []*functionspb.StateMessage{{
			Severity: functionspb.StateMessage_ERROR, Type: "DeploymentFailed", Message: rec.failure,
		}}
	} else {
		rec.failure = ""
		rec.pb.State, rec.pb.StateMessages = functionspb.Function_ACTIVE, nil
	}

	if rec.deployed != nil {
		close(rec.deployed)
		rec.deployed = nil
	}

	view := b.viewLocked(rec)
	b.mu.Unlock()

	if deployErr != nil {
		b.failOp(j, status.New(codes.FailedPrecondition, status.Convert(deployErr).Message()))

		return
	}

	b.finishOp(j, view)
}

// runDelete stops the function and removes it with its trigger subscription.
func (b *InMemoryBackend) runDelete(j job) {
	b.mu.Lock("runDelete")

	rec, ok := b.functions[j.name]
	if !ok {
		b.mu.Unlock()

		return
	}

	pool, sub, bus := b.takePoolLocked(rec), rec.subName, b.events
	if rec.deployed != nil {
		close(rec.deployed)
	}

	delete(b.functions, j.name)
	b.mu.Unlock()

	if pool != nil {
		pool.Close()
	}

	if bus != nil && sub != "" {
		if err := bus.DeleteSubscription(sub); err != nil {
			logger.Load(context.Background()).Debug("gcpfunctions: trigger subscription removal failed",
				"subscription", sub, "error", err)
		}
	}

	b.finishOp(j, &emptypb.Empty{})
}

// dropSubscription removes a push subscription left over from a previous Pub/Sub trigger.
func (b *InMemoryBackend) dropSubscription(name, sub string) error {
	if sub == "" {
		return nil
	}

	b.mu.RLock("dropSubscription")
	bus := b.events
	b.mu.RUnlock()

	if bus != nil {
		_ = bus.DeleteSubscription(sub)
	}

	b.mu.Lock("dropSubscription.clear")
	if cur := b.functions[name]; cur != nil {
		cur.subName = ""
	}
	b.mu.Unlock()

	return nil
}
