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
	b.finishDeploy(j, err)
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
	b.mu.RUnlock()

	rt, err := lookupRuntime(bc.GetRuntime())
	if err != nil {
		return err
	}

	if err = checkArchive(data, rt); err != nil {
		return err
	}

	if et == nil {
		return nil
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

	var old box

	if deployErr != nil {
		rec.failure = status.Convert(deployErr).Message()
		rec.pb.State = functionspb.Function_FAILED
		rec.pb.StateMessages = []*functionspb.StateMessage{{
			Severity: functionspb.StateMessage_ERROR, Type: "DeploymentFailed", Message: rec.failure,
		}}
	} else {
		rec.failure, rec.startErr = "", ""
		rec.pb.State, rec.pb.StateMessages = functionspb.Function_ACTIVE, nil
		old = b.takeBoxLocked(rec)
	}

	view := b.viewLocked(rec)
	b.mu.Unlock()

	b.teardown(old)

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

	x, sub, bus := b.takeBoxLocked(rec), rec.subName, b.events
	delete(b.functions, j.name)
	b.mu.Unlock()

	b.teardown(x)

	if bus != nil && sub != "" {
		if err := bus.DeleteSubscription(sub); err != nil {
			logger.Load(context.Background()).Debug("gcpfunctions: trigger subscription removal failed",
				"subscription", sub, "error", err)
		}
	}

	b.finishOp(j, &emptypb.Empty{})
}
