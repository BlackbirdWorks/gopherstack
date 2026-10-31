package gcprun

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	defaultContainerPort = 8080
	defaultConcurrency   = 80
	defaultRevTimeout    = 300 * time.Second
	maxContainers        = 10
	maxPort              = 65535
	teardownTimeout      = 15 * time.Second
)

var errStartFailed = errors.New("revision failed to start")

func reservedEnv(name string) bool {
	switch name {
	case "PORT", "K_SERVICE", "K_REVISION", "K_CONFIGURATION":
		return true
	}

	return false
}

func validateContainers(cs []*runpb.Container, allowPorts bool) error {
	if len(cs) == 0 || len(cs) > maxContainers {
		return errInvalid("A revision needs between 1 and %d containers.", maxContainers)
	}

	withPorts := 0

	for _, c := range cs {
		if c.GetImage() == "" {
			return errInvalid("Container image must be set.")
		}

		for _, p := range c.GetPorts() {
			if p.GetContainerPort() < 1 || p.GetContainerPort() > maxPort {
				return errInvalid("Container port %d is out of range.", p.GetContainerPort())
			}
		}

		if len(c.GetPorts()) > 0 {
			withPorts++
		}

		if err := validateEnv(c.GetEnv()); err != nil {
			return err
		}
	}

	if withPorts > 1 || (!allowPorts && withPorts > 0) {
		return errInvalid("Only one container may declare ports.")
	}

	return nil
}

func validateEnv(env []*runpb.EnvVar) error {
	for _, e := range env {
		if e.GetName() == "" {
			return errInvalid("Environment variable names must not be empty.")
		}

		if reservedEnv(e.GetName()) {
			return errInvalid("The following reserved env names were provided: %s.", e.GetName())
		}
	}

	return nil
}

func validateTemplate(t *runpb.RevisionTemplate) error {
	if t == nil {
		return errInvalid("template is required.")
	}

	if err := validateContainers(t.GetContainers(), true); err != nil {
		return err
	}

	if d := t.GetTimeout(); d != nil && (d.AsDuration() < time.Second || d.AsDuration() > time.Hour) {
		return errInvalid("Revision timeout must be between 1s and 3600s.")
	}

	if t.GetMaxInstanceRequestConcurrency() < 0 {
		return errInvalid("max_instance_request_concurrency must not be negative.")
	}

	return validateRuntimeShape(t)
}

// newRevisionLocked builds the next revision of svc from tmpl.
func newRevisionLocked(
	svc *svcRec, n resName, tmpl *runpb.RevisionTemplate, now *timestamppb.Timestamp,
) (*revRec, error) {
	svcID := n.ids[0]
	svc.revSeq++

	short := tmpl.GetRevision()
	if short == "" {
		short = fmt.Sprintf("%s-%05d-%s", svcID, svc.revSeq, shortHash(svc.pb.GetUid(), strconv.Itoa(svc.revSeq))[:3])
	} else if !strings.HasPrefix(short, svcID+"-") {
		return nil, errInvalid("Revision name %q must be prefixed with the service name %q.", short, svcID)
	}

	uid := newUID()
	pb := &runpb.Revision{
		Name:                          n.parent() + "/services/" + svcID + "/revisions/" + short,
		Uid:                           uid,
		Generation:                    1,
		Labels:                        tmpl.GetLabels(),
		Annotations:                   tmpl.GetAnnotations(),
		CreateTime:                    now,
		UpdateTime:                    now,
		LaunchStage:                   svc.pb.GetLaunchStage(),
		Service:                       svc.pb.GetName(),
		Scaling:                       tmpl.GetScaling(),
		VpcAccess:                     tmpl.GetVpcAccess(),
		MaxInstanceRequestConcurrency: tmpl.GetMaxInstanceRequestConcurrency(),
		Timeout:                       tmpl.GetTimeout(),
		ServiceAccount:                tmpl.GetServiceAccount(),
		Containers:                    tmpl.GetContainers(),
		Volumes:                       tmpl.GetVolumes(),
		ExecutionEnvironment:          tmpl.GetExecutionEnvironment(),
		EncryptionKey:                 tmpl.GetEncryptionKey(),
		SessionAffinity:               tmpl.GetSessionAffinity(),
		Reconciling:                   true,
		ObservedGeneration:            1,
		Conditions: []*runpb.Condition{
			newCondition(condReady, runpb.Condition_CONDITION_RECONCILING, "Revision is deploying.", now),
		},
	}

	if pb.GetMaxInstanceRequestConcurrency() == 0 {
		pb.MaxInstanceRequestConcurrency = defaultConcurrency
	}

	if pb.GetTimeout() == nil {
		pb.Timeout = durationpb.New(defaultRevTimeout)
	}

	pb.Etag = etag(uid, 1, now)

	return &revRec{pb: pb, seq: svc.revSeq}, nil
}

func ingressContainer(cs []*runpb.Container) (*runpb.Container, int) {
	for _, c := range cs {
		if len(c.GetPorts()) > 0 {
			return c, int(c.GetPorts()[0].GetContainerPort())
		}
	}

	return cs[0], defaultContainerPort
}

func (b *InMemoryBackend) baseCtx() context.Context {
	b.mu.RLock("baseCtx")
	defer b.mu.RUnlock()

	return b.runCtx
}

func (b *InMemoryBackend) stopContainer(runner Runner, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(b.baseCtx()), teardownTimeout)
	defer cancel()

	if err := runner.StopAndRemove(ctx, id); err != nil {
		logger.Load(ctx).WarnContext(ctx, "gcprun: container removal failed", "container", id, "error", err)
	}
}

// teardown retires every instance of a revision that is no longer reachable through the backend maps.
func (b *InMemoryBackend) teardown(r *revRec) {
	b.mu.RLock("teardown")
	pool := r.pool
	b.mu.RUnlock()

	if pool != nil {
		pool.Close()
	}
}
