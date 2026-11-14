package gcpeventarc

import (
	"time"

	"cel.dev/cel-go/cel"
	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

const (
	pipelineMinAttempts = 1
	pipelineMaxAttempts = 100
	pipelineMinDelay    = time.Second
	pipelineMaxDelay    = 600 * time.Second
	pipelineDefaultTry  = 5
	pipelineDefaultMin  = 5 * time.Second
	pipelineDefaultMax  = 60 * time.Second
)

func (b *InMemoryBackend) validateAdvanced(m proto.Message) error {
	if key := getString(m, "crypto_key_name"); key != "" {
		if err := b.checkKey(key, cmek.Location(getString(m, "name"))); err != nil {
			return err
		}
	}

	switch r := m.(type) {
	case *eventarcpb.Enrollment:
		return b.validateEnrollmentLocked(r)
	case *eventarcpb.Pipeline:
		return b.validatePipelineLocked(r)
	case *eventarcpb.GoogleApiSource:
		return b.validateSourceLocked(r)
	}

	return nil
}

func (b *InMemoryBackend) validateEnrollmentLocked(e *eventarcpb.Enrollment) error {
	if _, _, _, err := parseChild(e.GetMessageBus(), kindMessageBuses); err != nil {
		return errInvalid("message_bus must be projects/{project}/locations/{location}/messageBuses/{id}.")
	}

	if _, ok := b.advTable(busKind())[e.GetMessageBus()]; !ok {
		return errPrecondition("Message bus %q does not exist.", e.GetMessageBus())
	}

	if _, _, _, err := parseChild(e.GetDestination(), kindPipelines); err != nil {
		return errInvalid("destination must be projects/{project}/locations/{location}/pipelines/{id}.")
	}

	if _, ok := b.advTable(pipelineKind())[e.GetDestination()]; !ok {
		return errPrecondition("Pipeline %q does not exist.", e.GetDestination())
	}

	if e.GetCelMatch() == "" {
		return errInvalid("cel_match is required.")
	}

	if b.msgEnv == nil {
		return errPrecondition("The CEL environment is unavailable.")
	}

	if _, err := b.compileMessageExpr(e.GetCelMatch(), cel.BoolType); err != nil {
		return errInvalid("cel_match is not a valid CEL expression returning a bool: %v", err)
	}

	return nil
}

func (b *InMemoryBackend) validatePipelineLocked(p *eventarcpb.Pipeline) error {
	if len(p.GetDestinations()) != 1 {
		return errInvalid("destinations must contain exactly one destination.")
	}

	if err := b.validatePipelineDestination(p.GetDestinations()[0]); err != nil {
		return err
	}

	if len(p.GetMediations()) > 1 {
		return errInvalid("At most one mediation is supported.")
	}

	for _, m := range p.GetMediations() {
		t := m.GetTransformation()
		if t == nil || t.GetTransformationTemplate() == "" {
			return errInvalid("mediations.transformation.transformation_template is required.")
		}

		if _, err := b.compileMessageExpr(t.GetTransformationTemplate(), nil); err != nil {
			return errInvalid("transformation_template is not a valid CEL expression: %v", err)
		}
	}

	return validatePipelineRetry(p.GetRetryPolicy())
}

func (b *InMemoryBackend) validatePipelineDestination(d *eventarcpb.Pipeline_Destination) error {
	switch dd := d.GetDestinationDescriptor().(type) {
	case *eventarcpb.Pipeline_Destination_HttpEndpoint_:
		if err := validateHTTPEndpoint(dd.HttpEndpoint.GetUri()); err != nil {
			return err
		}

		if t := dd.HttpEndpoint.GetMessageBindingTemplate(); t != "" {
			if _, err := b.compileMessageExpr(t, nil); err != nil {
				return errInvalid("message_binding_template is not a valid CEL expression: %v", err)
			}
		}
	case *eventarcpb.Pipeline_Destination_Workflow:
		return validateWorkflowName(dd.Workflow)
	case *eventarcpb.Pipeline_Destination_MessageBus:
		if _, ok := b.advTable(busKind())[dd.MessageBus]; !ok {
			return errPrecondition("Message bus %q does not exist.", dd.MessageBus)
		}
	case *eventarcpb.Pipeline_Destination_Topic:
		if !topicRe.MatchString(dd.Topic) {
			return errInvalid("destination.topic %q must be projects/{project}/topics/{topic}.", dd.Topic)
		}
	default:
		return errInvalid("destination must set one of http_endpoint, workflow, message_bus or topic.")
	}

	return nil
}

func validatePipelineRetry(r *eventarcpb.Pipeline_RetryPolicy) error {
	if r == nil {
		return nil
	}

	if n := r.GetMaxAttempts(); n != 0 && (n < pipelineMinAttempts || n > pipelineMaxAttempts) {
		return errInvalid(
			"retry_policy.max_attempts must be between %d and %d.",
			pipelineMinAttempts,
			pipelineMaxAttempts,
		)
	}

	for name, d := range map[string]*struct{ v time.Duration }{
		"min_retry_delay": delayOf(r.GetMinRetryDelay().AsDuration(), r.GetMinRetryDelay() != nil),
		"max_retry_delay": delayOf(r.GetMaxRetryDelay().AsDuration(), r.GetMaxRetryDelay() != nil),
	} {
		if d != nil && (d.v < pipelineMinDelay || d.v > pipelineMaxDelay) {
			return errInvalid("retry_policy.%s must be between 1 and 600 seconds.", name)
		}
	}

	if r.GetMinRetryDelay() != nil && r.GetMaxRetryDelay() != nil &&
		r.GetMinRetryDelay().AsDuration() > r.GetMaxRetryDelay().AsDuration() {
		return errInvalid("retry_policy.min_retry_delay must not exceed max_retry_delay.")
	}

	return nil
}

func delayOf(d time.Duration, set bool) *struct{ v time.Duration } {
	if !set {
		return nil
	}

	return &struct{ v time.Duration }{d}
}

func (b *InMemoryBackend) validateSourceLocked(s *eventarcpb.GoogleApiSource) error {
	if _, _, _, err := parseChild(s.GetDestination(), kindMessageBuses); err != nil {
		return errInvalid("destination must be projects/{project}/locations/{location}/messageBuses/{id}.")
	}

	if _, ok := b.advTable(busKind())[s.GetDestination()]; !ok {
		return errPrecondition("Message bus %q does not exist.", s.GetDestination())
	}

	switch s.GetWideScopeSubscription().(type) {
	case *eventarcpb.GoogleApiSource_ProjectSubscriptions_:
		if len(s.GetProjectSubscriptions().GetList()) == 0 {
			return errInvalid("project_subscriptions.list must name at least one project.")
		}
	case *eventarcpb.GoogleApiSource_OrganizationSubscription_:
	default:
		return errInvalid("One of project_subscriptions or organization_subscription is required.")
	}

	return nil
}
