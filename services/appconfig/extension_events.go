package appconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	actionPointOnDeploymentStart      = "ON_DEPLOYMENT_START"
	actionPointOnDeploymentStep       = "ON_DEPLOYMENT_STEP"
	actionPointOnDeploymentBaking     = "ON_DEPLOYMENT_BAKING"
	actionPointOnDeploymentComplete   = "ON_DEPLOYMENT_COMPLETE"
	actionPointOnDeploymentRolledBack = "ON_DEPLOYMENT_ROLLED_BACK"
	actionPointAtDeploymentTick       = "AT_DEPLOYMENT_TICK"

	directiveRollBack = "ROLL_BACK"
	sourceAppConfig   = "aws.appconfig"
	arnMinParts       = 6
	arnRegionIdx      = 3
	arnServiceIdx     = 2

	keyConfigurationVer = "ConfigurationVersion"
	keyType             = "Type"
	keyApplication      = "Application"
	keyConfigProfile    = "ConfigurationProfile"
	keyDescription      = "Description"
)

var eventTypeWords = regexp.MustCompile(`[A-Z][a-z]+`)

var actionPointEventTypes = map[string]string{ //nolint:gochecknoglobals // static lookup table
	actionPointOnDeploymentStart:      "OnDeploymentStart",
	actionPointOnDeploymentStep:       "OnDeploymentStep",
	actionPointOnDeploymentBaking:     "OnDeploymentBaking",
	actionPointOnDeploymentComplete:   "OnDeploymentComplete",
	actionPointOnDeploymentRolledBack: "OnDeploymentRolledBack",
	actionPointAtDeploymentTick:       "AtDeploymentTick",
}

// ExtensionTargetDeliverer delivers extension action payloads to SNS topics, SQS queues and EventBridge buses.
type ExtensionTargetDeliverer interface {
	PublishSNS(topicARN, message string, attrs map[string]string) error
	SendSQS(queueARN, body string) error
	PutBusEvent(busARN, source, detailType, detail string, resources []string) error
}

// SetExtensionTargetDeliverer overrides the SNS/SQS/EventBridge delivery used for extension actions; by
// default the sibling services reachable through the app context are used.
func (b *InMemoryBackend) SetExtensionTargetDeliverer(d ExtensionTargetDeliverer) {
	b.mu.Lock("SetExtensionTargetDeliverer")
	defer b.mu.Unlock()

	b.extTargets = d
}

func (b *InMemoryBackend) extensionTargetDeliverer() ExtensionTargetDeliverer {
	b.mu.RLock("extensionTargetDeliverer")
	explicit, cfg := b.extTargets, b.appConfig
	b.mu.RUnlock()

	if explicit != nil {
		return explicit
	}

	return siblingTargets{cfg: cfg}
}

type (
	snsSibling         interface{ GetSNSHandler() service.Registerable }
	sqsSibling         interface{ GetSQSHandler() service.Registerable }
	eventBridgeSibling interface {
		GetEventBridgeHandler() service.Registerable
	}
)

type siblingTargets struct{ cfg any }

func (s siblingTargets) PublishSNS(topicARN, message string, attrs map[string]string) error {
	sib, ok := s.cfg.(snsSibling)
	if !ok {
		return nil
	}

	h, ok := sib.GetSNSHandler().(*snsbackend.Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil
	}

	ma := make(map[string]snsbackend.MessageAttribute, len(attrs))
	for k, v := range attrs {
		ma[k] = snsbackend.MessageAttribute{DataType: "String", StringValue: v}
	}

	_, err := h.Backend.Publish(topicARN, message, "", "", ma)

	return err
}

func (s siblingTargets) SendSQS(queueARN, body string) error {
	sib, ok := s.cfg.(sqsSibling)
	if !ok {
		return nil
	}

	h, ok := sib.GetSQSHandler().(*sqsbackend.Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil
	}

	parts := strings.Split(queueARN, ":")
	if len(parts) < arnMinParts {
		return fmt.Errorf("%w: invalid queue ARN %q", ErrBadRequest, queueARN)
	}

	_, err := h.Backend.SendMessage(&sqsbackend.SendMessageInput{
		QueueURL: parts[len(parts)-1], Region: parts[arnRegionIdx], MessageBody: body,
	})

	return err
}

func (s siblingTargets) PutBusEvent(
	busARN, source, detailType, detail string,
	resources []string,
) error {
	sib, ok := s.cfg.(eventBridgeSibling)
	if !ok {
		return nil
	}

	h, ok := sib.GetEventBridgeHandler().(*ebbackend.Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil
	}

	parts := strings.Split(busARN, ":")
	if len(parts) < arnMinParts {
		return fmt.Errorf("%w: invalid event bus ARN %q", ErrBadRequest, busARN)
	}

	bus := strings.TrimPrefix(parts[len(parts)-1], "event-bus/")
	ctx := awsmeta.WithRegion(context.Background(), parts[arnRegionIdx])

	res, err := h.Backend.PutEvents(ctx, []ebbackend.EventEntry{{
		Source: source, DetailType: detailType, Detail: detail, EventBusName: bus, Resources: resources,
	}})
	if err != nil {
		return err
	}

	if len(res) > 0 && res[0].ErrorCode != "" {
		return fmt.Errorf("%w: %s: %s", ErrBadRequest, res[0].ErrorCode, res[0].ErrorMessage)
	}

	return nil
}

func uriService(uri string) string {
	parts := strings.Split(uri, ":")
	if len(parts) < arnMinParts || parts[0] != "arn" {
		return ""
	}

	return parts[arnServiceIdx]
}

// extJob is one ON_* action point firing, delivered asynchronously and in order.
type extJob struct {
	event   map[string]any
	actions []preAction
}

// deploymentEventLocked builds the documented event payload for a deployment action point.
func (b *InMemoryBackend) deploymentEventLocked(d *Deployment, eventType string) map[string]any {
	envName := ""
	if env, ok := b.environments.Get(d.EnvironmentID); ok {
		envName = env.Name
	}

	var desc any
	if d.Description != "" {
		desc = d.Description
	}

	return map[string]any{
		keyType:             eventType,
		keyApplication:      b.resourceRef(d.ApplicationID, b.applicationName(d.ApplicationID)),
		"Environment":       b.resourceRef(d.EnvironmentID, envName),
		keyConfigProfile:    b.resourceRef(d.ConfigurationProfileID, d.ConfigurationName),
		"DeploymentNumber":  d.DeploymentNumber,
		keyDescription:      desc,
		keyConfigurationVer: d.ConfigurationVersion,
	}
}

// fireDeploymentActionLocked queues the ON_* actions registered for actionPoint. Must be called under lock.
func (b *InMemoryBackend) fireDeploymentActionLocked(d *Deployment, actionPoint string) {
	actions := b.actionsLocked(
		actionPoint,
		d.ApplicationID,
		d.EnvironmentID,
		d.ConfigurationProfileID,
	)
	if len(actions) == 0 {
		return
	}

	b.extQueue = append(b.extQueue, extJob{
		actions: actions,
		event:   b.deploymentEventLocked(d, actionPointEventTypes[actionPoint]),
	})

	if b.extDrainAlive {
		return
	}

	b.extDrainAlive = true

	go b.drainExtensionQueue()
}

func (b *InMemoryBackend) drainExtensionQueue() {
	for {
		b.mu.Lock("drainExtensionQueue")
		if len(b.extQueue) == 0 {
			b.extDrainAlive = false
			b.mu.Unlock()

			return
		}

		job := b.extQueue[0]
		b.extQueue = b.extQueue[1:]
		b.mu.Unlock()

		for _, act := range job.actions {
			if _, err := b.deliverAction(act, job.event); err != nil {
				ctx := context.Background()
				logger.Load(ctx).WarnContext(
					ctx, "appconfig: extension action failed", "uri", act.uri, "error", err,
				)
			}
		}
	}
}

// deliverAction sends the event to the action's target and returns a Lambda action's raw response.
func (b *InMemoryBackend) deliverAction(act preAction, event map[string]any) ([]byte, error) {
	ev := maps.Clone(event)
	ev["InvocationId"] = newInvocationID()

	params := act.params
	if params == nil {
		params = map[string]string{}
	}

	ev["Parameters"] = params

	switch uriService(act.uri) {
	case "lambda":
		invoker := b.extensionLambdaInvoker()
		if invoker == nil {
			return nil, nil
		}

		payload, err := json.Marshal(ev)
		if err != nil {
			return nil, fmt.Errorf("%w: encoding extension event: %w", ErrBadRequest, err)
		}

		return invoker.Invoke(context.Background(), act.uri, payload)
	case "sns":
		return nil, b.deliverSNS(act, ev)
	case "sqs":
		return nil, b.deliverSQS(act, ev)
	case "events":
		return nil, b.deliverEvents(act, ev)
	default:
		return nil, nil
	}
}

func stringifyDeploymentNumber(ev map[string]any) map[string]any {
	out := maps.Clone(ev)
	if n, ok := out["DeploymentNumber"].(int32); ok {
		out["DeploymentNumber"] = strconv.Itoa(int(n))
	}

	return out
}

func (b *InMemoryBackend) deliverSNS(act preAction, ev map[string]any) error {
	msg, err := json.Marshal(stringifyDeploymentNumber(ev))
	if err != nil {
		return err
	}

	eventType, _ := ev[keyType].(string)

	return b.extensionTargetDeliverer().
		PublishSNS(act.uri, string(msg), map[string]string{"MessageType": eventType})
}

func (b *InMemoryBackend) deliverSQS(act preAction, ev map[string]any) error {
	body, err := json.Marshal(stringifyDeploymentNumber(ev))
	if err != nil {
		return err
	}

	return b.extensionTargetDeliverer().SendSQS(act.uri, string(body))
}

func (b *InMemoryBackend) deliverEvents(act preAction, ev map[string]any) error {
	detail, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	eventType, _ := ev[keyType].(string)
	detailType := strings.Join(eventTypeWords.FindAllString(eventType, -1), " ")

	var resources []string
	if act.assocARN != "" {
		resources = []string{act.assocARN}
	}

	return b.extensionTargetDeliverer().
		PutBusEvent(act.uri, sourceAppConfig, detailType, string(detail), resources)
}

// tickJob is one due AT_DEPLOYMENT_TICK invocation for a deployment.
type tickJob struct {
	key     string
	event   map[string]any
	actions []preAction
}

// collectTicksLocked gathers AT_DEPLOYMENT_TICK invocations for deployments whose timer is due.
func (b *InMemoryBackend) collectTicksLocked() []tickJob {
	now := time.Now()

	var jobs []tickJob

	for key, timer := range b.deploymentTimers {
		if now.Before(timer.nextAt) {
			continue
		}

		d, ok := b.deployments.Get(key)
		if !ok || (d.State != deploymentStateDeploying && d.State != deploymentStateBaking) {
			continue
		}

		actions := b.actionsLocked(
			actionPointAtDeploymentTick,
			d.ApplicationID,
			d.EnvironmentID,
			d.ConfigurationProfileID,
		)
		if len(actions) == 0 {
			continue
		}

		ev := b.deploymentEventLocked(d, actionPointEventTypes[actionPointAtDeploymentTick])
		ev["DeploymentState"] = d.State
		ev["PercentageComplete"] = strconv.FormatFloat(float64(d.PercentageComplete), 'f', 1, 32)
		jobs = append(jobs, tickJob{key: key, event: ev, actions: actions})
	}

	return jobs
}

// runDeploymentTicks invokes AT_DEPLOYMENT_TICK actions synchronously and rolls a deployment back when an
// action errors or answers Directive ROLL_BACK.
func (b *InMemoryBackend) runDeploymentTicks() {
	b.mu.RLock("collectTicks")
	jobs := b.collectTicksLocked()
	b.mu.RUnlock()

	for _, job := range jobs {
		if reason := b.tickRollbackReason(job); reason != "" {
			b.rollBackFromTick(job.key, reason)
		}
	}
}

func (b *InMemoryBackend) tickRollbackReason(job tickJob) string {
	for _, act := range job.actions {
		raw, err := b.deliverAction(act, job.event)
		if err != nil {
			return fmt.Sprintf("Extension action %s failed: %v", act.uri, err)
		}

		if len(raw) == 0 || !json.Valid(raw) {
			continue
		}

		var resp actionResponse
		if json.Unmarshal(raw, &resp) != nil {
			continue
		}

		switch {
		case resp.Error != "":
			return fmt.Sprintf(
				"Extension action %s returned %s: %s",
				act.uri,
				resp.Error,
				resp.Message,
			)
		case resp.Directive == directiveRollBack:
			if resp.Description != "" {
				return resp.Description
			}

			return "Extension action " + act.uri + " requested a rollback"
		}
	}

	return ""
}

func (b *InMemoryBackend) rollBackFromTick(key, reason string) {
	b.mu.Lock("rollBackFromTick")
	defer b.mu.Unlock()

	d, ok := b.deployments.Get(key)
	if !ok || !stoppableDeploymentStates[d.State] {
		return
	}

	now := time.Now()
	d.State = deploymentStateRolledBack
	d.CompletedAt = now
	appendDeploymentEvent(d, "ROLLBACK_COMPLETED", "APPCONFIG", reason, now)
	delete(b.deploymentTimers, key)
	b.deployments.Put(d)
	b.fireDeploymentActionLocked(d, actionPointOnDeploymentRolledBack)
}
