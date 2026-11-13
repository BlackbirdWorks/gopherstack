package ocivault

import (
	"context"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iso8601"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	rotNotEnabled = "NOT_ENABLED"
	rotScheduled  = "SCHEDULED"
	rotInProgress = "IN_PROGRESS"
	rotCancelling = "CANCELLING"

	targetADB      = "ADB"
	targetFunction = "FUNCTION"

	stepVerify  = "VERIFY_CONNECTION"
	stepCreate  = "CREATE_PENDING_VERSION"
	stepUpdate  = "UPDATE_TARGET_SYSTEM"
	stepPromote = "PROMOTE_PENDING_VERSION"

	minRotation = day
	maxRotation = 360 * day
)

// FunctionInvoker calls an OCI function in-process with the given request body.
type FunctionInvoker func(ctx context.Context, functionID string, body []byte) error

// SetFunctionInvoker wires OCI Functions for FUNCTION rotation targets.
func (b *InMemoryBackend) SetFunctionInvoker(fn FunctionInvoker) {
	b.mu.Lock("SetFunctionInvoker")
	b.invoke = fn
	b.mu.Unlock()
}

// TargetSystemDetails is the polymorphic targetSystemDetails body.
type TargetSystemDetails struct {
	TargetSystemType string `json:"targetSystemType"`
	AdbID            string `json:"adbId,omitempty"`
	FunctionID       string `json:"functionId,omitempty"`
}

// RotationConfig is the secret rotationConfig body.
type RotationConfig struct {
	TargetSystemDetails        TargetSystemDetails `json:"targetSystemDetails"`
	RotationInterval           string              `json:"rotationInterval,omitempty"`
	IsScheduledRotationEnabled bool                `json:"isScheduledRotationEnabled"`
}

// SecretRotation is a secret's stored rotation state.
type SecretRotation struct {
	LastRotation *time.Time `json:"lastRotation,omitempty"`
	NextRotation *time.Time `json:"nextRotation,omitempty"`
	transition
	Status string         `json:"status"`
	Config RotationConfig `json:"config"`
}

func parseRotation(raw jsontext.Value) (*RotationConfig, *APIError) {
	if len(raw) == 0 || string(raw) == jsonNull {
		return nil, nil
	}

	var c RotationConfig
	if err := json.Unmarshal(raw, &c, jsonx.V1()); err != nil {
		return nil, errParam("rotationConfig is not valid")
	}

	t := c.TargetSystemDetails

	switch {
	case t.TargetSystemType == targetADB && t.AdbID != "":
	case t.TargetSystemType == targetFunction && t.FunctionID != "":
	default:
		return nil, errParam(
			"rotationConfig.targetSystemDetails needs targetSystemType ADB with adbId or FUNCTION with functionId",
		)
	}

	if c.RotationInterval != "" {
		d, err := iso8601.Parse(c.RotationInterval)
		if err != nil || d < minRotation || d > maxRotation {
			return nil, errParam("rotationConfig.rotationInterval must be an ISO 8601 duration of 1 to 360 days")
		}
	}

	if c.IsScheduledRotationEnabled && c.RotationInterval == "" {
		return nil, errParam("rotationConfig.rotationInterval is required when isScheduledRotationEnabled is true")
	}

	return &c, nil
}

func (b *InMemoryBackend) setRotation(s *Secret, c *RotationConfig) {
	if c == nil {
		return
	}

	r := &SecretRotation{Config: *c, Status: rotNotEnabled}
	if s.Rotation != nil {
		r.LastRotation = s.Rotation.LastRotation
	}

	if c.IsScheduledRotationEnabled {
		d, _ := iso8601.Parse(c.RotationInterval)
		next := b.nowUTC().Add(d)
		r.NextRotation, r.Status = &next, rotScheduled
	}

	s.Rotation = r
}

func (b *InMemoryBackend) setGeneration(s *Secret, g *GenerationContext, auto *bool) *APIError {
	if g != nil {
		s.GenContext = g
	}

	if auto != nil {
		s.AutoGen = *auto
	}

	if s.AutoGen && s.GenContext == nil {
		return errParam("enableAutoGeneration requires secretGenerationContext")
	}

	return nil
}

func generatedContent(g *GenerationContext) (*SecretContent, *APIError) {
	raw, err := g.generate()
	if err != nil {
		return nil, newErr(http.StatusInternalServerError, "InternalServerError", "secret generation failed")
	}

	return &SecretContent{ContentType: contentBase64, Content: base64.StdEncoding.EncodeToString(raw)}, nil
}

// RotateSecret forces a rotation now; the version stages move PENDING to CURRENT when it succeeds.
func (b *InMemoryBackend) RotateSecret(id, ifMatch, token string) error {
	b.mu.Lock("RotateSecret")

	scope := "RotateSecret/" + id
	if _, ok := b.retrySeen(scope, token); ok {
		b.mu.Unlock()

		return nil
	}

	s, aerr := b.writableSecret(id, ifMatch)
	if aerr == nil && s.Rotation == nil {
		aerr = errParam("The secret has no rotationConfig")
	}

	if aerr == nil && s.Rotation.Status == rotInProgress {
		aerr = errState("A rotation of secret " + id + " is already in progress")
	}

	if aerr == nil && s.Rotation.Config.TargetSystemDetails.TargetSystemType == targetFunction && b.invoke == nil {
		aerr = errParam("OCI Functions is not available to run the rotation function")
	}

	if aerr == nil {
		b.retryNote(scope, token, "")
	}

	b.mu.Unlock()

	if aerr != nil {
		return aerr
	}

	b.rotate(id)

	return nil
}

// CancelSecretRotation disables scheduled rotation for the secret.
func (b *InMemoryBackend) CancelSecretRotation(id, ifMatch string) error {
	b.mu.Lock("CancelSecretRotation")
	defer b.mu.Unlock()

	s, aerr := b.writableSecret(id, ifMatch)
	if aerr != nil {
		return aerr
	}

	if s.Rotation == nil || s.Rotation.Status != rotScheduled {
		return errState("Secret " + id + " has no scheduled rotation to cancel")
	}

	s.Rotation.Config.IsScheduledRotationEnabled, s.Rotation.NextRotation = false, nil
	b.begin(&s.Rotation.Status, &s.Rotation.transition, rotCancelling, rotNotEnabled)
	s.Etag = newEtag()

	return nil
}

type rotationRun struct {
	invoke  FunctionInvoker
	target  TargetSystemDetails
	current int64
}

func (b *InMemoryBackend) startRotation(id string) (rotationRun, bool) {
	b.mu.Lock("rotate")
	defer b.mu.Unlock()

	s, ok := b.secrets[id]
	if !ok || s.State != stateActive || s.Rotation == nil || s.Rotation.Status == rotInProgress {
		return rotationRun{}, false
	}

	s.Rotation.Status, s.Rotation.SettleTo, s.Rotation.SettleAt = rotInProgress, "", nil

	return rotationRun{target: s.Rotation.Config.TargetSystemDetails, invoke: b.invoke, current: s.Current}, true
}

func (b *InMemoryBackend) rotate(id string) {
	run, ok := b.startRotation(id)
	if !ok {
		return
	}

	ctx := context.Background()
	err := b.runSteps(ctx, id, run)

	b.mu.Lock("rotate")
	defer b.mu.Unlock()

	s, ok := b.secrets[id]
	if !ok || s.Rotation == nil {
		return
	}

	now := b.nowUTC()
	r := s.Rotation

	r.Status = rotNotEnabled

	if r.Config.IsScheduledRotationEnabled {
		d, _ := iso8601.Parse(r.Config.RotationInterval)
		next := now.Add(d)
		r.NextRotation, r.Status = &next, rotScheduled
	}

	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ocivault: secret rotation failed", "secretId", id, "error", err.Error())

		return
	}

	r.LastRotation = &now
	s.Etag = newEtag()
}

func (b *InMemoryBackend) runSteps(ctx context.Context, id string, run rotationRun) error {
	isFn := run.target.TargetSystemType == targetFunction

	call := func(step string) error {
		if !isFn {
			return nil
		}

		body, _ := json.Marshal(map[string]any{
			"secretId": id, "step": step, "versionNo": run.current,
		}, jsonx.V1())

		return run.invoke(ctx, run.target.FunctionID, body)
	}

	if err := call(stepVerify); err != nil {
		return err
	}

	if err := call(stepCreate); err != nil {
		return err
	}

	if err := b.ensurePending(id); err != nil {
		return err
	}

	if err := call(stepUpdate); err != nil {
		return err
	}

	if err := call(stepPromote); err != nil {
		return err
	}

	return b.promotePending(id)
}

// ErrFunctionFailed marks a rotation function call that did not succeed.
var ErrFunctionFailed = errors.New("ocivault: rotation function failed")

type rotationError string

func (e rotationError) Error() string { return string(e) }

func (b *InMemoryBackend) ensurePending(id string) error {
	b.mu.RLock("ensurePending")
	s, ok := b.secrets[id]

	var gen *GenerationContext

	if ok {
		if s.Pending != 0 {
			b.mu.RUnlock()

			return nil
		}

		if s.AutoGen {
			gen = s.GenContext
		}
	}

	b.mu.RUnlock()

	if !ok || gen == nil {
		return rotationError("no pending version exists and the secret has no generation context to create one")
	}

	c, aerr := generatedContent(gen)
	if aerr != nil {
		return aerr
	}

	c.Stage = stagePending

	b.mu.Lock("ensurePending")
	defer b.mu.Unlock()

	if s, ok = b.secrets[id]; !ok {
		return rotationError("secret disappeared during rotation")
	}

	if s.Pending != 0 {
		return nil
	}

	if aerr = b.addSecretVersion(s, c); aerr != nil {
		return aerr
	}

	s.Etag = newEtag()

	return nil
}

func (b *InMemoryBackend) promotePending(id string) error {
	b.mu.Lock("promotePending")
	defer b.mu.Unlock()

	s, ok := b.secrets[id]
	if !ok {
		return rotationError("secret disappeared during rotation")
	}

	if s.Pending != 0 {
		promote(s, s.Pending)
	}

	return nil
}

// dueRotations lists active secrets whose scheduled rotation time has passed.
func (b *InMemoryBackend) dueRotations(now time.Time) []string {
	var ids []string

	for id, s := range b.secrets {
		r := s.Rotation
		if s.State == stateActive && r != nil && r.Status == rotScheduled && due(r.NextRotation, now) {
			ids = append(ids, id)
		}
	}

	return ids
}
