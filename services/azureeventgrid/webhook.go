package azureeventgrid

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/google/uuid"
)

const (
	validationEventType = "Microsoft.EventGrid.SubscriptionValidationEvent"
	maxResponseBytes    = 4096
	validatePath        = "/validate"
	originEventGrid     = "eventgrid.azure.net"
)

// validateEndpoint runs the WebHook subscription validation handshake when the
// endpoint is new or changed, and returns the resulting state and manual code.
func (b *InMemoryBackend) validateEndpoint(
	ctx context.Context, topicName, topicRID, name string, spec Spec,
) (string, string, error) {
	if spec.Destination.EndpointType != EndpointWebHook {
		return StateSucceeded, "", nil
	}

	if b.unchangedEndpoint(topicName, name, spec) {
		return b.currentState(topicName, name)
	}

	code := uuid.NewString()
	validationURL := b.validationURL(code)

	var (
		echoed bool
		err    error
	)

	if spec.EventDeliverySchema == SchemaCloudEvent {
		echoed, err = b.optionsHandshake(ctx, spec.Destination.str(propEndpointURL), validationURL)
	} else {
		echoed, err = b.postHandshake(ctx, topicRID, spec.Destination.str(propEndpointURL), code, validationURL)
	}

	switch {
	case err != nil:
		return "", "", err
	case echoed:
		return StateSucceeded, "", nil
	}

	return StateAwaitingManual, code, nil
}

func (b *InMemoryBackend) unchangedEndpoint(topicName, name string, spec Spec) bool {
	b.mu.RLock("unchangedEndpoint")
	defer b.mu.RUnlock()

	_, s, err := b.subLocked(topicName, name)

	return err == nil && s.Spec.Destination.EndpointType == EndpointWebHook &&
		reflect.DeepEqual(s.Spec.Destination.Properties[propEndpointURL], spec.Destination.Properties[propEndpointURL])
}

func (b *InMemoryBackend) currentState(topicName, name string) (string, string, error) {
	b.mu.RLock("currentState")
	defer b.mu.RUnlock()

	_, s, err := b.subLocked(topicName, name)
	if err != nil {
		return "", "", err
	}

	return s.State, s.ValidationCode, nil
}

func (b *InMemoryBackend) validationURL(code string) string {
	b.mu.RLock("validationURL")
	defer b.mu.RUnlock()

	base := b.validationBase
	if base == "" {
		base = "https://localhost"
	}

	return base + validatePath + "?id=" + url.QueryEscape(code)
}

func validationEvent(topicRID, code, validationURL string) ([]byte, error) {
	return json.Marshal([]map[string]any{{
		"id": uuid.NewString(), "topic": topicRID, "subject": "", "eventType": validationEventType,
		"eventTime": time.Now().UTC().Format(time.RFC3339Nano), "metadataVersion": "1", "dataVersion": "2",
		"data": map[string]any{"validationCode": code, "validationUrl": validationURL},
	}})
}

// postHandshake sends the SubscriptionValidationEvent; echoed reports whether the
// endpoint returned the validationCode (otherwise manual validation is required).
func (b *InMemoryBackend) postHandshake(
	ctx context.Context, topicRID, endpoint, code, validationURL string,
) (bool, error) {
	body, err := validationEvent(topicRID, code, validationURL)
	if err != nil {
		return false, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return false, ErrWebhookValidation
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Aeg-Event-Type", "SubscriptionValidation")

	resp, err := b.httpc.Do(req)
	if err != nil {
		return false, ErrWebhookValidation
	}

	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, ErrWebhookValidation
	}

	var out struct {
		ValidationResponse string `json:"validationResponse"`
	}

	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	_ = json.Unmarshal(data, &out)

	return out.ValidationResponse == code, nil
}

// optionsHandshake performs the CloudEvents abuse-protection OPTIONS handshake.
func (b *InMemoryBackend) optionsHandshake(ctx context.Context, endpoint, validationURL string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodOptions, endpoint, http.NoBody)
	if err != nil {
		return false, ErrWebhookValidation
	}

	req.Header.Set("Webhook-Request-Origin", originEventGrid)
	req.Header.Set("Webhook-Request-Callback", validationURL)
	req.Header.Set("Webhook-Request-Rate", "*")

	resp, err := b.httpc.Do(req)
	if err != nil {
		return false, ErrWebhookValidation
	}

	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, ErrWebhookValidation
	}

	allowed := resp.Header.Get("Webhook-Allowed-Origin")

	return allowed == "*" || allowed == originEventGrid, nil
}
