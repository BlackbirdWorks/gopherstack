package iot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	msgTypeConfirmation  = "DestinationConfirmation"
	headerMessageType    = "X-Amz-Rules-Engine-Message-Type"
	headerDestinationARN = "X-Amz-Rules-Engine-Destination-Arn"
	statusError          = "ERROR"
	confirmFailedReason  = "Confirmation request failed"
	templateOpen         = "${"
)

// confirmation is one pending HTTP destination confirmation request (iot http-action-destination).
type confirmation struct {
	destARN   string
	url       string
	token     string
	enableURL string
}

// SetEndpointBase records the scheme and host clients reach this service on; enableUrl is built from it.
func (b *InMemoryBackend) SetEndpointBase(base string) {
	b.mu.Lock("SetEndpointBase")
	defer b.mu.Unlock()

	b.endpointBase = base
}

// DrainBackground waits for in-flight confirmation requests to finish.
func (b *InMemoryBackend) DrainBackground(ctx context.Context) {
	done := make(chan struct{})

	go func() {
		b.bg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// newConfirmationLocked builds the confirmation request for a destination; the caller holds b.mu.
func (b *InMemoryBackend) newConfirmationLocked(d *TopicRuleDestination) confirmation {
	base := b.endpointBase
	if base == "" {
		base = "https://iot." + b.region + ".amazonaws.com"
	}

	return confirmation{
		destARN: d.ARN, url: d.HTTPURLProperties.ConfirmationURL, token: d.ConfirmationToken,
		enableURL: base + pathConfirmDestination + "/" + d.ConfirmationToken,
	}
}

// dispatchConfirmation sends the confirmation request in the background; a failure sets the destination to ERROR.
func (b *InMemoryBackend) dispatchConfirmation(c confirmation) {
	b.bg.Go(func() {
		if err := sendConfirmation(c); err != nil {
			b.failConfirmation(c)
		}
	})
}

func sendConfirmation(c confirmation) error {
	u, err := url.Parse(c.url)
	if err != nil || (u.Scheme != "https" && u.Scheme != actionHTTP) || u.Host == "" {
		return errHTTPTransport
	}

	if u.Path == "" {
		u.Path = "/"
	}

	q := u.Query()
	q.Set("confirmationToken", c.token)
	u.RawQuery = q.Encode()

	body, err := json.Marshal(map[string]string{
		keyArn: c.destARN, "confirmationToken": c.token, "enableUrl": c.enableURL, "messageType": msgTypeConfirmation,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), httpActionTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return errHTTPTransport
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerMessageType, msgTypeConfirmation)
	req.Header.Set(headerDestinationARN, c.destARN)

	client := newEgressClient()
	defer client.CloseIdleConnections()

	_, err = doOnce(client, req)

	return err
}

func (b *InMemoryBackend) failConfirmation(c confirmation) {
	b.mu.Lock("failConfirmation")
	defer b.mu.Unlock()

	d, ok := b.topicRuleDestinations.Get(c.destARN)
	if !ok || d.Status != statusInProgress || d.ConfirmationToken != c.token {
		return
	}

	d.Status = statusError
	d.StatusReason = confirmFailedReason
	d.LastUpdatedAt = time.Now()
}

// httpActionsOf returns the http actions of a rule payload, including the error action.
func httpActionsOf(actions []RuleAction, errAction *RuleAction) ([]httpWire, error) {
	all := actions
	if errAction != nil {
		all = append(append([]RuleAction{}, actions...), *errAction)
	}

	var out []httpWire

	for _, a := range all {
		raw, ok := a.Other[actionHTTP]
		if !ok {
			continue
		}

		var w httpWire
		if err := json.Unmarshal(raw, &w); err != nil {
			return nil, fmt.Errorf("%w: http action: %w", ErrValidation, err)
		}

		out = append(out, w)
	}

	return out, nil
}

// effectiveConfirmationURL applies the url/confirmationUrl rules of the http action docs.
func (w httpWire) effectiveConfirmationURL() (string, error) {
	if w.URL == "" {
		return "", fmt.Errorf("%w: http action needs a url", ErrValidation)
	}

	cu := w.ConfirmationURL

	switch {
	case cu == "" && strings.Contains(w.URL, templateOpen):
		return "", fmt.Errorf("%w: confirmationUrl is required when url has a substitution template", ErrValidation)
	case cu == "":
		return w.URL, nil
	case !strings.Contains(cu, templateOpen) && !strings.HasPrefix(w.URL, cu):
		return "", fmt.Errorf("%w: confirmationUrl must be a prefix of url", ErrValidation)
	}

	return cu, nil
}

// ensureHTTPDestinationsLocked creates the destination each http action implies; the caller holds b.mu.
func (b *InMemoryBackend) ensureHTTPDestinationsLocked(actions []RuleAction, errAction *RuleAction) error {
	https, err := httpActionsOf(actions, errAction)
	if err != nil {
		return err
	}

	urls := make([]string, 0, len(https))

	for _, w := range https {
		cu, cerr := w.effectiveConfirmationURL()
		if cerr != nil {
			return cerr
		}

		if !strings.Contains(cu, templateOpen) {
			urls = append(urls, cu)
		}
	}

	for _, cu := range urls {
		if !b.hasHTTPDestinationLocked(cu) {
			b.createHTTPDestinationLocked(cu)
		}
	}

	return nil
}

func (b *InMemoryBackend) hasHTTPDestinationLocked(confirmationURL string) bool {
	for _, d := range b.topicRuleDestinations.All() {
		if d.HTTPURLProperties != nil && d.HTTPURLProperties.ConfirmationURL == confirmationURL {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) createHTTPDestinationLocked(confirmationURL string) {
	now := time.Now()
	d := &TopicRuleDestination{
		ARN:               arn.Build("iot", b.region, b.accountID, "ruledestination/http/"+uuid.NewString()),
		CreatedAt:         now,
		LastUpdatedAt:     now,
		HTTPURLProperties: &HTTPURLDestinationProperties{ConfirmationURL: confirmationURL},
		Status:            statusInProgress,
		ConfirmationToken: randomHex(),
	}

	b.topicRuleDestinations.Put(d)
	b.dispatchConfirmation(b.newConfirmationLocked(d))
}
