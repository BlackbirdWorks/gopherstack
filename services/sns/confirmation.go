package sns

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	messageTypeSubscriptionConfirmation = "SubscriptionConfirmation"
	certPEMSuffix                       = "/SimpleNotificationService.pem"
)

// ConfirmationToken returns the token delivered to an HTTP/HTTPS endpoint in
// its SubscriptionConfirmation message for the given subscription ARN.
func ConfirmationToken(subscriptionARN string) string {
	sum := sha256.Sum256([]byte("sns-subscription-confirmation:" + subscriptionARN))

	return hex.EncodeToString(sum[:])
}

type snsSubscriptionConfirmation struct {
	Type             string `json:"Type"`
	MessageID        string `json:"MessageId"`
	Token            string `json:"Token"`
	TopicArn         string `json:"TopicArn"`
	Message          string `json:"Message"`
	SubscribeURL     string `json:"SubscribeURL"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
	SigningCertURL   string `json:"SigningCertURL"`
}

func (b *InMemoryBackend) dispatchSubscriptionConfirmation(
	topicArn, subscriptionARN, endpoint, sigAttr, baseURL string,
) {
	if b.closing.Load() {
		return
	}

	b.deliveryWg.Go(func() {
		b.deliverSubscriptionConfirmation(b.svcCtx, topicArn, subscriptionARN, endpoint, sigAttr, baseURL)
	})
}

func (b *InMemoryBackend) deliverSubscriptionConfirmation(
	ctx context.Context, topicArn, subscriptionARN, endpoint, sigAttr, baseURL string,
) {
	b.mu.RLock("SubscriptionConfirmation")
	client := b.httpClient
	b.mu.RUnlock()

	token := ConfirmationToken(subscriptionARN)
	msgID := uuid.NewString()
	timestamp := time.Now().UTC().Format(time.RFC3339)
	certURL := b.signer.certURL()
	base := strings.TrimRight(baseURL, "/")
	if base == "" {
		base = strings.TrimSuffix(certURL, certPEMSuffix)
	}

	subscribeURL := base + "/?Action=ConfirmSubscription&TopicArn=" + topicArn + "&Token=" + token
	message := "You have chosen to subscribe to the topic " + topicArn + ".\n" +
		"To confirm the subscription, visit the SubscribeURL included in this message."
	version := resolveSignatureVersion(sigAttr)

	canonical := canonicalString([]signedField{
		{"Message", message},
		{"MessageId", msgID},
		{"SubscribeURL", subscribeURL},
		{"Timestamp", timestamp},
		{"Token", token},
		{topicArnKey, topicArn},
		{"Type", messageTypeSubscriptionConfirmation},
	})

	payload, err := json.Marshal(snsSubscriptionConfirmation{
		Type:             messageTypeSubscriptionConfirmation,
		MessageID:        msgID,
		Token:            token,
		TopicArn:         topicArn,
		Message:          message,
		SubscribeURL:     subscribeURL,
		Timestamp:        timestamp,
		SignatureVersion: version,
		Signature:        b.signer.signWithVersion(canonical, version),
		SigningCertURL:   certURL,
	})
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, snsHTTPTimeout)
	defer cancel()

	target, ok := sanitizeEndpointURL(endpoint)
	if !ok {
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "text/plain; charset=UTF-8")
	req.Header.Set("X-Amz-Sns-Message-Type", messageTypeSubscriptionConfirmation)
	req.Header.Set("X-Amz-Sns-Message-Id", msgID)
	req.Header.Set("X-Amz-Sns-Topic-Arn", topicArn)

	resp, err := client.Do(req)
	if err != nil {
		return
	}

	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDeliveryResponseBytes))
	_ = resp.Body.Close()
}

// sanitizeEndpointURL accepts only http/https URLs and rebuilds them from parsed parts.
func sanitizeEndpointURL(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", false
	}

	clean := url.URL{
		Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath, RawQuery: u.RawQuery, User: u.User,
	}

	return clean.String(), true
}
