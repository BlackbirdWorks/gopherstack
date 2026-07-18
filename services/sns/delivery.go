package sns

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func parseTopicEffectivePolicy(topicEffectivePolicy, protocol string) *int {
	var tp map[string]struct {
		DefaultHealthyRetryPolicy struct {
			NumRetries *int `json:"numRetries"`
		} `json:"defaultHealthyRetryPolicy"`
	}
	if err := json.Unmarshal([]byte(topicEffectivePolicy), &tp); err == nil {
		pKey := protocol
		if pKey == protocolHTTPS {
			pKey = protocolHTTP
		}
		if protoPol, ok := tp[pKey]; ok {
			return protoPol.DefaultHealthyRetryPolicy.NumRetries
		}
	}

	return nil
}

func parseSubPolicy(subPolicy string) *int {
	var sp struct {
		HealthyRetryPolicy struct {
			NumRetries *int `json:"numRetries"`
		} `json:"healthyRetryPolicy"`
	}
	if err := json.Unmarshal([]byte(subPolicy), &sp); err == nil {
		return sp.HealthyRetryPolicy.NumRetries
	}

	return nil
}

// getRetryConfig parses the EffectiveDeliveryPolicy and/or DeliveryPolicy for a given protocol.
// It returns the number of retries configured.
func getRetryConfig(topicEffectivePolicy, subPolicy, protocol string) int {
	numRetries := 3

	if topicEffectivePolicy != "" {
		if nr := parseTopicEffectivePolicy(topicEffectivePolicy, protocol); nr != nil {
			numRetries = *nr
		}
	}

	if subPolicy != "" {
		if nr := parseSubPolicy(subPolicy); nr != nil {
			numRetries = *nr
		}
	}

	if numRetries < 0 {
		numRetries = 0
	}

	return numRetries
}

func (b *InMemoryBackend) logDeliveryStatus(
	ctx context.Context,
	topicARN, protocol, endpoint, status string,
	err error,
) {
	var roleArn string

	func() {
		b.mu.RLock("logDeliveryStatus")
		defer b.mu.RUnlock()

		topic, ok := b.topics.Get(topicARN)
		if !ok {
			return
		}

		// Determine the protocol prefix
		constHTTPPrefix := "HTTP"
		constHTTPSPrefix := "HTTPS"
		var prefix string
		switch protocol {
		case protocolHTTP, protocolHTTPS:
			prefix = constHTTPPrefix
			if protocol == protocolHTTPS {
				if topic.Attributes["HTTPSSuccessFeedbackRoleArn"] != "" {
					prefix = constHTTPSPrefix
				} else {
					prefix = constHTTPPrefix
				}
			}
		case protocolLambda:
			prefix = "Lambda"
		case protocolFirehose:
			prefix = "Firehose"
		case protocolApplication:
			prefix = "Application"
		case protocolSQS:
			prefix = "SQS"
		default:
			prefix = constHTTPPrefix
		}

		roleArnAttr := prefix + "SuccessFeedbackRoleArn"
		if status == "FAILURE" {
			roleArnAttr = prefix + "FailureFeedbackRoleArn"
		}

		roleArn = topic.Attributes[roleArnAttr]
	}()

	if roleArn == "" {
		return
	}

	l := logger.Load(ctx).With(
		"protocol", protocol,
		"endpoint", endpoint,
		"status", status,
		"role_arn", roleArn,
		"topic_arn", topicARN,
	)
	if err != nil {
		l.InfoContext(ctx, "SNS delivery status", "error", err.Error())
	} else {
		l.InfoContext(ctx, "SNS delivery status")
	}
}

func buildHTTPDeliveryPayload(d httpDelivery) string {
	body := d.body

	if !d.rawDelivery && d.messageID != "" {
		timestamp := time.Now().UTC().Format(time.RFC3339)

		const (
			arnFieldCount   = 6
			arnRegionIndex  = 3
			arnMinFieldsReg = 4
		)
		topicRegion := "us-east-1"
		if parts := strings.SplitN(d.topicARN, ":", arnFieldCount); len(parts) >= arnMinFieldsReg &&
			parts[arnRegionIndex] != "" {
			topicRegion = parts[arnRegionIndex]
		}
		certURL := fmt.Sprintf(
			"https://sns.%s.amazonaws.com/SimpleNotificationService.pem",
			topicRegion,
		)
		signature := "MOCK-SIGNATURE"
		if d.signer != nil {
			certURL = d.signer.certURL()
			canonical := canonicalNotificationString(
				d.messageID, d.topicARN, d.subject, d.body, timestamp,
			)
			signature = d.signer.sign(canonical)
		}

		env := snsHTTPNotification{
			Type:             messageTypeNotification,
			MessageID:        d.messageID,
			TopicArn:         d.topicARN,
			Message:          d.body,
			Timestamp:        timestamp,
			SignatureVersion: "2",
			Signature:        signature,
			SigningCertURL:   certURL,
			UnsubscribeURL: "https://sns." + topicRegion +
				".amazonaws.com/?Action=Unsubscribe&SubscriptionArn=" + d.subscriptionARN,
		}
		if d.subject != "" {
			env.Subject = d.subject
		}

		if enc, err := json.Marshal(env); err == nil {
			body = string(enc)
		}
	}

	return body
}

// deliverHTTPWithMeta sends a best-effort HTTP POST with SNS notification headers
// to the endpoint. Standard AWS SNS headers are added when metadata is available.
// When rawDelivery is false the body is wrapped in a SNS Notification JSON envelope
// (matching what AWS SNS sends to http/https subscribers by default).
// On network error or non-2xx response, the message is forwarded to the DLQ when
// a RedrivePolicy is configured.
func deliverHTTPWithMeta(parent context.Context, d httpDelivery, client *http.Client, b *InMemoryBackend) {
	ctx, cancel := context.WithTimeout(parent, snsHTTPTimeout)
	defer cancel()

	body := buildHTTPDeliveryPayload(d)

	protocol := protocolHTTP
	if strings.HasPrefix(d.endpoint, "https://") {
		protocol = protocolHTTPS
	}
	numRetries := getRetryConfig(d.topicEffectivePolicy, d.deliveryPolicy, protocol)

	var err error
	var resp *http.Response

	for i := 0; i <= numRetries; i++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Amz-Sns-Message-Type", messageTypeNotification)
		if d.messageID != "" {
			req.Header.Set("X-Amz-Sns-Message-Id", d.messageID)
		}
		if d.topicARN != "" {
			req.Header.Set("X-Amz-Sns-Topic-Arn", d.topicARN)
		}
		if d.subscriptionARN != "" {
			req.Header.Set("X-Amz-Sns-Subscription-Arn", d.subscriptionARN)
		}

		resp, err = client.Do(req)
		if err == nil {
			defer func() { _ = resp.Body.Close() }()
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDeliveryResponseBytes))

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				b.logDeliveryStatus(parent, d.topicARN, protocol, d.endpoint, "SUCCESS", nil)

				return
			}
			err = fmt.Errorf("%w: %d", ErrHTTPStatus, resp.StatusCode)
		}
	}

	b.logDeliveryStatus(parent, d.topicARN, protocol, d.endpoint, "FAILURE", err)
	sendSubscriptionDLQ(parent, d)
}

// sendSubscriptionDLQ delivers the message body to the DLQ configured in d.redrivePolicy when
// d.sqsSender is non-nil. It is a no-op when either is absent.
func sendSubscriptionDLQ(ctx context.Context, d httpDelivery) {
	if d.sqsSender == nil || d.redrivePolicy == "" {
		return
	}

	var policy struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}

	if err := json.Unmarshal([]byte(d.redrivePolicy), &policy); err != nil {
		return
	}

	if policy.DeadLetterTargetArn == "" {
		return
	}

	_ = d.sqsSender.SendMessageToQueue(ctx, policy.DeadLetterTargetArn, d.body)
}
