package sqs_test

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// snsEnvelope mirrors the SNS notification envelope delivered to SQS.
type snsEnvelopeFields struct {
	MessageAttributes map[string]map[string]string `json:"MessageAttributes"`
	Signature         string                       `json:"Signature"`
	SigningCertURL    string                       `json:"SigningCertURL"`
	UnsubscribeURL    string                       `json:"UnsubscribeURL"`
	TopicArn          string                       `json:"TopicArn"`
	Message           string                       `json:"Message"`
	MessageID         string                       `json:"MessageId"`
	Type              string                       `json:"Type"`
	Subject           string                       `json:"Subject"`
}

// receiveOneEnvelope publishes a message and receives the resulting SQS envelope.
func receiveOneEnvelope(t *testing.T, snsBk *snsbackend.InMemoryBackend, sqsBk *sqs.InMemoryBackend,
	queueURL, topicARN, message string,
	attrs map[string]snsbackend.MessageAttribute,
) snsEnvelopeFields {
	t.Helper()

	_, err := snsBk.Publish(topicARN, message, "", "", attrs)
	require.NoError(t, err)

	out, err := sqsBk.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	var env snsEnvelopeFields
	require.NoError(t, json.Unmarshal([]byte(out.Messages[0].Body), &env))

	return env
}

// TestSNS_SQS_Envelope_NonRaw_MessageAttributes verifies that MessageAttributes published
// via SNS appear in the SQS message envelope body for non-raw subscriptions.
func TestSNS_SQS_Envelope_NonRaw_MessageAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		attrs         map[string]snsbackend.MessageAttribute
		wantAttrNames []string
		wantNoAttrs   bool
	}{
		{
			name: "StringAttribute_PresentInEnvelope",
			attrs: map[string]snsbackend.MessageAttribute{
				"event-type": {DataType: "String", StringValue: "order-placed"},
			},
			wantAttrNames: []string{"event-type"},
		},
		{
			name: "NumberAttribute_PresentInEnvelope",
			attrs: map[string]snsbackend.MessageAttribute{
				"priority": {DataType: "Number", StringValue: "5"},
			},
			wantAttrNames: []string{"priority"},
		},
		{
			name: "MultipleAttributes_AllPresent",
			attrs: map[string]snsbackend.MessageAttribute{
				"type":   {DataType: "String", StringValue: "invoice"},
				"amount": {DataType: "Number", StringValue: "99"},
			},
			wantAttrNames: []string{"type", "amount"},
		},
		{
			name:        "NoAttributes_MessageAttributesEmptyOrAbsent",
			attrs:       nil,
			wantNoAttrs: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snsBk, sqsBk := newWiredPair(t)

			_, err := sqsBk.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "env-attr-queue",
				Attributes: map[string]string{
					"VisibilityTimeout": "30",
				},
			})
			require.NoError(t, err)

			topic, err := snsBk.CreateTopic("env-attr-topic", nil)
			require.NoError(t, err)

			_, err = snsBk.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:env-attr-queue", "")
			require.NoError(t, err)

			env := receiveOneEnvelope(t, snsBk, sqsBk,
				"http://localhost:8000/000000000000/env-attr-queue",
				topic.TopicArn, "test message", tt.attrs)

			assert.Equal(t, "Notification", env.Type)

			if tt.wantNoAttrs {
				assert.Empty(t, env.MessageAttributes)
			} else {
				require.NotNil(t, env.MessageAttributes)
				for _, attrName := range tt.wantAttrNames {
					_, ok := env.MessageAttributes[attrName]
					assert.True(t, ok, "expected MessageAttribute %q in envelope", attrName)
				}
			}
		})
	}
}

// TestSNS_SQS_Envelope_NonRaw_Signature verifies that the SNS→SQS envelope includes a
// non-empty Signature and SigningCertURL (AWS spec §3.2). The Signature must be a valid
// base64-decodeable RSA-SHA1 blob, and the signing certificate PEM must be parseable.
func TestSNS_SQS_Envelope_NonRaw_Signature(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		message       string
		wantSignature bool
		wantCertURL   bool
	}{
		{
			name:          "BasicMessage_SignaturePresent",
			message:       "hello signed world",
			wantSignature: true,
			wantCertURL:   true,
		},
		{
			name:          "EmptyMessage_SignaturePresent",
			message:       "",
			wantSignature: true,
			wantCertURL:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snsBk, sqsBk := newWiredPair(t)

			_, err := sqsBk.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "sig-check-queue",
				Attributes: map[string]string{
					"VisibilityTimeout": "30",
				},
			})
			require.NoError(t, err)

			topic, err := snsBk.CreateTopic("sig-check-topic", nil)
			require.NoError(t, err)

			_, err = snsBk.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:sig-check-queue", "")
			require.NoError(t, err)

			env := receiveOneEnvelope(t, snsBk, sqsBk,
				"http://localhost:8000/000000000000/sig-check-queue",
				topic.TopicArn, tt.message, nil)

			if tt.wantSignature {
				assert.NotEmpty(t, env.Signature, "Signature must be present in non-raw envelope")

				// Must be valid base64.
				sigBytes, decErr := base64.StdEncoding.DecodeString(env.Signature)
				require.NoError(t, decErr, "Signature must be valid base64")
				assert.NotEmpty(t, sigBytes)
			}

			if tt.wantCertURL {
				assert.NotEmpty(t, env.SigningCertURL, "SigningCertURL must be present in non-raw envelope")
			}
		})
	}
}

// TestSNS_SQS_Envelope_NonRaw_SignatureCertParseable verifies that the PEM certificate
// served at SigningCertURL is parseable as an X.509 certificate. This is the cert that
// AWS subscribers use to verify the Signature field.
func TestSNS_SQS_Envelope_NonRaw_SignatureCertParseable(t *testing.T) {
	t.Parallel()

	snsBk, sqsBk := newWiredPair(t)

	_, err := sqsBk.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "cert-parse-queue",
		Attributes: map[string]string{
			"VisibilityTimeout": "30",
		},
	})
	require.NoError(t, err)

	topic, err := snsBk.CreateTopic("cert-parse-topic", nil)
	require.NoError(t, err)

	_, err = snsBk.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:cert-parse-queue", "")
	require.NoError(t, err)

	env := receiveOneEnvelope(t, snsBk, sqsBk,
		"http://localhost:8000/000000000000/cert-parse-queue",
		topic.TopicArn, "cert test", nil)

	require.NotEmpty(t, env.SigningCertURL)

	// Fetch the PEM from the cert URL exposed by the SNS backend.
	certPEM := snsBk.SigningCertPEM()
	require.NotEmpty(t, certPEM, "SNS backend must expose the signing certificate PEM")

	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block, "SigningCertURL body must be a valid PEM block")

	_, parseErr := x509.ParseCertificate(block.Bytes)
	require.NoError(t, parseErr, "PEM must decode to a valid X.509 certificate")
}

// TestSNS_SQS_Envelope_NonRaw_UnsubscribeURL verifies that the envelope includes a
// non-empty UnsubscribeURL matching the subscription.
func TestSNS_SQS_Envelope_NonRaw_UnsubscribeURL(t *testing.T) {
	t.Parallel()

	snsBk, sqsBk := newWiredPair(t)

	_, err := sqsBk.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "unsub-queue",
		Attributes: map[string]string{
			"VisibilityTimeout": "30",
		},
	})
	require.NoError(t, err)

	topic, err := snsBk.CreateTopic("unsub-topic", nil)
	require.NoError(t, err)

	_, err = snsBk.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:unsub-queue", "")
	require.NoError(t, err)

	env := receiveOneEnvelope(t, snsBk, sqsBk,
		"http://localhost:8000/000000000000/unsub-queue",
		topic.TopicArn, "unsubscribe test", nil)

	assert.NotEmpty(t, env.UnsubscribeURL, "UnsubscribeURL must be present in non-raw envelope")
}
