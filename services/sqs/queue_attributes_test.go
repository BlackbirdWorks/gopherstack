package sqs_test

import (
	"fmt"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKMSAttrsConfigurable(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// CreateQueue with KMS attrs should succeed.
	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "kms-queue",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"KmsMasterKeyId":               "alias/aws/sqs",
			"KmsDataKeyReusePeriodSeconds": "300",
		},
	})
	require.NoError(t, err)

	// Idempotency: same attrs → same URL.
	out2, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "kms-queue",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"KmsMasterKeyId":               "alias/aws/sqs",
			"KmsDataKeyReusePeriodSeconds": "300",
		},
	})
	require.NoError(t, err)
	assert.Contains(t, out2.QueueURL, "kms-queue")

	// Different KMS key → QueueAlreadyExists.
	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "kms-queue",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"KmsMasterKeyId": "alias/other",
		},
	})
	require.ErrorIs(t, err, sqs.ErrQueueAlreadyExists)
}

func TestKMSDataKeyReuseValidation(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Out-of-range values are rejected.
	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "bad-kms",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"KmsDataKeyReusePeriodSeconds": "30", // below min 60
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidAttribute)

	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "bad-kms2",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"KmsDataKeyReusePeriodSeconds": "90000", // above max 86400
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidAttribute)
}

func TestApproxCounts_EmptyQueue(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "approx-empty")
	attrs := b2getAttrs(
		t,
		b,
		qURL,
		"ApproximateNumberOfMessages",
		"ApproximateNumberOfMessagesNotVisible",
		"ApproximateNumberOfMessagesDelayed",
	)
	assert.Equal(t, "0", attrs["ApproximateNumberOfMessages"])
	assert.Equal(t, "0", attrs["ApproximateNumberOfMessagesNotVisible"])
	assert.Equal(t, "0", attrs["ApproximateNumberOfMessagesDelayed"])
}

func TestApproxCounts_VisibleMessages(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "approx-visible")
	b2send(t, b, qURL, "a")
	b2send(t, b, qURL, "b")
	b2send(t, b, qURL, "c")

	attrs := b2getAttrs(t, b, qURL, "ApproximateNumberOfMessages")
	assert.Equal(t, "3", attrs["ApproximateNumberOfMessages"])
}

func TestApproxCounts_InFlight(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "approx-inflight",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "msg1")
	b2send(t, b, qURL.QueueURL, "msg2")

	// Receive 1 — it goes in-flight
	msgs := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs, 1)

	attrs := b2getAttrs(t, b, qURL.QueueURL, "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible")
	assert.Equal(t, "1", attrs["ApproximateNumberOfMessages"])
	assert.Equal(t, "1", attrs["ApproximateNumberOfMessagesNotVisible"])

	b2delete(t, b, qURL.QueueURL, msgs[0].ReceiptHandle)
}

func TestApproxCounts_DelayedMessages(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "approx-delayed")

	// Send with delay — message not yet visible
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:     qURL,
		MessageBody:  "delayed",
		DelaySeconds: 900,
	})
	require.NoError(t, err)

	attrs := b2getAttrs(t, b, qURL, "ApproximateNumberOfMessagesDelayed", "ApproximateNumberOfMessages")
	assert.Equal(t, "1", attrs["ApproximateNumberOfMessagesDelayed"])
	assert.Equal(t, "0", attrs["ApproximateNumberOfMessages"])
}

func TestMessageRetentionPeriod_Validation(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		seconds string
		valid   bool
	}{
		{"60", true},
		{"1209600", true},
		{"345600", true},
		{"59", false},
		{"1209601", false},
		{"0", false},
	}
	for _, tc := range cases {
		t.Run("secs="+tc.seconds, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "mrp-val-" + tc.seconds,
				Endpoint:  "localhost",
				Attributes: map[string]string{
					"MessageRetentionPeriod": tc.seconds,
				},
			})
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestMessageRetentionPeriod_ExpiredMessagesNotDelivered(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := b2newBackend(t)

		qURL := b2createQueue(t, b, "mrp-expire")

		// Inject a retention of 1 second (below minimum, so use test helper)
		b2send(t, b, qURL, "will-expire")

		// Use test helper to set 1s retention and fast-forward janitor
		b.SetRetentionForTest(qURL, 1)
		time.Sleep(2 * time.Millisecond)
		b.RunJanitorOnceForTest(time.Now().Add(2 * time.Second))

		msgs := b2receive(t, b, qURL, 1)
		assert.Empty(t, msgs)
	})
}

func TestMessageRetentionPeriod_SetViaAttributes(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "mrp-set")
	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"MessageRetentionPeriod": "3600"},
	}))

	attrs := b2getAttrs(t, b, qURL, "MessageRetentionPeriod")
	assert.Equal(t, "3600", attrs["MessageRetentionPeriod"])
}

func TestSSE_SqsManagedSseEnabled_Default(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sse-default")
	attrs := b2getAttrs(t, b, qURL, "SqsManagedSseEnabled")
	assert.Equal(t, "true", attrs["SqsManagedSseEnabled"])
}

func TestSSE_SqsManagedSseEnabled_SetFalse(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "sse-false",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"SqsManagedSseEnabled": "false",
		},
	})
	require.NoError(t, err)

	attrs := b2getAttrs(t, b, qURL.QueueURL, "SqsManagedSseEnabled")
	assert.Equal(t, "false", attrs["SqsManagedSseEnabled"])
}

func TestSSE_KmsMasterKeyId_Configurable(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "kms-key",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"KmsMasterKeyId":               "alias/aws/sqs",
			"KmsDataKeyReusePeriodSeconds": "300",
		},
	})
	require.NoError(t, err)

	attrs := b2getAttrs(t, b, qURL.QueueURL, "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds")
	assert.Equal(t, "alias/aws/sqs", attrs["KmsMasterKeyId"])
	assert.Equal(t, "300", attrs["KmsDataKeyReusePeriodSeconds"])
}

func TestSSE_KmsDataKeyReuseRange_Validated(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		secs  string
		valid bool
	}{
		{"60", true},
		{"86400", true},
		{"300", true},
		{"59", false},
		{"86401", false},
	}
	for _, tc := range cases {
		t.Run("secs="+tc.secs, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "kms-rng-" + tc.secs,
				Endpoint:  "localhost",
				Attributes: map[string]string{
					"KmsMasterKeyId":               "alias/key",
					"KmsDataKeyReusePeriodSeconds": tc.secs,
				},
			})
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestSSE_KMS_SetViaSetQueueAttributes(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "kms-setattr")

	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL: qURL,
		Attributes: map[string]string{
			"KmsMasterKeyId":               "alias/my-key",
			"KmsDataKeyReusePeriodSeconds": "120",
		},
	}))

	attrs := b2getAttrs(t, b, qURL, "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds")
	assert.Equal(t, "alias/my-key", attrs["KmsMasterKeyId"])
	assert.Equal(t, "120", attrs["KmsDataKeyReusePeriodSeconds"])
}

func TestSSE_Idempotency_SameKMSKey(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	attrs := map[string]string{"KmsMasterKeyId": "alias/aws/sqs"}

	out1, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "kms-idem", Endpoint: "localhost", Attributes: attrs})
	require.NoError(t, err)

	// Same KMS key → idempotent
	out2, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "kms-idem", Endpoint: "localhost", Attributes: attrs})
	require.NoError(t, err)
	assert.Equal(t, out1.QueueURL, out2.QueueURL)

	// Different KMS key → QueueAlreadyExists
	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "kms-idem",
		Endpoint:   "localhost",
		Attributes: map[string]string{"KmsMasterKeyId": "alias/other-key"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueAlreadyExists)
}

func TestSetGetAttributes_VisibilityTimeout(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sqa-vt")
	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"VisibilityTimeout": "120"},
	}))

	attrs := b2getAttrs(t, b, qURL, "VisibilityTimeout")
	assert.Equal(t, "120", attrs["VisibilityTimeout"])
}

func TestSetGetAttributes_UpdatesLastModified(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := b2newBackend(t)

		qURL := b2createQueue(t, b, "sqa-lm")
		before := b2getAttrs(t, b, qURL, "LastModifiedTimestamp")["LastModifiedTimestamp"]

		time.Sleep(time.Millisecond)

		require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
			QueueURL:   qURL,
			Attributes: map[string]string{"VisibilityTimeout": "15"},
		}))

		after := b2getAttrs(t, b, qURL, "LastModifiedTimestamp")["LastModifiedTimestamp"]
		assert.GreaterOrEqual(t, after, before)
	})
}

func TestSetQueueAttributes_InvalidRange(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sqa-range")

	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"VisibilityTimeout": "43201"},
	})
	require.Error(t, err)
}

func TestGetQueueAttributes_AllReturnsAll(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sqa-all")
	attrs := b2getAttrs(t, b, qURL, "All")

	required := []string{
		"VisibilityTimeout",
		"MaximumMessageSize",
		"MessageRetentionPeriod",
		"DelaySeconds",
		"ReceiveMessageWaitTimeSeconds",
		"QueueArn",
		"CreatedTimestamp",
		"LastModifiedTimestamp",
		"ApproximateNumberOfMessages",
		"ApproximateNumberOfMessagesNotVisible",
	}
	for _, k := range required {
		assert.Contains(t, attrs, k, "missing attribute: %s", k)
	}
}

func TestGetQueueAttributes_NotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	_, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       "http://localhost/000000000000/nonexistent",
		AttributeNames: []string{"All"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestQueueAttr_VisibilityTimeout_Range(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		v  string
		ok bool
	}{
		{"0", true},
		{"43200", true},
		{"30", true},
		{"-1", false},
		{"43201", false},
	}
	for _, tc := range cases {
		t.Run("vt="+tc.v, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "vtr-" + tc.v,
				Endpoint:   "localhost",
				Attributes: map[string]string{"VisibilityTimeout": tc.v},
			})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestQueueAttr_DelaySeconds_Range(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		v  string
		ok bool
	}{
		{"0", true},
		{"900", true},
		{"-1", false},
		{"901", false},
	}
	for _, tc := range cases {
		t.Run("ds="+tc.v, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "dsr-" + tc.v,
				Endpoint:   "localhost",
				Attributes: map[string]string{"DelaySeconds": tc.v},
			})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestQueueAttr_ReceiveWaitSeconds_Range(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		v  string
		ok bool
	}{
		{"0", true},
		{"20", true},
		{"-1", false},
		{"21", false},
	}
	for _, tc := range cases {
		t.Run("rws="+tc.v, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "rwsr-" + tc.v,
				Endpoint:   "localhost",
				Attributes: map[string]string{"ReceiveMessageWaitTimeSeconds": tc.v},
			})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestQueueAttr_MaxMessageSize_Range(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		v  string
		ok bool
	}{
		{"1024", true},
		{"262144", true},
		{"65536", true},
		{"1023", false},
		{"262145", false},
	}
	for _, tc := range cases {
		t.Run("mms="+tc.v, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "mmsr-" + tc.v,
				Endpoint:   "localhost",
				Attributes: map[string]string{"MaximumMessageSize": tc.v},
			})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// TestApproximateCounts verifies GetQueueAttributes returns accurate counts.
func TestApproximateCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sendCount    int
		receiveCount int // messages to receive (put in-flight)
		delaySeconds int // per-message delay
		wantVisible  int
		wantInFlight int
		wantDelayed  int
	}{
		{
			name:         "all visible — no receives",
			sendCount:    3,
			receiveCount: 0,
			wantVisible:  3,
			wantInFlight: 0,
			wantDelayed:  0,
		},
		{
			name:         "some in-flight",
			sendCount:    4,
			receiveCount: 2,
			wantVisible:  2,
			wantInFlight: 2,
			wantDelayed:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "counts-q")

			for i := range tc.sendCount {
				_, err := b.SendMessage(&sqs.SendMessageInput{
					QueueURL:     qURL,
					MessageBody:  fmt.Sprintf("msg-%d", i),
					DelaySeconds: tc.delaySeconds,
				})
				require.NoError(t, err)
			}

			if tc.receiveCount > 0 {
				_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            qURL,
					MaxNumberOfMessages: tc.receiveCount,
					VisibilityTimeout:   30,
				})
				require.NoError(t, err)
			}

			attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueURL:       qURL,
				AttributeNames: []string{"All"},
			})
			require.NoError(t, err)

			assert.Equal(t, strconv.Itoa(tc.wantVisible),
				attrs.Attributes["ApproximateNumberOfMessages"],
				"ApproximateNumberOfMessages")
			assert.Equal(t, strconv.Itoa(tc.wantInFlight),
				attrs.Attributes["ApproximateNumberOfMessagesNotVisible"],
				"ApproximateNumberOfMessagesNotVisible")
			assert.Equal(t, strconv.Itoa(tc.wantDelayed),
				attrs.Attributes["ApproximateNumberOfMessagesDelayed"],
				"ApproximateNumberOfMessagesDelayed")
		})
	}
}

func TestGetQueueAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: []string{"All"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.Attributes["VisibilityTimeout"])
	assert.NotEmpty(t, out.Attributes["QueueArn"])
	assert.Contains(t, out.Attributes["QueueArn"], "my-queue")
}

func TestSetQueueAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	require.NoError(t, err)

	out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: []string{"VisibilityTimeout"},
	})
	require.NoError(t, err)
	assert.Equal(t, "60", out.Attributes["VisibilityTimeout"])
}

// TestQueueAttributeName_Allowlist verifies CreateQueue rejects attribute
// names outside AWS's 21-value QueueAttributeName enum (aws-sdk-go-v2/
// service/sqs@v1.46.4 types/enums.go:62-83) instead of silently storing them.
func TestQueueAttributeName_Allowlist(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		key  string
		val  string
		ok   bool
	}{
		{"known_visibility_timeout", "VisibilityTimeout", "30", true},
		{"known_fifo_throughput_limit", "FifoThroughputLimit", "perQueue", true},
		{"unknown_attribute", "NotARealAttribute", "x", false},
		{"typo_of_known_attribute", "VisibilityTimeOut", "30", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "allowlist-" + tc.name,
				Endpoint:   testEndpoint,
				Attributes: map[string]string{tc.key: tc.val},
			})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, sqs.ErrInvalidAttributeName)
			}
		})
	}
}

func TestSetQueueAttributes_UnknownAttributeName(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "unknown-attr-q")

	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"NotARealAttribute": "x"},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidAttributeName)
}

// TestFifoThroughputLimit_DeduplicationScopePairing verifies AWS's documented
// rule ("The perMessageGroupId value is allowed only when the value for
// DeduplicationScope is messageGroup", aws-sdk-go-v2/service/sqs@v1.46.4
// api_op_SetQueueAttributes.go:179-180) is enforced against the *effective*
// attribute state, not just the fields present in a single SetQueueAttributes
// call.
func TestFifoThroughputLimit_DeduplicationScopePairing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		existingScope string
		setScope      string
		wantErr       bool
	}{
		{"explicit_messagegroup_same_call", "", "messageGroup", false},
		{"explicit_queue_same_call", "", "queue", true},
		{"default_scope_unset", "", "", false},
		{"existing_queue_scope_not_overridden", "queue", "", true},
		{"existing_messagegroup_scope_not_overridden", "messageGroup", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			createAttrs := map[string]string{}
			if tc.existingScope != "" {
				createAttrs["DeduplicationScope"] = tc.existingScope
			}

			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "pairing-" + tc.name + ".fifo",
				Endpoint:   testEndpoint,
				Attributes: createAttrs,
			})
			require.NoError(t, err)

			setAttrs := map[string]string{"FifoThroughputLimit": "perMessageGroupId"}
			if tc.setScope != "" {
				setAttrs["DeduplicationScope"] = tc.setScope
			}

			err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
				QueueURL:   out.QueueURL,
				Attributes: setAttrs,
			})
			if tc.wantErr {
				require.ErrorIs(t, err, sqs.ErrInvalidAttribute)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateQueue_FifoThroughputLimit_InvalidPairing(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "create-pairing-invalid.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"FifoThroughputLimit": "perMessageGroupId",
			"DeduplicationScope":  "queue",
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidAttribute)
}

func TestCreateQueue_FifoThroughputLimit_ValidPairing(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "create-pairing-valid.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"FifoThroughputLimit": "perMessageGroupId",
			"DeduplicationScope":  "messageGroup",
		},
	})
	require.NoError(t, err)
}
