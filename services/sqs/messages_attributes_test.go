package sqs_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestValidStringAttribute(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "attr-valid")

	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Name": {DataType: "String", StringValue: "Alice"},
		},
	})
	require.NoError(t, err)
}

func TestValidNumberAttribute(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "attr-num")

	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Count": {DataType: "Number", StringValue: "42"},
		},
	})
	require.NoError(t, err)
}

func TestValidBinaryAttribute(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "attr-bin")

	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Data": {DataType: "Binary", BinaryValue: []byte("hello")},
		},
	})
	require.NoError(t, err)
}

func TestInvalidDataTypeRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "attr-bad-type")

	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Bad": {DataType: "InvalidType", StringValue: "val"},
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestStringWithNoStringValueRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "attr-bad-val")

	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Name": {DataType: "String"}, // missing StringValue
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestBinaryWithNoBinaryValueRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "attr-bad-bin")

	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Data": {DataType: "Binary"}, // missing BinaryValue
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestCustomSubtypeValid(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "attr-custom")

	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Custom": {DataType: "String.json", StringValue: `{"key":"value"}`},
		},
	})
	require.NoError(t, err)
}

func TestMessageAttributes_MaxTen(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-max")

	// Exactly 10 — allowed
	attrs10 := make(map[string]sqs.MessageAttributeValue, 10)
	for i := range 10 {
		attrs10[fmt.Sprintf("attr%d", i)] = sqs.MessageAttributeValue{
			DataType:    "String",
			StringValue: "v",
		}
	}
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:          qURL,
		MessageBody:       "body",
		MessageAttributes: attrs10,
	})
	require.NoError(t, err)

	// 11 — rejected
	attrs11 := make(map[string]sqs.MessageAttributeValue, 11)
	for i := range 11 {
		attrs11[fmt.Sprintf("attr%d", i)] = sqs.MessageAttributeValue{
			DataType:    "String",
			StringValue: "v",
		}
	}
	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:          qURL,
		MessageBody:       "body",
		MessageAttributes: attrs11,
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestMessageAttributes_ReservedNames_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-reserved")

	reserved := []string{
		"AWS.SomeAttribute",
		"aws.other",
		"Amazon.Trace",
		"amazon.x",
	}

	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    qURL,
				MessageBody: "body",
				MessageAttributes: map[string]sqs.MessageAttributeValue{
					name: {DataType: "String", StringValue: "v"},
				},
			})
			require.ErrorIs(
				t,
				err,
				sqs.ErrInvalidMessageAttributeValue,
				"expected rejection for reserved name: %s",
				name,
			)
		})
	}
}

func TestMessageAttributes_ValidCustomNames(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-valid")

	valid := []string{"MyAttr", "my-attr", "my.attr.subtype", "MyAttr123"}
	for _, name := range valid {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    qURL,
				MessageBody: "body",
				MessageAttributes: map[string]sqs.MessageAttributeValue{
					name: {DataType: "String", StringValue: "v"},
				},
			})
			require.NoError(t, err, "valid name should not be rejected: %s", name)
		})
	}
}

func TestMessageAttributes_StringType(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-string")
	b2send(t, b, qURL, "noop") // put something for flush

	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyStr": {DataType: "String", StringValue: "hello"},
		},
	})
	require.NoError(t, err)
}

func TestMessageAttributes_NumberType(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-number")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyNum": {DataType: "Number", StringValue: "42"},
		},
	})
	require.NoError(t, err)
}

func TestMessageAttributes_BinaryType(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-binary")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyBin": {DataType: "Binary", BinaryValue: []byte{0x01, 0x02}},
		},
	})
	require.NoError(t, err)
}

func TestMessageAttributes_InvalidDataType_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-badtype")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Bad": {DataType: "InvalidType", StringValue: "v"},
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestMessageAttributes_StringMissingValue_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-nostrval")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Str": {DataType: "String" /* no StringValue */},
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestMessageAttributes_BinaryMissingValue_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-nobinval")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Bin": {DataType: "Binary" /* no BinaryValue */},
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestMessageAttributes_CustomSubtype_Valid(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-subtype")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyCustom": {DataType: "String.json", StringValue: `{"k":"v"}`},
		},
	})
	require.NoError(t, err)
}
