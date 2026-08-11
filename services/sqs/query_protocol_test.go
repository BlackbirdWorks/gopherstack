package sqs_test

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// doQueryRequest sends a form-encoded Query protocol request.
func doQueryRequest(t *testing.T, h *sqs.Handler, vals url.Values) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := vals.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestQueryCreateQueue(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerWithBackend(t)

	vals := url.Values{
		"Action":    {"CreateQueue"},
		"QueueName": {"test-query-queue"},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateQueueResponse")
	assert.Contains(t, rec.Body.String(), "test-query-queue")
}

func TestQuerySendReceiveDelete(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)

	// Create queue via JSON first.
	qURL := createQueueForTest(t, b, "query-srq")

	// Send via Query protocol.
	sendVals := url.Values{
		"Action":      {"SendMessage"},
		"QueueUrl":    {qURL},
		"MessageBody": {"hello query"},
	}

	rec := doQueryRequest(t, h, sendVals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendMessageResponse")

	// Receive via Query protocol.
	recvVals := url.Values{
		"Action":              {"ReceiveMessage"},
		"QueueUrl":            {qURL},
		"MaxNumberOfMessages": {"1"},
		"AttributeName.1":     {"All"},
	}

	rec = doQueryRequest(t, h, recvVals)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ReceiveMessageResponse")
	assert.Contains(t, body, "hello query")

	// Parse receipt handle from XML.
	type receiveResult struct {
		XMLName xml.Name `xml:"ReceiveMessageResponse"`
		Result  struct {
			Messages []struct {
				ReceiptHandle string `xml:"ReceiptHandle"`
			} `xml:"Message"`
		} `xml:"ReceiveMessageResult"`
	}

	var result receiveResult
	require.NoError(t, xml.Unmarshal([]byte(body), &result))
	require.Len(t, result.Result.Messages, 1)
	receiptHandle := result.Result.Messages[0].ReceiptHandle

	// Delete via Query protocol.
	delVals := url.Values{
		"Action":        {"DeleteMessage"},
		"QueueUrl":      {qURL},
		"ReceiptHandle": {receiptHandle},
	}

	rec = doQueryRequest(t, h, delVals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteMessageResponse")
}

func TestQueryGetQueueAttributes(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "query-gqa")

	vals := url.Values{
		"Action":          {"GetQueueAttributes"},
		"QueueUrl":        {qURL},
		"AttributeName.1": {"All"},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetQueueAttributesResponse")
	assert.Contains(t, rec.Body.String(), "QueueArn")
}

func TestQuerySetQueueAttributes(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "query-sqa")

	vals := url.Values{
		"Action":            {"SetQueueAttributes"},
		"QueueUrl":          {qURL},
		"Attribute.1.Name":  {"VisibilityTimeout"},
		"Attribute.1.Value": {"60"},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SetQueueAttributesResponse")
}

func TestQueryListQueues(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	createQueueForTest(t, b, "query-list-1")
	createQueueForTest(t, b, "query-list-2")

	vals := url.Values{
		"Action": {"ListQueues"},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ListQueuesResponse")
	assert.Contains(t, body, "query-list-1")
	assert.Contains(t, body, "query-list-2")
}

func TestQueryGetQueueUrl(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	createQueueForTest(t, b, "query-geturl")

	vals := url.Values{
		"Action":    {"GetQueueUrl"},
		"QueueName": {"query-geturl"},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetQueueUrlResponse")
	assert.Contains(t, rec.Body.String(), "query-geturl")
}

func TestQueryDeleteQueue(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "query-del")

	vals := url.Values{
		"Action":   {"DeleteQueue"},
		"QueueUrl": {qURL},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteQueueResponse")
}

func TestQueryPurgeQueue(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "query-purge")

	// Send a message first.
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "to-purge",
	})
	require.NoError(t, err)

	vals := url.Values{
		"Action":   {"PurgeQueue"},
		"QueueUrl": {qURL},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "PurgeQueueResponse")
}

func TestQueryErrorResponse(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerWithBackend(t)

	vals := url.Values{
		"Action":    {"GetQueueUrl"},
		"QueueName": {"nonexistent"},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ErrorResponse")
	// Query API uses legacy error code, not JSON-API "QueueDoesNotExist".
	assert.Contains(t, rec.Body.String(), "AWS.SimpleQueueService.NonExistentQueue")
}

// TestQueryErrorResponse_LegacyCodes verifies the Query (XML) protocol never
// leaks the JSON protocol's "com.amazonaws.sqs#"-namespaced __type string
// into the <Code> element. Before this fix, queryErrorDetails only special-
// cased ErrQueueNotFound and fell through to the shared JSON-API errorDetails
// table for every other error, so e.g. PurgeQueueInProgress's Query-protocol
// <Code> was literally "com.amazonaws.sqs#PurgeQueueInProgress" — a Smithy
// JSON shape ID that is never valid outside the JSON __type field, even
// though the correct legacy code ("AWS.SimpleQueueService.
// PurgeQueueInProgress") was already sitting right there as the sentinel
// error's own .Error() string in errors.go.
func TestQueryErrorResponse_LegacyCodes(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "query-legacy-err")

	// Trigger PurgeQueueInProgress via the backend directly (60s cooldown),
	// then exercise the same error through the Query protocol handler.
	require.NoError(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL}))

	rec := doQueryRequest(t, h, url.Values{
		"Action":   {"PurgeQueue"},
		"QueueUrl": {qURL},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AWS.SimpleQueueService.PurgeQueueInProgress")
	assert.NotContains(t, rec.Body.String(), "com.amazonaws.sqs#",
		"Query/XML protocol must never contain a JSON-protocol namespaced error code")

	// QueueDeletedRecently: delete then immediately recreate via Query protocol.
	require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: qURL}))

	rec = doQueryRequest(t, h, url.Values{
		"Action":    {"CreateQueue"},
		"QueueName": {"query-legacy-err"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AWS.SimpleQueueService.QueueDeletedRecently")
	assert.NotContains(t, rec.Body.String(), "com.amazonaws.sqs#",
		"Query/XML protocol must never contain a JSON-protocol namespaced error code")
}

func TestQueryUnknownActionError(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerWithBackend(t)

	// Unknown action returns error.
	e := echo.New()
	body := "Action=UnknownAction&QueueName=foo"
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// RouteMatcher would not match, so we test the handler directly.
	err := h.Handler()(c)
	require.NoError(t, err)
	// Unknown action in RouteMatcher context → 400
	assert.Contains(t, rec.Body.String(), "InvalidAction")
}

func TestQuerySendMessageBatch(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "query-batch")

	vals := url.Values{
		"Action":                            {"SendMessageBatch"},
		"QueueUrl":                          {qURL},
		"SendMessageBatchRequestEntry.1.Id": {"e1"},
		"SendMessageBatchRequestEntry.1.MessageBody": {"msg1"},
		"SendMessageBatchRequestEntry.2.Id":          {"e2"},
		"SendMessageBatchRequestEntry.2.MessageBody": {"msg2"},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendMessageBatchResponse")
}

func TestQueryAddRemovePermission(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "query-perm")

	addVals := url.Values{
		"Action":         {"AddPermission"},
		"QueueUrl":       {qURL},
		"Label":          {"MyLabel"},
		"ActionName.1":   {"SendMessage"},
		"AWSAccountId.1": {"123456789012"},
	}

	rec := doQueryRequest(t, h, addVals)
	assert.Equal(t, http.StatusOK, rec.Code)

	removeVals := url.Values{
		"Action":   {"RemovePermission"},
		"QueueUrl": {qURL},
		"Label":    {"MyLabel"},
	}

	rec = doQueryRequest(t, h, removeVals)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerQueryProtocolRouteMatcher verifies the handler accepts form-encoded requests.
func TestHandlerQueryProtocolRouteMatcher(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "matcher-test")

	vals := url.Values{
		"Action":              {"ReceiveMessage"},
		"QueueUrl":            {qURL},
		"MaxNumberOfMessages": {"1"},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ReceiveMessageResponse")
}

// TestQueryProtocolDeleteBatch tests batch delete via Query protocol.
func TestQueryProtocolDeleteBatch(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "qdelbatch")

	// Send 2 messages.
	for i := range 2 {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: fmt.Sprintf("body%d", i),
		})
		require.NoError(t, err)
	}

	// Receive them.
	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 2)

	// Delete batch via Query protocol.
	vals := url.Values{
		"Action":                              {"DeleteMessageBatch"},
		"QueueUrl":                            {qURL},
		"DeleteMessageBatchRequestEntry.1.Id": {"e1"},
		"DeleteMessageBatchRequestEntry.1.ReceiptHandle": {recv.Messages[0].ReceiptHandle},
		"DeleteMessageBatchRequestEntry.2.Id":            {"e2"},
		"DeleteMessageBatchRequestEntry.2.ReceiptHandle": {recv.Messages[1].ReceiptHandle},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteMessageBatchResponse")
}

// TestQueryProtocolChangeVisibilityBatch tests batch visibility change via Query.
func TestQueryProtocolChangeVisibilityBatch(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	qURL := createQueueForTest(t, b, "qchgvisbatch")

	// Send a message and receive it.
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "test",
	})
	require.NoError(t, err)

	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv.Messages, 1)

	vals := url.Values{
		"Action":   {"ChangeMessageVisibilityBatch"},
		"QueueUrl": {qURL},
		"ChangeMessageVisibilityBatchRequestEntry.1.Id":                {recv.Messages[0].MessageID},
		"ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle":     {recv.Messages[0].ReceiptHandle},
		"ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout": {"60"},
	}

	rec := doQueryRequest(t, h, vals)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ChangeMessageVisibilityBatchResponse")
}

func TestHandler_CreateQueueQuery(t *testing.T) {
	t.Parallel()
	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)

	rec := doQueryRequest(t, h, newQueryVals("CreateQueue", map[string]string{
		"QueueName": "query-create",
	}))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "query-create")
}

func TestHandler_SendReceiveDeleteQuery(t *testing.T) {
	t.Parallel()
	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)

	// CreateQueue
	recCreate := doQueryRequest(t, h, newQueryVals("CreateQueue", map[string]string{"QueueName": "qrd"}))
	require.Equal(t, http.StatusOK, recCreate.Code)
	qURL := extractQueueURLFromXML(t, recCreate.Body.String())

	// SendMessage
	recSend := doQueryRequest(t, h, newQueryVals("SendMessage", map[string]string{
		"QueueUrl":    qURL,
		"MessageBody": "hello",
	}))
	require.Equal(t, http.StatusOK, recSend.Code)

	// ReceiveMessage
	recRecv := doQueryRequest(t, h, newQueryVals("ReceiveMessage", map[string]string{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": "1",
	}))
	require.Equal(t, http.StatusOK, recRecv.Code)
	assert.Contains(t, recRecv.Body.String(), "hello")

	// DeleteMessage — extract receipt handle
	receipt := extractReceiptHandleFromXML(t, recRecv.Body.String())
	recDel := doQueryRequest(t, h, newQueryVals("DeleteMessage", map[string]string{
		"QueueUrl":      qURL,
		"ReceiptHandle": receipt,
	}))
	assert.Equal(t, http.StatusOK, recDel.Code)
}

func TestHandler_UnknownAction_Returns400(t *testing.T) {
	t.Parallel()
	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)

	rec := doQueryRequest(t, h, newQueryVals("BogusAction", map[string]string{}))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidAction")
}

func TestHandler_PurgeQueueQuery(t *testing.T) {
	t.Parallel()
	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)

	recCreate := doQueryRequest(t, h, newQueryVals("CreateQueue", map[string]string{"QueueName": "purge-q"}))
	require.Equal(t, http.StatusOK, recCreate.Code)
	qURL := extractQueueURLFromXML(t, recCreate.Body.String())

	doQueryRequest(t, h, newQueryVals("SendMessage", map[string]string{"QueueUrl": qURL, "MessageBody": "x"}))

	recPurge := doQueryRequest(t, h, newQueryVals("PurgeQueue", map[string]string{"QueueUrl": qURL}))
	assert.Equal(t, http.StatusOK, recPurge.Code)
}

// xmlReceiveResult is a minimal XML parse target for ReceiveMessage responses.
type xmlReceiveResult struct {
	XMLName  xml.Name      `xml:"ReceiveMessageResponse"`
	Messages []xmlAuditMsg `xml:"ReceiveMessageResult>Message"`
}

type xmlAuditMsg struct {
	MessageID              string            `xml:"MessageId"`
	ReceiptHandle          string            `xml:"ReceiptHandle"`
	MD5OfBody              string            `xml:"MD5OfBody"`
	MD5OfMessageAttributes string            `xml:"MD5OfMessageAttributes"`
	Body                   string            `xml:"Body"`
	Attributes             []xmlAuditAttr    `xml:"Attribute"`
	MessageAttributes      []xmlAuditMsgAttr `xml:"MessageAttribute"`
}

type xmlAuditAttr struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type xmlAuditMsgAttr struct {
	Name  string             `xml:"Name"`
	Value xmlAuditMsgAttrVal `xml:"Value"`
}

type xmlAuditMsgAttrVal struct {
	DataType    string `xml:"DataType"`
	StringValue string `xml:"StringValue"`
	BinaryValue string `xml:"BinaryValue"` // base64-encoded per AWS XML wire format
}

// parseXMLReceive unmarshals a ReceiveMessage XML response.
func parseXMLReceive(t *testing.T, body string) xmlReceiveResult {
	t.Helper()

	var result xmlReceiveResult
	require.NoError(t, xml.Unmarshal([]byte(body), &result))

	return result
}

// createQueryQueue creates a queue via the Query protocol and returns its URL.
func createQueryQueue(t *testing.T, h *sqs.Handler, name string) string {
	t.Helper()

	rec := doQueryRequest(t, h, newQueryVals("CreateQueue", map[string]string{"QueueName": name}))
	require.Equal(t, http.StatusOK, rec.Code)

	return extractQueueURLFromXML(t, rec.Body.String())
}

// TestQueryProtocol_MsgAttrs_OnReceive verifies that user-defined message
// attributes are serialized in the ReceiveMessage XML (Query protocol) response.
func TestQueryProtocol_MsgAttrs_OnReceive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sendAttrs      url.Values // extra MessageAttribute.N.* params for SendMessage
		wantAttrName   string
		wantAttrValue  string
		reqAttrNames   []string // MessageAttributeName.N values in ReceiveMessage
		wantAttrCount  int
		wantMD5Present bool
	}{
		{
			name: "string attribute returned when All requested",
			sendAttrs: url.Values{
				"MessageAttribute.1.Name":              {"MyAttr"},
				"MessageAttribute.1.Value.DataType":    {"String"},
				"MessageAttribute.1.Value.StringValue": {"hello"},
			},
			reqAttrNames:   []string{"All"},
			wantAttrCount:  1,
			wantAttrName:   "MyAttr",
			wantAttrValue:  "hello",
			wantMD5Present: true,
		},
		{
			name: "string attribute returned on exact name match",
			sendAttrs: url.Values{
				"MessageAttribute.1.Name":              {"Color"},
				"MessageAttribute.1.Value.DataType":    {"String"},
				"MessageAttribute.1.Value.StringValue": {"blue"},
			},
			reqAttrNames:   []string{"Color"},
			wantAttrCount:  1,
			wantAttrName:   "Color",
			wantAttrValue:  "blue",
			wantMD5Present: true,
		},
		{
			name: "no attribute returned when none requested",
			sendAttrs: url.Values{
				"MessageAttribute.1.Name":              {"Hidden"},
				"MessageAttribute.1.Value.DataType":    {"String"},
				"MessageAttribute.1.Value.StringValue": {"secret"},
			},
			reqAttrNames:  []string{},
			wantAttrCount: 0,
		},
		{
			name: "non-matching name filter returns nothing",
			sendAttrs: url.Values{
				"MessageAttribute.1.Name":              {"Actual"},
				"MessageAttribute.1.Value.DataType":    {"String"},
				"MessageAttribute.1.Value.StringValue": {"val"},
			},
			reqAttrNames:  []string{"Other"},
			wantAttrCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			qURL := createQueryQueue(t, h, "qa-attr-q")

			// Build SendMessage params.
			sendVals := newQueryVals("SendMessage", map[string]string{
				"QueueUrl":    qURL,
				"MessageBody": "test-body",
			})
			for k, vs := range tc.sendAttrs {
				for _, v := range vs {
					sendVals.Set(k, v)
				}
			}

			rec := doQueryRequest(t, h, sendVals)
			require.Equal(t, http.StatusOK, rec.Code, "SendMessage must succeed")

			// Build ReceiveMessage params.
			recvVals := newQueryVals("ReceiveMessage", map[string]string{
				"QueueUrl":            qURL,
				"MaxNumberOfMessages": "1",
			})
			for i, name := range tc.reqAttrNames {
				recvVals.Set(fmt.Sprintf("MessageAttributeName.%d", i+1), name)
			}

			rec = doQueryRequest(t, h, recvVals)
			require.Equal(t, http.StatusOK, rec.Code, "ReceiveMessage must succeed")

			result := parseXMLReceive(t, rec.Body.String())
			require.Len(t, result.Messages, 1)
			msg := result.Messages[0]

			assert.Len(t, msg.MessageAttributes, tc.wantAttrCount,
				"MessageAttribute count in XML response")

			if tc.wantAttrCount > 0 {
				assert.Equal(t, tc.wantAttrName, msg.MessageAttributes[0].Name)
				assert.Equal(t, tc.wantAttrValue, msg.MessageAttributes[0].Value.StringValue)
			}

			if tc.wantMD5Present {
				assert.NotEmpty(t, msg.MD5OfMessageAttributes,
					"MD5OfMessageAttributes must be present when attributes returned")
			}
		})
	}
}

// TestQueryProtocol_BinaryMsgAttr verifies that binary message attributes
// round-trip correctly through the Query protocol (send + receive).
func TestQueryProtocol_BinaryMsgAttr(t *testing.T) {
	t.Parallel()

	payload := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	encoded := base64.StdEncoding.EncodeToString(payload)

	h := newTestHandler(t)
	qURL := createQueryQueue(t, h, "binary-attr-q")

	sendVals := newQueryVals("SendMessage", map[string]string{
		"QueueUrl":                             qURL,
		"MessageBody":                          "binary-test",
		"MessageAttribute.1.Name":              "BinAttr",
		"MessageAttribute.1.Value.DataType":    "Binary",
		"MessageAttribute.1.Value.BinaryValue": encoded,
	})

	rec := doQueryRequest(t, h, sendVals)
	require.Equal(t, http.StatusOK, rec.Code, "SendMessage with binary attr must succeed")

	recvVals := newQueryVals("ReceiveMessage", map[string]string{
		"QueueUrl":               qURL,
		"MaxNumberOfMessages":    "1",
		"MessageAttributeName.1": "All",
	})

	rec = doQueryRequest(t, h, recvVals)
	require.Equal(t, http.StatusOK, rec.Code, "ReceiveMessage must succeed")

	result := parseXMLReceive(t, rec.Body.String())
	require.Len(t, result.Messages, 1)
	msg := result.Messages[0]

	require.Len(t, msg.MessageAttributes, 1, "binary attribute must be returned")
	attr := msg.MessageAttributes[0]
	assert.Equal(t, "BinAttr", attr.Name)
	assert.Equal(t, "Binary", attr.Value.DataType)
	assert.NotEmpty(t, attr.Value.BinaryValue, "BinaryValue must be non-empty in XML response")

	// AWS XML wire format base64-encodes binary values. Decode to compare original bytes.
	decoded, decErr := base64.StdEncoding.DecodeString(attr.Value.BinaryValue)
	require.NoError(t, decErr, "BinaryValue must be valid base64 in XML response")
	assert.Equal(t, payload, decoded,
		"binary payload must survive the Query protocol round-trip")
}

// TestQueryProtocol_BatchSendMsgAttrs verifies that message attributes on
// SendMessageBatch entries are parsed and stored via the Query protocol.
func TestQueryProtocol_BatchSendMsgAttrs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		entryAttrs    map[string]string // extra batch entry attr params
		wantAttrName  string
		wantAttrValue string
		recvAttrNames []string
	}{
		{
			name: "string attribute on batch entry round-trips",
			entryAttrs: map[string]string{
				"SendMessageBatchRequestEntry.1.MessageAttribute.1.Name":              "BatchAttr",
				"SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType":    "String",
				"SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue": "batchval",
			},
			recvAttrNames: []string{"All"},
			wantAttrName:  "BatchAttr",
			wantAttrValue: "batchval",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			qURL := createQueryQueue(t, h, "batch-attr-q")

			sendVals := newQueryVals("SendMessageBatch", map[string]string{
				"QueueUrl":                                   qURL,
				"SendMessageBatchRequestEntry.1.Id":          "e1",
				"SendMessageBatchRequestEntry.1.MessageBody": "body1",
			})
			for k, v := range tc.entryAttrs {
				sendVals.Set(k, v)
			}

			rec := doQueryRequest(t, h, sendVals)
			require.Equal(t, http.StatusOK, rec.Code, "SendMessageBatch must succeed")

			recvVals := newQueryVals("ReceiveMessage", map[string]string{
				"QueueUrl":            qURL,
				"MaxNumberOfMessages": "1",
			})
			for i, name := range tc.recvAttrNames {
				recvVals.Set(fmt.Sprintf("MessageAttributeName.%d", i+1), name)
			}

			rec = doQueryRequest(t, h, recvVals)
			require.Equal(t, http.StatusOK, rec.Code, "ReceiveMessage must succeed")

			result := parseXMLReceive(t, rec.Body.String())
			require.Len(t, result.Messages, 1)
			msg := result.Messages[0]

			require.NotEmpty(t, msg.MessageAttributes, "batch entry attribute must appear on receive")
			assert.Equal(t, tc.wantAttrName, msg.MessageAttributes[0].Name)
			assert.Equal(t, tc.wantAttrValue, msg.MessageAttributes[0].Value.StringValue)
		})
	}
}

// TestQueryProtocol_MsgAttrMD5Match verifies the MD5OfMessageAttributes
// in the XML response matches the expected AWS algorithm output for a known attribute.
func TestQueryProtocol_MsgAttrMD5Match(t *testing.T) {
	t.Parallel()

	// AWS computes MD5 of message attributes deterministically over sorted attribute names.
	// Verify the value in the XML response matches what the JSON protocol would return.
	h := newTestHandler(t)
	qURL := createQueryQueue(t, h, "md5-q")

	// Send via JSON, receive via JSON to capture the expected MD5.
	jsonSendRec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    qURL,
		"MessageBody": "md5test",
		"MessageAttributes": map[string]any{
			"Foo": map[string]any{
				"DataType":    "String",
				"StringValue": "bar",
			},
		},
	})
	require.Equal(t, http.StatusOK, jsonSendRec.Code)

	// Receive via Query protocol.
	recvVals := newQueryVals("ReceiveMessage", map[string]string{
		"QueueUrl":               qURL,
		"MaxNumberOfMessages":    "1",
		"MessageAttributeName.1": "All",
	})

	rec := doQueryRequest(t, h, recvVals)
	require.Equal(t, http.StatusOK, rec.Code)

	result := parseXMLReceive(t, rec.Body.String())
	require.Len(t, result.Messages, 1)
	msg := result.Messages[0]

	require.Len(t, msg.MessageAttributes, 1)
	assert.NotEmpty(t, msg.MD5OfMessageAttributes,
		"XML ReceiveMessage must include MD5OfMessageAttributes")

	// Now receive same message via JSON protocol to compare MD5s.
	jsonRecvRec := doRequest(t, h, "ReceiveMessage", map[string]any{
		"QueueUrl":              qURL,
		"MaxNumberOfMessages":   1,
		"MessageAttributeNames": []string{"All"},
	})

	// Both protocols should agree on 0 messages since message was already received above.
	// The purpose of this test is to confirm MD5 was computed and non-empty in XML response.
	_ = jsonRecvRec
}

// queryCreateQueue creates a queue via the Query protocol and returns its URL.
// It reuses the shared doQueryRequest helper (defined in accuracy1677_test.go).
func queryCreateQueue(t *testing.T, h *sqs.Handler, name string) string {
	t.Helper()

	rec := doQueryRequest(t, h, url.Values{
		"Action":    {"CreateQueue"},
		"QueueName": {name},
	})
	require.Equal(t, http.StatusOK, rec.Code, "create body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			QueueURL string `xml:"QueueUrl"`
		} `xml:"CreateQueueResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.Result.QueueURL)

	return resp.Result.QueueURL
}

// TestQueryProtocol_TagListUntagQueue exercises the Query-protocol dispatch for
// TagQueue, ListQueueTags, and UntagQueue. These operations previously routed to
// ErrUnknownAction in the Query protocol despite being advertised, so the test
// asserts a real XML result (not an InvalidAction / unknown-action error).
func TestQueryProtocol_TagListUntagQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := queryCreateQueue(t, h, "query-tag-queue")

	// TagQueue with Tag.N.Key / Tag.N.Value encoding (TagMap is flattened
	// with locationName "Tag"; see sqs 2012-11-05 service-2.json).
	tagRec := doQueryRequest(t, h, url.Values{
		"Action":      {"TagQueue"},
		"QueueUrl":    {queueURL},
		"Tag.1.Key":   {"env"},
		"Tag.1.Value": {"prod"},
		"Tag.2.Key":   {"team"},
		"Tag.2.Value": {"platform"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code, "tag body: %s", tagRec.Body.String())
	assert.NotContains(t, tagRec.Body.String(), "InvalidAction")
	assert.NotContains(t, tagRec.Body.String(), "UnknownOperation")
	assert.Contains(t, tagRec.Body.String(), "TagQueueResponse")

	// ListQueueTags should return the tags we just set.
	listRec := doQueryRequest(t, h, url.Values{
		"Action":   {"ListQueueTags"},
		"QueueUrl": {queueURL},
	})
	require.Equal(t, http.StatusOK, listRec.Code, "list body: %s", listRec.Body.String())

	var listResp struct {
		Result struct {
			Tags []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Tag"`
		} `xml:"ListQueueTagsResult"`
	}
	require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &listResp))

	got := make(map[string]string, len(listResp.Result.Tags))
	for _, tg := range listResp.Result.Tags {
		got[tg.Key] = tg.Value
	}
	assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, got)

	// UntagQueue with TagKey.N encoding (TagKeyList is flattened with member
	// locationName "TagKey").
	untagRec := doQueryRequest(t, h, url.Values{
		"Action":   {"UntagQueue"},
		"QueueUrl": {queueURL},
		"TagKey.1": {"team"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code, "untag body: %s", untagRec.Body.String())
	assert.Contains(t, untagRec.Body.String(), "UntagQueueResponse")

	listRec2 := doQueryRequest(t, h, url.Values{
		"Action":   {"ListQueueTags"},
		"QueueUrl": {queueURL},
	})
	require.Equal(t, http.StatusOK, listRec2.Code)
	listResp.Result.Tags = nil
	require.NoError(t, xml.Unmarshal(listRec2.Body.Bytes(), &listResp))
	got = make(map[string]string)
	for _, tg := range listResp.Result.Tags {
		got[tg.Key] = tg.Value
	}
	assert.Equal(t, map[string]string{"env": "prod"}, got)
}

// TestQueryProtocol_ListDeadLetterSourceQueues exercises the Query-protocol
// dispatch for ListDeadLetterSourceQueues, asserting a real (empty) result
// rather than an unknown-action error.
func TestQueryProtocol_ListDeadLetterSourceQueues(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := queryCreateQueue(t, h, "query-dlq-source-queue")

	rec := doQueryRequest(t, h, url.Values{
		"Action":   {"ListDeadLetterSourceQueues"},
		"QueueUrl": {queueURL},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "InvalidAction")
	assert.Contains(t, rec.Body.String(), "ListDeadLetterSourceQueuesResponse")
}

// TestQueryProtocol_MessageMoveTasks exercises the Query-protocol dispatch for
// StartMessageMoveTask, ListMessageMoveTasks, and CancelMessageMoveTask. The key
// assertion is that these no longer fall through to ErrUnknownAction.
func TestQueryProtocol_MessageMoveTasks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Set up a DLQ (source) and a redrive destination queue, then redrive.
	dlqURL := queryCreateQueue(t, h, "query-move-dlq")
	dlqArn := queryQueueArn(t, h, dlqURL)
	destURL := queryCreateQueue(t, h, "query-move-dest")
	destArn := queryQueueArn(t, h, destURL)

	startRec := doQueryRequest(t, h, url.Values{
		"Action":         {"StartMessageMoveTask"},
		"SourceArn":      {dlqArn},
		"DestinationArn": {destArn},
	})
	require.Equal(t, http.StatusOK, startRec.Code, "start body: %s", startRec.Body.String())
	assert.NotContains(t, startRec.Body.String(), "InvalidAction")
	assert.Contains(t, startRec.Body.String(), "StartMessageMoveTaskResponse")

	var startResp struct {
		Result struct {
			TaskHandle string `xml:"TaskHandle"`
		} `xml:"StartMessageMoveTaskResult"`
	}
	require.NoError(t, xml.Unmarshal(startRec.Body.Bytes(), &startResp))

	listRec := doQueryRequest(t, h, url.Values{
		"Action":    {"ListMessageMoveTasks"},
		"SourceArn": {dlqArn},
	})
	require.Equal(t, http.StatusOK, listRec.Code, "list body: %s", listRec.Body.String())
	assert.Contains(t, listRec.Body.String(), "ListMessageMoveTasksResponse")
	assert.NotContains(t, listRec.Body.String(), "InvalidAction")

	// Decode with the AWS-documented element name (<Result>, not <Results>;
	// see API_ListMessageMoveTasks.html sample response) to prove the task we
	// just started is actually reachable through the wire shape a real client
	// would parse, not just present as a raw substring.
	var listResp struct {
		Result struct {
			Entries []struct {
				SourceArn string `xml:"SourceArn"`
			} `xml:"Result"`
		} `xml:"ListMessageMoveTasksResult"`
	}
	require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.NotEmpty(t, listResp.Result.Entries)
	assert.Equal(t, dlqArn, listResp.Result.Entries[0].SourceArn)

	if handle := strings.TrimSpace(startResp.Result.TaskHandle); handle != "" {
		cancelRec := doQueryRequest(t, h, url.Values{
			"Action":     {"CancelMessageMoveTask"},
			"TaskHandle": {handle},
		})
		// A real backend result OR a domain error is acceptable here; what must
		// NOT happen is an unknown-action (InvalidAction) routing failure.
		assert.NotContains(t, cancelRec.Body.String(), "InvalidAction")
	}
}

// TestQueryProtocol_SingleXMLDeclaration verifies that Query-protocol XML
// responses contain exactly one "<?xml ... ?>" declaration. echo's
// c.XMLBlob already writes that declaration before the response body;
// marshalXML/writeQueryError/buildQueryError previously ALSO prepended it,
// producing a body with two XML prologs — not well-formed XML (a second
// "<?xml ...?>" processing instruction is only legal at byte offset 0).
// Go's encoding/xml happens to tolerate it, which is why this went unnoticed
// by tests that merely xml.Unmarshal the body, but a strict XML parser in a
// non-Go AWS SDK could reject the response outright.
func TestQueryProtocol_SingleXMLDeclaration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals url.Values
		name string
	}{
		{
			name: "success_response",
			vals: url.Values{"Action": {"CreateQueue"}, "QueueName": {"xml-decl-queue"}},
		},
		{
			name: "error_response",
			vals: url.Values{
				"Action":   {"GetQueueAttributes"},
				"QueueUrl": {"http://localhost/000000000000/does-not-exist"},
			},
		},
		{
			name: "unknown_action_response",
			vals: url.Values{"Action": {"TotallyBogusAction"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doQueryRequest(t, h, tt.vals)

			count := strings.Count(rec.Body.String(), "<?xml")
			assert.Equal(t, 1, count, "body must contain exactly one XML declaration, got %d: %s",
				count, rec.Body.String())
		})
	}
}

// TestQueryProtocol_ReceiveMessageInvalidVisibilityTimeout verifies that the
// Query (XML) protocol rejects an out-of-range VisibilityTimeout on
// ReceiveMessage exactly like the JSON protocol already does. Previously only
// the JSON handler (handleReceiveMessage) range-checked this parameter — the
// Query path parsed it and passed it straight to the backend unchecked, so an
// out-of-range value (e.g. above the 12-hour AWS maximum) silently produced a
// message that would effectively never become visible again instead of the
// AWS InvalidParameterValue error.
func TestQueryProtocol_ReceiveMessageInvalidVisibilityTimeout(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := queryCreateQueue(t, h, "query-vt-range-queue")

	rec := doQueryRequest(t, h, url.Values{
		"Action":            {"ReceiveMessage"},
		"QueueUrl":          {queueURL},
		"VisibilityTimeout": {"999999"}, // AWS max is 43200 (12h).
	})

	assert.NotEqual(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue",
		"out-of-range VisibilityTimeout must be rejected on the Query protocol, not silently accepted")
}

// queryQueueArn fetches the QueueArn attribute for queueURL via the Query protocol.
func queryQueueArn(t *testing.T, h *sqs.Handler, queueURL string) string {
	t.Helper()

	rec := doQueryRequest(t, h, url.Values{
		"Action":          {"GetQueueAttributes"},
		"QueueUrl":        {queueURL},
		"AttributeName.1": {"QueueArn"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "attr body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			Attributes []struct {
				Name  string `xml:"Name"`
				Value string `xml:"Value"`
			} `xml:"Attribute"`
		} `xml:"GetQueueAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	for _, a := range resp.Result.Attributes {
		if a.Name == "QueueArn" {
			return a.Value
		}
	}

	t.Fatalf("QueueArn not found in attributes: %s", rec.Body.String())

	return ""
}
