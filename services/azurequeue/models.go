package azurequeue

import (
	"encoding/xml"
	"time"
)

// QueueInfo is a read-only snapshot of a queue's metadata, returned by
// StorageBackend.ListQueues. It intentionally excludes the queue's message
// slice so callers cannot mutate backend state through it.
type QueueInfo struct {
	CreatedAt time.Time
	Name      string
}

// MessageInfo is a read-only snapshot of a message, returned by the
// StorageBackend message accessors. PopReceipt is empty for Peek Messages
// (peeking never assigns one -- see AZURE.md section 2), and Text is only
// populated for Get/Peek (Put/Update return no body, only metadata, mirroring
// real Azure Queue Storage).
type MessageInfo struct {
	InsertionTime   time.Time
	ExpirationTime  time.Time
	TimeNextVisible time.Time
	ID              string
	PopReceipt      string
	Text            string
	DequeueCount    int64
}

// storedMessage is the backend's internal representation of a queued
// message. Text is stored verbatim: the wire protocol (and every real SDK)
// base64-encodes message bodies by default, but this backend treats it as an
// opaque string and never decodes/re-encodes it (see AZURE.md/PARITY.md).
type storedMessage struct {
	InsertionTime   time.Time
	ExpirationTime  time.Time
	NextVisibleTime time.Time
	ID              string
	PopReceipt      string
	Text            string
	DequeueCount    int64
}

// info returns a read-only MessageInfo snapshot. includePopReceipt is false
// for Peek Messages (which never assigns/exposes one).
func (m *storedMessage) info(includePopReceipt bool) MessageInfo {
	mi := MessageInfo{
		ID:              m.ID,
		InsertionTime:   m.InsertionTime,
		ExpirationTime:  m.ExpirationTime,
		DequeueCount:    m.DequeueCount,
		Text:            m.Text,
		TimeNextVisible: m.NextVisibleTime,
	}
	if includePopReceipt {
		mi.PopReceipt = m.PopReceipt
	}

	return mi
}

// isVisible reports whether m is visible (not currently hidden by an
// in-flight dequeue's visibility timeout) as of now.
func (m *storedMessage) isVisible(now time.Time) bool {
	return !m.NextVisibleTime.After(now)
}

// isExpired reports whether m has exceeded its message TTL as of now, swept
// by Janitor.
func (m *storedMessage) isExpired(now time.Time) bool {
	return now.After(m.ExpirationTime)
}

// storedQueue is the backend's internal representation of a queue. Messages
// is kept in insertion order so Get/Peek Messages serve the oldest visible
// message first, matching Azure Queue Storage's approximately-FIFO ordering.
type storedQueue struct {
	CreatedAt time.Time
	Name      string
	Messages  []*storedMessage
}

// --- Azure Queue REST XML wire shapes ---
//
// These mirror the wire shape of Azure Storage's "EnumerationResults",
// "QueueMessagesList", and "Error" bodies closely enough for
// azure-sdk-for-go (and Azurite-targeting SDKs generally) to parse
// successfully. Timestamps use net/http.TimeFormat (RFC1123-with-GMT),
// matching services/azureblob's Last-Modified formatting and the shape real
// Azure Queue Storage timestamps take.

// azureError is the standard Azure Storage REST error body.
type azureError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// enumerationResults is the top-level shape returned by List Queues.
type enumerationResults struct {
	XMLName         xml.Name   `xml:"EnumerationResults"`
	ServiceEndpoint string     `xml:"ServiceEndpoint,attr"`
	Queues          *queueList `xml:"Queues"`
	NextMarker      string     `xml:"NextMarker"`
}

type queueList struct {
	Queue []queueEntry `xml:"Queue"`
}

type queueEntry struct {
	Name string `xml:"Name"`
}

// queueMessagesList is the top-level shape returned by Put/Get/Peek
// Messages -- always a list, even for Put Message's single-element result.
type queueMessagesList struct {
	XMLName  xml.Name          `xml:"QueueMessagesList"`
	Messages []queueMessageXML `xml:"QueueMessage"`
}

// queueMessageXML is one <QueueMessage> entry. Fields are populated
// selectively per operation (see Handler.messageXMLFor): PopReceipt/
// TimeNextVisible are omitted for Peek Messages, MessageText is omitted for
// Put Message's response (matching real Azure Queue Storage, which never
// echoes the body back on Put).
type queueMessageXML struct {
	MessageID       string `xml:"MessageId"`
	InsertionTime   string `xml:"InsertionTime"`
	ExpirationTime  string `xml:"ExpirationTime"`
	PopReceipt      string `xml:"PopReceipt,omitempty"`
	TimeNextVisible string `xml:"TimeNextVisible,omitempty"`
	DequeueCount    *int64 `xml:"DequeueCount,omitempty"`
	MessageText     string `xml:"MessageText,omitempty"`
}

// putMessageBody is the request body shape for Put Message:
// <QueueMessage><MessageText>...</MessageText></QueueMessage>.
type putMessageBody struct {
	XMLName     xml.Name `xml:"QueueMessage"`
	MessageText string   `xml:"MessageText"`
}
