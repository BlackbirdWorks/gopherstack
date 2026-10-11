package azurequeue

import (
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/xmlappend"
)

const (
	listEnvelopeSize = 64
	messageEntrySize = 400
	decimalBase      = 10
)

// encodeMessagesList renders a QueueMessagesList; output is byte-identical to
// xml.Marshal of the equivalent queueMessagesList.
func encodeMessagesList(msgs []queueMessageXML) []byte {
	buf := make([]byte, 0, listEnvelopeSize+len(msgs)*messageEntrySize)
	buf = append(buf, `<QueueMessagesList>`...)

	for i := range msgs {
		m := &msgs[i]
		buf = append(buf, `<QueueMessage>`...)
		buf = xmlappend.Elem(buf, "MessageId", m.MessageID)
		buf = xmlappend.Elem(buf, "InsertionTime", m.InsertionTime)
		buf = xmlappend.Elem(buf, "ExpirationTime", m.ExpirationTime)

		if m.PopReceipt != "" {
			buf = xmlappend.Elem(buf, "PopReceipt", m.PopReceipt)
		}

		if m.TimeNextVisible != "" {
			buf = xmlappend.Elem(buf, "TimeNextVisible", m.TimeNextVisible)
		}

		if m.DequeueCount != nil {
			buf = append(buf, `<DequeueCount>`...)
			buf = strconv.AppendInt(buf, *m.DequeueCount, decimalBase)
			buf = append(buf, `</DequeueCount>`...)
		}

		if m.MessageText != "" {
			buf = xmlappend.Elem(buf, "MessageText", m.MessageText)
		}

		buf = append(buf, `</QueueMessage>`...)
	}

	return append(buf, `</QueueMessagesList>`...)
}
