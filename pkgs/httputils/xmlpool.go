package httputils

import (
	"bytes"
	"encoding/xml"
	"sync"
)

// pooledXMLEncoder pairs an encoder with the buffer it writes to so neither
// xml.NewEncoder's 4 KB bufio writer nor the buffer is reallocated per response.
type pooledXMLEncoder struct {
	enc *xml.Encoder
	buf bytes.Buffer
}

//nolint:gochecknoglobals // process-wide pool, same as bufferPool
var xmlEncoderPool = sync.Pool{
	New: func() any {
		p := &pooledXMLEncoder{}
		p.enc = xml.NewEncoder(&p.buf)

		return p
	},
}

// withXML encodes payload behind the XML declaration and passes the bytes to use,
// which must not retain them. A failed encode drops the encoder: its state is suspect.
func withXML(payload any, use func(body []byte) error) error {
	p, ok := xmlEncoderPool.Get().(*pooledXMLEncoder)
	if !ok {
		p = &pooledXMLEncoder{}
		p.enc = xml.NewEncoder(&p.buf)
	}

	p.buf.Reset()
	p.buf.WriteString(xml.Header)

	if err := p.enc.Encode(payload); err != nil {
		return err
	}

	err := use(p.buf.Bytes())

	if p.buf.Cap() <= maxPooledBufferSize {
		xmlEncoderPool.Put(p)
	}

	return err
}

// MarshalXML returns payload encoded as XML behind the XML declaration, in a fresh slice.
func MarshalXML(payload any) ([]byte, error) {
	var out []byte

	err := withXML(payload, func(body []byte) error {
		out = bytes.Clone(body)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}
