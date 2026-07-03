package s3

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// AWS "aws-chunked" streaming content-encoding constants. When a client PUTs an
// object (or uploads a part) with SigV4 streaming signatures, the wire body is
// framed as a sequence of length-prefixed chunks:
//
//	<hex-chunk-size>;chunk-signature=<64-hex>\r\n
//	<chunk-data>\r\n
//	...
//	0;chunk-signature=<64-hex>\r\n
//	[trailer-header:value\r\n]...
//	\r\n
//
// The real object bytes are the concatenation of the <chunk-data> segments; the
// chunk headers and per-chunk signatures MUST be stripped before storage, or the
// stored content (and its ETag) is corrupted. These are the x-amz-content-sha256
// sentinel values that select the chunked encoding.
const (
	streamingSignedPayload         = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingSignedPayloadTrailer  = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	streamingUnsignedTrailer       = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	awsChunkedContentEncoding      = "aws-chunked"
	decodedContentLengthHeaderName = "X-Amz-Decoded-Content-Length"
	contentSHA256HeaderName        = "X-Amz-Content-Sha256"
)

// errMalformedChunkedBody is returned when the aws-chunked framing cannot be
// parsed (bad chunk-size line, truncated chunk, missing CRLF terminator).
var errMalformedChunkedBody = errors.New("malformed aws-chunked encoded body")

// maxChunkHeaderLine caps the size of a single "size;chunk-signature=..." line
// so a hostile client can't force unbounded buffering while we look for the
// CRLF. A legitimate line is well under 200 bytes.
const maxChunkHeaderLine = 1024

// isAWSChunkedRequest reports whether r carries an aws-chunked / SigV4 streaming
// body that must be decoded before the payload is stored. It recognises the
// STREAMING-* x-amz-content-sha256 sentinels as well as the Content-Encoding:
// aws-chunked marker that older SDKs set.
func isAWSChunkedRequest(r *http.Request) bool {
	switch strings.ToUpper(r.Header.Get(contentSHA256HeaderName)) {
	case streamingSignedPayload, streamingSignedPayloadTrailer, streamingUnsignedTrailer:
		return true
	}

	for enc := range strings.SplitSeq(r.Header.Get("Content-Encoding"), ",") {
		if strings.EqualFold(strings.TrimSpace(enc), awsChunkedContentEncoding) {
			return true
		}
	}

	return false
}

// decodedContentLength returns the intended decoded object size from the
// x-amz-decoded-content-length header, and whether it was present and valid.
func decodedContentLength(r *http.Request) (int64, bool) {
	raw := r.Header.Get(decodedContentLengthHeaderName)
	if raw == "" {
		return 0, false
	}

	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}

	return n, true
}

// maybeDecodeChunkedBody wraps r.Body in a chunk-stripping reader when the
// request uses aws-chunked streaming encoding, and rewrites the request so
// downstream code (which reads r.Body and trusts Content-Length) sees only the
// real decoded payload. It is a no-op for ordinary requests.
//
// The returned request is r itself (mutated in place) so callers can keep using
// their existing reference; it is returned for call-site clarity.
func maybeDecodeChunkedBody(r *http.Request) *http.Request {
	if !isAWSChunkedRequest(r) || r.Body == nil {
		return r
	}

	r.Body = newChunkedReadCloser(r.Body)

	// After decoding, the effective payload length is the decoded length, not
	// the on-the-wire (chunk-framed) Content-Length. Correct both so ETag/size
	// computation downstream is accurate.
	if n, ok := decodedContentLength(r); ok {
		r.ContentLength = n
		r.Header.Set("Content-Length", strconv.FormatInt(n, 10))
	} else {
		// Unknown decoded length: force chunked/unknown so nothing truncates.
		r.ContentLength = -1
	}

	// The stored object must not advertise aws-chunked as its content-encoding;
	// that framing is a transport detail, not part of the object.
	r.Header.Del("Content-Encoding")

	return r
}

// chunkedReader strips aws-chunked framing from an underlying stream, yielding
// only the concatenated chunk payloads. Chunk signatures and trailer headers
// are parsed and discarded (we do not re-verify per-chunk signatures; the
// header-level SigV4 check, when enabled, already gates the request).
type chunkedReader struct {
	br        *bufio.Reader
	err       error // sticky terminal error
	remaining int64 // bytes left in the current chunk's data segment
	done      bool  // final zero-length chunk consumed
}

// chunkedReadCloser couples a chunkedReader with the underlying Closer so the
// original request body is still closed by the server.
type chunkedReadCloser struct {
	*chunkedReader
	closer io.Closer
}

func newChunkedReadCloser(body io.ReadCloser) io.ReadCloser {
	return &chunkedReadCloser{
		chunkedReader: &chunkedReader{br: bufio.NewReader(body)},
		closer:        body,
	}
}

func (c *chunkedReadCloser) Close() error { return c.closer.Close() }

// Read implements io.Reader over the decoded payload.
func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}

	if c.remaining == 0 {
		if err := c.advanceChunk(); err != nil {
			c.err = err

			return 0, err
		}
		if c.done {
			c.err = io.EOF

			return 0, io.EOF
		}
	}

	return c.readChunkData(p)
}

// readChunkData copies up to len(p) bytes from the current chunk, then consumes
// the trailing CRLF once the chunk is exhausted.
func (c *chunkedReader) readChunkData(p []byte) (int, error) {
	toRead := min(int64(len(p)), c.remaining)

	n, err := io.ReadFull(c.br, p[:toRead])
	c.remaining -= int64(n)
	if err != nil {
		c.err = errMalformedChunkedBody

		return n, c.err
	}

	if c.remaining == 0 {
		// Each data segment is terminated by CRLF before the next chunk header.
		if crlfErr := c.expectCRLF(); crlfErr != nil {
			c.err = crlfErr

			return n, crlfErr
		}
	}

	return n, nil
}

// advanceChunk reads and parses the next "size;chunk-signature=..." header line,
// setting remaining to the chunk size. A zero size marks the terminal chunk and
// triggers trailer consumption.
func (c *chunkedReader) advanceChunk() error {
	line, err := c.readHeaderLine()
	if err != nil {
		return err
	}

	sizeField := line
	if before, _, found := strings.Cut(line, ";"); found {
		sizeField = before
	}

	size, err := strconv.ParseInt(strings.TrimSpace(sizeField), 16, 64)
	if err != nil || size < 0 {
		return errMalformedChunkedBody
	}

	if size == 0 {
		c.done = true

		return c.consumeTrailers()
	}

	c.remaining = size

	return nil
}

// readHeaderLine reads a single CRLF-terminated framing line (chunk header or
// trailer), returning it without the terminator.
func (c *chunkedReader) readHeaderLine() (string, error) {
	line, err := c.br.ReadString('\n')
	if err != nil {
		return "", errMalformedChunkedBody
	}
	if len(line) > maxChunkHeaderLine {
		return "", errMalformedChunkedBody
	}

	return strings.TrimRight(line, "\r\n"), nil
}

// consumeTrailers drains any trailer headers that follow the terminal chunk
// (present only for the *-TRAILER variants), up to the final empty line.
func (c *chunkedReader) consumeTrailers() error {
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			// Some clients omit the closing CRLF after a signed (non-trailer)
			// zero chunk; treat a clean EOF as a successful end of stream.
			if errors.Is(err, io.EOF) {
				return nil
			}

			return errMalformedChunkedBody
		}
		if strings.TrimRight(line, "\r\n") == "" {
			return nil
		}
	}
}

// expectCRLF consumes the mandatory CRLF that terminates a chunk data segment.
func (c *chunkedReader) expectCRLF() error {
	cr, err := c.br.ReadByte()
	if err != nil {
		return errMalformedChunkedBody
	}
	if cr == '\n' {
		return nil // tolerate bare LF
	}
	if cr != '\r' {
		return errMalformedChunkedBody
	}

	lf, err := c.br.ReadByte()
	if err != nil || lf != '\n' {
		return errMalformedChunkedBody
	}

	return nil
}
