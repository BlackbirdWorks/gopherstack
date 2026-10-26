package azureblob

import (
	"crypto/md5" //nolint:gosec // Content-MD5 is the Azure wire integrity header, not a security control
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"maps"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	headerLeaseID        = "X-Ms-Lease-Id"
	headerIfTags         = "X-Ms-If-Tags"
	headerContentCRC64   = "X-Ms-Content-Crc64"
	headerSourceCRC64    = "X-Ms-Source-Content-Crc64"
	headerSourceMD5      = "X-Ms-Source-Content-Md5"
	headerSourceRange    = "X-Ms-Source-Range"
	crc64Polynomial      = 0x9A6C9329AC4BC9B5
	headerPrefixSourceIf = "X-Ms-Source-"
)

func crc64Table() *crc64.Table { return crc64.MakeTable(crc64Polynomial) }

func parseHTTPDate(r *http.Request, name string) (time.Time, *apiError) {
	raw := r.Header.Get(name)
	if raw == "" {
		return time.Time{}, nil
	}

	parsed, err := http.ParseTime(raw)
	if err != nil {
		return time.Time{}, errInvalidHeader(name)
	}

	return parsed, nil
}

func conditionsFromRequest(r *http.Request, prefix string) (Precond, *apiError) {
	var p Precond

	ims, apiErr := parseHTTPDate(r, prefix+"If-Modified-Since")
	if apiErr != nil {
		return p, apiErr
	}

	ius, apiErr := parseHTTPDate(r, prefix+"If-Unmodified-Since")
	if apiErr != nil {
		return p, apiErr
	}

	p.IfModifiedSince, p.IfUnmodifiedSince = ims, ius
	p.IfMatch = r.Header.Get(prefix + "If-Match")
	p.IfNoneMatch = r.Header.Get(prefix + "If-None-Match")

	return p, nil
}

// precondFromRequest reads the conditional headers, x-ms-if-tags, the lease
// id and the x-ms-source-if-* copy-source conditions.
func precondFromRequest(r *http.Request) (Precond, *apiError) {
	p, apiErr := conditionsFromRequest(r, "")
	if apiErr != nil {
		return p, apiErr
	}

	p.IfTags = r.Header.Get(headerIfTags)
	p.LeaseID = r.Header.Get(headerLeaseID)

	src, apiErr := conditionsFromRequest(r, headerPrefixSourceIf)
	if apiErr != nil {
		return p, apiErr
	}

	src.IfTags = r.Header.Get("X-Ms-Source-If-Tags")

	if src != (Precond{}) {
		p.Source = &src
	}

	return p, nil
}

// writeBackendError renders a StorageError; other errors are reported by the
// caller.
func (h *Handler) writeBackendError(c *echo.Context, err error) (bool, error) {
	var se *StorageError
	if !errors.As(err, &se) {
		return false, nil
	}

	if se.Status == http.StatusNotModified {
		maps.Copy(c.Response().Header(), se.Header)

		return true, c.NoContent(http.StatusNotModified)
	}

	return true, h.writeError(c, se.Status, se.Code, se.Message)
}

func (h *Handler) writeContainerError(c *echo.Context, err error) error {
	if done, werr := h.writeBackendError(c, err); done {
		return werr
	}

	return h.writeContainerNotFound(c)
}

// verifyTransactional checks Content-MD5 and x-ms-content-crc64 against body.
func verifyTransactional(r *http.Request, body []byte, md5Header, crcHeader string) *apiError {
	if want := r.Header.Get(md5Header); want != "" {
		sum := md5.Sum(body) //nolint:gosec // wire integrity check

		if want != base64.StdEncoding.EncodeToString(sum[:]) {
			return &apiError{
				status:  http.StatusBadRequest,
				code:    "Md5Mismatch",
				message: "The MD5 value specified in the request did not match with the MD5 value calculated by the server.",
			}
		}
	}

	if want := r.Header.Get(crcHeader); want != "" && want != crc64Header(body) {
		return &apiError{
			status:  http.StatusBadRequest,
			code:    "Crc64Mismatch",
			message: "The CRC64 value specified in the request did not match with the CRC64 value calculated by the server.",
		}
	}

	return nil
}

func crc64Header(body []byte) string {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], crc64.Checksum(body, crc64Table()))

	return base64.StdEncoding.EncodeToString(buf[:])
}

func md5Header(body []byte) string {
	sum := md5.Sum(body) //nolint:gosec // wire integrity header

	return base64.StdEncoding.EncodeToString(sum[:])
}

func setLeaseHeaders(hdr http.Header, lp LeaseProps) {
	hdr.Set("X-Ms-Lease-Status", lp.Status)
	hdr.Set("X-Ms-Lease-State", lp.State)

	if lp.Duration != "" {
		hdr.Set("X-Ms-Lease-Duration", lp.Duration)
	}
}
