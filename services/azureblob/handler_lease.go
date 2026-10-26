package azureblob

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const (
	headerLeaseAction   = "X-Ms-Lease-Action"
	headerLeaseDuration = "X-Ms-Lease-Duration"
	headerLeaseBreak    = "X-Ms-Lease-Break-Period"
	headerProposedLease = "X-Ms-Proposed-Lease-Id"

	minLeaseSeconds = 15
	maxLeaseSeconds = 60
	maxBreakSeconds = 60
)

func errMissingHeader(name string) *apiError {
	return &apiError{
		status: http.StatusBadRequest, code: "MissingRequiredHeader",
		message: "An HTTP header that's mandatory for this request is not specified (" + strings.ToLower(name) + ").",
	}
}

func leaseRequestFromHeaders(r *http.Request) (LeaseRequest, *apiError) {
	cond, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return LeaseRequest{}, apiErr
	}

	req := LeaseRequest{
		Action:     strings.ToLower(r.Header.Get(headerLeaseAction)),
		ID:         r.Header.Get(headerLeaseID),
		ProposedID: r.Header.Get(headerProposedLease),
		Cond:       cond,
	}

	if req.ProposedID != "" {
		if _, err := uuid.Parse(req.ProposedID); err != nil {
			return req, errInvalidHeader(headerProposedLease)
		}
	}

	switch req.Action {
	case LeaseActionAcquire:
		return req, acquireHeaders(r, &req)
	case LeaseActionRenew, LeaseActionRelease:
		return req, requireHeader(req.ID, headerLeaseID)
	case LeaseActionChange:
		if apiErr = requireHeader(req.ID, headerLeaseID); apiErr != nil {
			return req, apiErr
		}

		return req, requireHeader(req.ProposedID, headerProposedLease)
	case LeaseActionBreak:
		return req, breakHeaders(r, &req)
	case "":
		return req, errMissingHeader(headerLeaseAction)
	default:
		return req, errInvalidHeader(headerLeaseAction)
	}
}

func requireHeader(val, name string) *apiError {
	if val == "" {
		return errMissingHeader(name)
	}

	return nil
}

func acquireHeaders(r *http.Request, req *LeaseRequest) *apiError {
	raw := r.Header.Get(headerLeaseDuration)
	if raw == "" {
		return errMissingHeader(headerLeaseDuration)
	}

	n, err := strconv.Atoi(raw)
	if err != nil || (n != LeaseInfinite && (n < minLeaseSeconds || n > maxLeaseSeconds)) {
		return errInvalidHeader(headerLeaseDuration)
	}

	req.Duration = n

	return nil
}

func breakHeaders(r *http.Request, req *LeaseRequest) *apiError {
	raw := r.Header.Get(headerLeaseBreak)
	if raw == "" {
		return nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > maxBreakSeconds {
		return errInvalidHeader(headerLeaseBreak)
	}

	req.BreakPeriod, req.HasBreakPeriod = n, true

	return nil
}

func (h *Handler) writeLeaseResult(c *echo.Context, req LeaseRequest, res LeaseResult) error {
	hdr := c.Response().Header()

	if res.ETag != "" {
		hdr.Set("ETag", res.ETag)
		hdr.Set("Last-Modified", res.LastModified.Format(http.TimeFormat))
	}

	switch req.Action {
	case LeaseActionAcquire:
		hdr.Set(headerLeaseID, res.ID)

		return c.NoContent(http.StatusCreated)
	case LeaseActionRenew, LeaseActionChange:
		hdr.Set(headerLeaseID, res.ID)
	case LeaseActionBreak:
		hdr.Set("X-Ms-Lease-Time", strconv.Itoa(res.BreakRemaining))

		return c.NoContent(http.StatusAccepted)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) leaseBlob(c *echo.Context, container, blob string) error {
	req, apiErr := leaseRequestFromHeaders(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	res, err := h.Backend.LeaseBlob(container, blob, req)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	return h.writeLeaseResult(c, req, res)
}

func (h *Handler) leaseContainer(c *echo.Context, container string) error {
	req, apiErr := leaseRequestFromHeaders(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	res, err := h.Backend.LeaseContainer(container, req)
	if err != nil {
		return h.writeContainerError(c, err)
	}

	return h.writeLeaseResult(c, req, res)
}
