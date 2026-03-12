package mediastoredata

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// msdMatchPriority must be higher than S3 (0) to intercept mediastoredata SDK requests.
	msdMatchPriority = 87
	// userAgentMarker is the AWS SDK marker present in User-Agent for MediaStore Data requests.
	userAgentMarker = "mediastoredata"
)

// Handler is the Echo HTTP handler for Amazon MediaStore Data operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new MediaStore Data handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaStoreData" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"PutObject",
		"GetObject",
		"DeleteObject",
		"ListItems",
		"DescribeObject",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "mediastoredata" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{"us-east-1"} }

// RouteMatcher returns a function that matches MediaStore Data requests.
// It identifies requests by the "mediastoredata" marker in the User-Agent
// header, which the AWS SDK always includes.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		ua := c.Request().Header.Get("User-Agent")

		return strings.Contains(ua, userAgentMarker)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return msdMatchPriority }

// ExtractOperation returns the operation name from the request method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()

	switch r.Method {
	case http.MethodPut:
		return "PutObject"
	case http.MethodGet:
		if r.URL.Path == "/" || r.URL.Path == "" {
			return "ListItems"
		}

		return "GetObject"
	case http.MethodDelete:
		return "DeleteObject"
	case http.MethodHead:
		return "DescribeObject"
	}

	return "Unknown"
}

// ExtractResource extracts the path from the URL.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return c.Request().URL.Path
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		log := logger.Load(r.Context())

		switch r.Method {
		case http.MethodPut:
			return h.handlePutObject(c)
		case http.MethodGet:
			if r.URL.Path == "/" || r.URL.Path == "" {
				return h.handleListItems(c)
			}

			return h.handleGetObject(c)
		case http.MethodDelete:
			return h.handleDeleteObject(c)
		case http.MethodHead:
			return h.handleDescribeObject(c)
		}

		log.WarnContext(r.Context(), "mediastoredata: unmatched request", "method", r.Method, "path", r.URL.Path)

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowedException", "method not allowed"))
	}
}

// handlePutObject handles PUT /{Path+}.
func (h *Handler) handlePutObject(c *echo.Context) error {
	r := c.Request()
	log := logger.Load(r.Context())

	body, err := httputils.ReadBody(r)
	if err != nil {
		log.ErrorContext(r.Context(), "mediastoredata: failed to read body", "error", err)

		return c.JSON(
			http.StatusInternalServerError,
			errorResponse("InternalServerError", "failed to read request body"),
		)
	}

	path := r.URL.Path
	contentType := r.Header.Get("Content-Type")
	cacheControl := r.Header.Get("Cache-Control")
	storageClass := r.Header.Get("X-Amz-Storage-Class")

	obj := h.Backend.PutObject(path, body, contentType, cacheControl, storageClass)

	return c.JSON(http.StatusOK, map[string]string{
		"ContentSHA256": contentSHA256(body),
		"ETag":          obj.ETag,
		"StorageClass":  obj.StorageClass,
	})
}

// handleGetObject handles GET /{Path+}.
func (h *Handler) handleGetObject(c *echo.Context) error {
	r := c.Request()

	obj, err := h.Backend.GetObject(r.URL.Path)
	if err != nil {
		return h.writeError(c, err)
	}

	w := c.Response()
	setObjectHeaders(w, obj)
	w.WriteHeader(http.StatusOK)

	if _, writeErr := w.Write(obj.Body); writeErr != nil {
		logger.Load(r.Context()).
			ErrorContext(r.Context(), "mediastoredata: failed to write response body", "error", writeErr)
	}

	return nil
}

// handleDeleteObject handles DELETE /{Path+}.
func (h *Handler) handleDeleteObject(c *echo.Context) error {
	r := c.Request()

	if err := h.Backend.DeleteObject(r.URL.Path); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// listItemsOutput is the JSON response for ListItems.
type listItemsOutput struct {
	NextToken *string     `json:"NextToken,omitempty"`
	Items     []itemEntry `json:"Items"`
}

// itemEntry represents a single item in a ListItems response.
type itemEntry struct {
	LastModified  *float64 `json:"LastModified,omitempty"`
	ContentLength *int64   `json:"ContentLength,omitempty"`
	ContentType   string   `json:"ContentType,omitempty"`
	ETag          string   `json:"ETag,omitempty"`
	Name          string   `json:"Name"`
	Type          string   `json:"Type"`
}

// handleListItems handles GET / or GET /?Path=...
func (h *Handler) handleListItems(c *echo.Context) error {
	folderPath := c.Request().URL.Query().Get("Path")

	items := h.Backend.ListItems(folderPath)
	entries := make([]itemEntry, 0, len(items))

	for _, item := range items {
		entry := itemEntry{
			Name: item.Name,
			Type: item.Type,
		}

		if item.Type == "OBJECT" {
			ts := float64(item.LastModified.Unix())
			entry.LastModified = &ts
			entry.ContentLength = &item.ContentLength
			entry.ContentType = item.ContentType
			entry.ETag = item.ETag
		}

		entries = append(entries, entry)
	}

	return c.JSON(http.StatusOK, listItemsOutput{Items: entries})
}

// handleDescribeObject handles HEAD /{Path+}.
func (h *Handler) handleDescribeObject(c *echo.Context) error {
	r := c.Request()

	obj, err := h.Backend.GetObject(r.URL.Path)
	if err != nil {
		return h.writeError(c, err)
	}

	w := c.Response()
	setObjectHeaders(w, obj)
	w.WriteHeader(http.StatusOK)

	return nil
}

// setObjectHeaders sets common response headers for an object.
func setObjectHeaders(w http.ResponseWriter, obj *Object) {
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.ContentLength, 10))
	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))

	if obj.CacheControl != "" {
		w.Header().Set("Cache-Control", obj.CacheControl)
	}
}

// writeError maps backend errors to appropriate HTTP responses.
func (h *Handler) writeError(c *echo.Context, err error) error {
	if errors.Is(err, ErrNotFound) {
		return c.JSON(http.StatusNotFound, errorResponse("ObjectNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerError", err.Error()))
}

// errorResponse returns a JSON-serialisable error payload.
func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}
