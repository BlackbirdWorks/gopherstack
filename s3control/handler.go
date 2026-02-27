package s3control

import (
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	s3ControlMatchPriority = 85
	// defaultAccountID is used when no account ID is provided in the request header.
	defaultAccountID = "default"
)

// Handler is the Echo HTTP handler for S3 Control operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new S3 Control handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "S3Control" }

// GetSupportedOperations returns the list of supported S3 Control operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"GetPublicAccessBlock",
		"PutPublicAccessBlock",
		"DeletePublicAccessBlock",
	}
}

// RouteMatcher returns a function that matches S3 Control requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, "/v20180820/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return s3ControlMatchPriority }

// ExtractOperation extracts the S3 Control operation from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if strings.HasSuffix(r.URL.Path, "/configuration/publicAccessBlock") {
		switch r.Method {
		case http.MethodGet:
			return "GetPublicAccessBlock"
		case http.MethodPut:
			return "PutPublicAccessBlock"
		case http.MethodDelete:
			return "DeletePublicAccessBlock"
		}
	}

	return "Unknown"
}

// ExtractResource returns the account ID from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return c.Request().Header.Get("X-Amz-Account-Id")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if strings.HasSuffix(r.URL.Path, "/configuration/publicAccessBlock") {
			switch r.Method {
			case http.MethodGet:
				return h.handleGetPublicAccessBlock(c)
			case http.MethodPut:
				return h.handlePutPublicAccessBlock(c)
			case http.MethodDelete:
				return h.handleDeletePublicAccessBlock(c)
			}
		}

		return c.String(http.StatusNotFound, "not found")
	}
}

type publicAccessBlockConfigurationXML struct {
	XMLName               xml.Name `xml:"PublicAccessBlockConfiguration"`
	BlockPublicAcls       bool     `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool     `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool     `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool     `xml:"RestrictPublicBuckets"`
}

type getPublicAccessBlockOutputXML struct {
	XMLName                        xml.Name                          `xml:"GetPublicAccessBlockOutput"`
	PublicAccessBlockConfiguration publicAccessBlockConfigurationXML `xml:"PublicAccessBlockConfiguration"`
}

func (h *Handler) handleGetPublicAccessBlock(c *echo.Context) error {
	accountID := c.Request().Header.Get("X-Amz-Account-Id")
	if accountID == "" {
		accountID = defaultAccountID
	}

	cfg, err := h.Backend.GetPublicAccessBlock(accountID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.String(http.StatusNotFound, "NoSuchPublicAccessBlockConfiguration")
		}

		return c.String(http.StatusInternalServerError, err.Error())
	}

	out := getPublicAccessBlockOutputXML{
		PublicAccessBlockConfiguration: publicAccessBlockConfigurationXML{
			BlockPublicAcls:       cfg.BlockPublicAcls,
			IgnorePublicAcls:      cfg.IgnorePublicAcls,
			BlockPublicPolicy:     cfg.BlockPublicPolicy,
			RestrictPublicBuckets: cfg.RestrictPublicBuckets,
		},
	}

	xmlBytes, err := xml.Marshal(out)
	if err != nil {
		return c.String(http.StatusInternalServerError, "marshal error")
	}

	return c.Blob(http.StatusOK, "application/xml", append([]byte(xml.Header), xmlBytes...))
}

func (h *Handler) handlePutPublicAccessBlock(c *echo.Context) error {
	accountID := c.Request().Header.Get("X-Amz-Account-Id")
	if accountID == "" {
		accountID = defaultAccountID
	}

	var body publicAccessBlockConfigurationXML
	if err := xml.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	h.Backend.PutPublicAccessBlock(PublicAccessBlock{
		AccountID:             accountID,
		BlockPublicAcls:       body.BlockPublicAcls,
		IgnorePublicAcls:      body.IgnorePublicAcls,
		BlockPublicPolicy:     body.BlockPublicPolicy,
		RestrictPublicBuckets: body.RestrictPublicBuckets,
	})

	return c.NoContent(http.StatusCreated)
}

func (h *Handler) handleDeletePublicAccessBlock(c *echo.Context) error {
	accountID := c.Request().Header.Get("X-Amz-Account-Id")
	if accountID == "" {
		accountID = defaultAccountID
	}

	if err := h.Backend.DeletePublicAccessBlock(accountID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.String(http.StatusNotFound, "NoSuchPublicAccessBlockConfiguration")
		}

		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}
