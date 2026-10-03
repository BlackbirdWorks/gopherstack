package main

import (
	"net/http"
	"sort"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	sesv2backend "github.com/blackbirdworks/gopherstack/services/sesv2"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

type sesRetroDestination struct {
	ToAddresses  []string `json:"ToAddresses"`
	CcAddresses  []string `json:"CcAddresses"`
	BccAddresses []string `json:"BccAddresses"`
}

type sesRetroBody struct {
	TextPart *string `json:"text_part"`
	HTMLPart *string `json:"html_part"`
}

// sesRetroMessage is LocalStack's /_aws/ses per-message shape.
type sesRetroMessage struct {
	Body        sesRetroBody        `json:"Body"`
	ID          string              `json:"Id"`
	Region      string              `json:"Region"`
	Source      string              `json:"Source"`
	Timestamp   string              `json:"Timestamp"`
	Subject     string              `json:"Subject"`
	Destination sesRetroDestination `json:"Destination"`
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

func sesRetroMessages(services []service.Registerable) []sesRetroMessage {
	out := make([]sesRetroMessage, 0)

	for _, svc := range services {
		switch h := svc.(type) {
		case *sesbackend.Handler:
			if b, ok := h.Backend.(*sesbackend.InMemoryBackend); ok {
				for _, e := range b.ListEmails() {
					out = append(out, sesRetroMessage{
						ID: e.MessageID, Region: b.Region(), Source: e.From, Subject: e.Subject,
						Timestamp: e.Timestamp.UTC().Format(time.RFC3339Nano),
						Destination: sesRetroDestination{
							ToAddresses: orEmpty(e.To), CcAddresses: orEmpty(e.Cc), BccAddresses: orEmpty(e.Bcc),
						},
						Body: sesRetroBody{TextPart: nilIfEmpty(e.BodyText), HTMLPart: nilIfEmpty(e.BodyHTML)},
					})
				}
			}
		case *sesv2backend.Handler:
			for _, b := range h.MailBackends() {
				for _, e := range b.ListEmails() {
					out = append(out, sesRetroMessage{
						ID: e.MessageID, Region: b.Region(), Source: e.From, Subject: e.Subject,
						Timestamp: e.Timestamp.UTC().Format(time.RFC3339Nano),
						Destination: sesRetroDestination{
							ToAddresses: orEmpty(e.To), CcAddresses: []string{}, BccAddresses: []string{},
						},
						Body: sesRetroBody{TextPart: nilIfEmpty(e.BodyText), HTMLPart: nilIfEmpty(e.BodyHTML)},
					})
				}
			}
		}
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })

	return out
}

// buildSESRetrospectionGet serves GET /_aws/ses with optional id/email filters.
func buildSESRetrospectionGet(services []service.Registerable) echo.HandlerFunc {
	return func(c *echo.Context) error {
		id, email := c.QueryParam("id"), c.QueryParam("email")
		msgs := make([]sesRetroMessage, 0)

		for _, m := range sesRetroMessages(services) {
			if (id == "" || m.ID == id) && (email == "" || m.Source == email) {
				msgs = append(msgs, m)
			}
		}

		return c.JSON(http.StatusOK, map[string]any{"messages": msgs})
	}
}

type sesMailStore interface {
	ClearEmails()
	DeleteEmail(messageID string) bool
}

func sesMailStores(services []service.Registerable) []sesMailStore {
	var out []sesMailStore

	for _, svc := range services {
		switch h := svc.(type) {
		case *sesbackend.Handler:
			if b, ok := h.Backend.(*sesbackend.InMemoryBackend); ok {
				out = append(out, b)
			}
		case *sesv2backend.Handler:
			for _, b := range h.MailBackends() {
				out = append(out, b)
			}
		}
	}

	return out
}

// buildSESRetrospectionDelete serves DELETE /_aws/ses, clearing one message (id) or all.
func buildSESRetrospectionDelete(services []service.Registerable) echo.HandlerFunc {
	return func(c *echo.Context) error {
		id := c.QueryParam("id")

		for _, st := range sesMailStores(services) {
			if id == "" {
				st.ClearEmails()
			} else {
				st.DeleteEmail(id)
			}
		}

		return c.NoContent(http.StatusNoContent)
	}
}

// registerLocalstackDevEndpoints wires LocalStack's developer/introspection endpoints.
func registerLocalstackDevEndpoints(e *echo.Echo, services []service.Registerable) {
	e.GET("/_aws/ses", buildSESRetrospectionGet(services))
	e.DELETE("/_aws/ses", buildSESRetrospectionDelete(services))
	e.POST("/_localstack/state/reset", buildResetHandler(services))

	for _, svc := range services {
		switch h := svc.(type) {
		case *sqsbackend.Handler:
			e.GET("/_aws/sqs/messages", h.ServeInspectMessages)
			e.POST("/_aws/sqs/messages", h.ServeInspectMessages)
			e.GET("/_aws/sqs/messages/:region/:account/:queue", h.ServeInspectMessages)
		case *cwbackend.Handler:
			e.GET("/_aws/cloudwatch/metrics/raw", h.ServeRawMetrics)
		}
	}
}
