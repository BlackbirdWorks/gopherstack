// Package apigatewaymanagementapi provides an in-memory stub for the
// AWS API Gateway Management API, which is used to send data to connected
// WebSocket API clients and manage WebSocket connections.
package apigatewaymanagementapi

import (
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrConnectionNotFound is returned when the requested connection does not exist.
	ErrConnectionNotFound = awserr.New("GoneException", awserr.ErrNotFound)
	// ErrPayloadTooLarge is returned when the payload exceeds the maximum allowed size.
	ErrPayloadTooLarge = errors.New("payload too large")
)

// Connection represents an active WebSocket API connection.
type Connection struct {
	ConnectedAt    time.Time `json:"connectedAt"`
	LastActiveAt   time.Time `json:"lastActiveAt"`
	ConnectionID   string    `json:"connectionId"`
	SourceIP       string    `json:"sourceIp"`
	UserAgent      string    `json:"userAgent"`
	PostedMessages int       `json:"postedMessages"`
}

// PostedMessage represents a message sent to a connection via PostToConnection.
type PostedMessage struct {
	ReceivedAt   time.Time `json:"receivedAt"`
	ConnectionID string    `json:"connectionId"`
	Data         []byte    `json:"data"`
}
