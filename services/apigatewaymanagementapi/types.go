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
	// ErrConnectionExists is returned when attempting to create a duplicate connection.
	ErrConnectionExists = errors.New("connection already exists")
	// ErrLimitExceeded is returned when a frame cannot be queued for delivery
	// because the WebSocket connection's client-side buffer is full. Real AWS
	// documents this exact condition as a LimitExceededException.
	ErrLimitExceeded = errors.New("websocket client-side buffer is full")
)

// EventType describes a lifecycle event recorded against a connection.
type EventType string

const (
	// EventConnected marks the moment a connection was registered.
	EventConnected EventType = "connected"
	// EventMessage marks a successful PostToConnection.
	EventMessage EventType = "message"
	// EventDisconnected marks the moment a connection was terminated.
	EventDisconnected EventType = "disconnected"
	// EventPing marks a heartbeat that updates LastActiveAt without storing data.
	EventPing EventType = "ping"
	// EventBroadcast marks a broadcast send to all active connections.
	EventBroadcast EventType = "broadcast"
)

// Connection represents an active WebSocket API connection.
type Connection struct {
	ConnectedAt    time.Time `json:"connectedAt"`
	LastActiveAt   time.Time `json:"lastActiveAt"`
	ConnectionID   string    `json:"connectionId"`
	SourceIP       string    `json:"sourceIp"`
	UserAgent      string    `json:"userAgent"`
	PostedMessages int       `json:"postedMessages"`
	BytesSent      int64     `json:"bytesSent"`
}

// PostedMessage represents a message sent to a connection via PostToConnection.
type PostedMessage struct {
	ReceivedAt   time.Time `json:"receivedAt"`
	ConnectionID string    `json:"connectionId"`
	Data         []byte    `json:"data"`
}

// LifecycleEvent records a notable moment in a connection's history.
type LifecycleEvent struct {
	At     time.Time `json:"at"`
	Type   EventType `json:"type"`
	Detail string    `json:"detail,omitempty"`
	Bytes  int       `json:"bytes,omitempty"`
}

// Stats summarises backend-wide activity.
type Stats struct {
	ActiveConnections   int   `json:"activeConnections"`
	BufferedMessages    int   `json:"bufferedMessages"`
	TotalConnections    int64 `json:"totalConnections"`
	TotalDisconnections int64 `json:"totalDisconnections"`
	TotalMessages       int64 `json:"totalMessages"`
	TotalBroadcasts     int64 `json:"totalBroadcasts"`
	TotalBytesSent      int64 `json:"totalBytesSent"`
	TotalRejected       int64 `json:"totalRejected"`
}
