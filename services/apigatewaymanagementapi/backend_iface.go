package apigatewaymanagementapi

import "time"

// StorageBackend defines the operations supported by the API Gateway
// Management API in-memory backend.
type StorageBackend interface {
	// PostToConnection sends data to the specified WebSocket connection.
	PostToConnection(connectionID string, data []byte) error
	// GetConnection retrieves metadata for the specified WebSocket connection.
	GetConnection(connectionID string) (*Connection, error)
	// DeleteConnection deletes the specified WebSocket connection.
	DeleteConnection(connectionID string) error
	// CreateConnection creates a new simulated WebSocket connection.
	CreateConnection(connectionID, sourceIP, userAgent string, downstream chan []byte) (*Connection, error)
	// ListConnections returns all active WebSocket connections.
	ListConnections() []Connection
	// FilterConnections returns connections whose ID, IP, or user-agent contain query.
	FilterConnections(query string) []Connection
	// GetMessages returns all messages posted to the given connection.
	GetMessages(connectionID string) []PostedMessage
	// ClearMessages drops all stored messages for the connection.
	ClearMessages(connectionID string) error
	// GetTimeline returns lifecycle events for the given connection.
	GetTimeline(connectionID string) []LifecycleEvent
	// PingConnection refreshes LastActiveAt without storing payload data.
	PingConnection(connectionID string) error
	// Broadcast posts data to every active connection and returns the count delivered.
	Broadcast(data []byte) (int, error)
	// PruneIdle removes connections idle longer than threshold.
	PruneIdle(threshold time.Duration) []string
	// Stats returns cumulative counters and active state.
	Stats() Stats
	// Snapshot serialises backend state to JSON for persistence.
	Snapshot() []byte
	// Restore loads backend state from a JSON snapshot.
	Restore(data []byte) error
	// Reset clears all in-memory state for test isolation.
	Reset()
}
