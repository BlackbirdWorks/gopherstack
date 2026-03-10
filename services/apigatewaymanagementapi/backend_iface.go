package apigatewaymanagementapi

// StorageBackend defines the operations supported by the API Gateway
// Management API in-memory backend.
type StorageBackend interface {
	// PostToConnection sends data to the specified WebSocket connection.
	PostToConnection(connectionID string, data []byte) error
	// GetConnection retrieves metadata for the specified WebSocket connection.
	GetConnection(connectionID string) (*Connection, error)
	// DeleteConnection deletes the specified WebSocket connection.
	DeleteConnection(connectionID string) error
	// CreateConnection creates a new simulated WebSocket connection for testing.
	CreateConnection(connectionID, sourceIP, userAgent string) (*Connection, error)
	// ListConnections returns all active WebSocket connections.
	ListConnections() []Connection
	// GetMessages returns all messages posted to the given connection.
	GetMessages(connectionID string) []PostedMessage
}
