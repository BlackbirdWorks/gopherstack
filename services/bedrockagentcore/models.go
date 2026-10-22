package bedrockagentcore

import "time"

// Event is one memory event of an actor session.
type Event struct {
	Timestamp   time.Time      `json:"timestamp"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Branch      map[string]any `json:"branch,omitempty"`
	Account     string         `json:"account"`
	Region      string         `json:"region"`
	MemoryID    string         `json:"memoryId"`
	ActorID     string         `json:"actorId"`
	SessionID   string         `json:"sessionId"`
	EventID     string         `json:"eventId"`
	ClientToken string         `json:"clientToken,omitempty"`
	Payload     []any          `json:"payload"`
	Seq         uint64         `json:"seq"`
}

// MemoryRecord is a long-term memory record.
type MemoryRecord struct {
	Created    time.Time      `json:"created"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	Account    string         `json:"account"`
	Region     string         `json:"region"`
	MemoryID   string         `json:"memoryId"`
	ID         string         `json:"id"`
	StrategyID string         `json:"strategyId,omitempty"`
	Text       string         `json:"text"`
	Namespaces []string       `json:"namespaces"`
	Seq        uint64         `json:"seq"`
}

// ToolSession is a browser or code interpreter session.
type ToolSession struct {
	Created    time.Time `json:"created"`
	Updated    time.Time `json:"updated"`
	LastActive time.Time `json:"lastActive"`
	Config     doc       `json:"config,omitempty"`
	Account    string    `json:"account"`
	Region     string    `json:"region"`
	Kind       ToolKind  `json:"kind"`
	ToolID     string    `json:"toolId"`
	ID         string    `json:"id"`
	Name       string    `json:"name,omitempty"`
	Status     string    `json:"status"`
	Token      string    `json:"clientToken,omitempty"`
	TimeoutSec int       `json:"timeoutSec"`
}

// AccessToken is an issued workload access token.
type AccessToken struct {
	Expires  time.Time `json:"expires"`
	Token    string    `json:"token"`
	Account  string    `json:"account"`
	Region   string    `json:"region"`
	Workload string    `json:"workload"`
	UserID   string    `json:"userId,omitempty"`
}
