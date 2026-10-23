package amqp1

import "context"

// Handler supplies the application behaviour behind a Server.
type Handler interface {
	// Authenticate validates SASL credentials. ANONYMOUS passes empty strings.
	Authenticate(mechanism Symbol, username, password string) bool
	// NewConn creates the per-connection handler.
	NewConn() ConnHandler
}

// ConnHandler resolves link attaches for one connection.
type ConnHandler interface {
	// AttachSender is called when the peer opens a receiving link; the server
	// then sends messages from the returned endpoint.
	AttachSender(a *Attach) (SenderEndpoint, *Error)
	// AttachReceiver is called when the peer opens a sending link; the server
	// then receives messages into the returned endpoint.
	AttachReceiver(a *Attach) (ReceiverEndpoint, *Error)
	// Close releases connection state.
	Close()
}

// Outgoing is a message the server sends to the peer.
type Outgoing struct {
	Ref     any
	Tag     []byte
	Payload []byte
	Format  uint32
	Settled bool
}

// Incoming is a fully assembled message received from the peer.
type Incoming struct {
	Tag     []byte
	Payload []byte
	Format  uint32
	Settled bool
}

// SenderEndpoint is the source of messages for a server-sending link.
type SenderEndpoint interface {
	// Next blocks until a message is available or ctx is done.
	Next(ctx context.Context) (*Outgoing, error)
	// Settle records the peer's outcome for a sent message.
	Settle(o *Outgoing, st DeliveryState)
	// Release returns an unsettled message when its link or connection ends.
	Release(o *Outgoing)
	// Close is called once when the link ends.
	Close()
}

// AttachReplier is implemented by a SenderEndpoint that amends the attach
// reply (for example to report a negotiated source filter or link properties).
type AttachReplier interface {
	ReplyAttach(reply *Attach)
}

// DeferredAttach is implemented by a SenderEndpoint whose attach completes
// asynchronously. Ready blocks (off the connection read loop) until the link
// may open, or returns the error that refuses it; it must return when ctx is
// done.
type DeferredAttach interface {
	Ready(ctx context.Context) *Error
}

// SettleGuard is implemented by a SenderEndpoint that can invalidate a
// settlement; a non-nil error detaches the link with that error instead of
// settling.
type SettleGuard interface {
	SettleError(o *Outgoing) *Error
}

// ReceiverEndpoint consumes messages sent by the peer.
type ReceiverEndpoint interface {
	// Deliver handles one message and returns the outcome to report.
	Deliver(ctx context.Context, in *Incoming) DeliveryState
	// Close is called once when the link ends.
	Close()
}

// Standard error conditions.
const (
	CondNotFound        Symbol = "amqp:not-found"
	CondNotImplemented  Symbol = "amqp:not-implemented"
	CondUnauthorized    Symbol = "amqp:unauthorized-access"
	CondInternal        Symbol = "amqp:internal-error"
	CondResourceLimit   Symbol = "amqp:resource-limit-exceeded"
	CondMessageTooLarge Symbol = "amqp:link:message-size-exceeded"
	CondFrameSize       Symbol = "amqp:connection:framing-error"
	CondDecode          Symbol = "amqp:decode-error"
	CondIllegalState    Symbol = "amqp:illegal-state"
	CondNotAllowed      Symbol = "amqp:not-allowed"
)
