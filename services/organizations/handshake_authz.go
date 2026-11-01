package organizations

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

const handshakeTargetEmail = "EMAIL"

// handshakeStateError classifies a response to a handshake no longer OPEN:
// already in the requested state vs an invalid transition (types/errors.go).
func handshakeStateError(current, requested string) error {
	if current == requested {
		return ErrHandshakeAlreadyInState
	}

	return ErrInvalidHandshakeTransition
}

// authorizeHandshakeCaller enforces who may act on h: only the originator
// cancels; only a recipient accepts or declines (api_op_CancelHandshake.go,
// api_op_DeclineHandshake.go, api_op_AcceptHandshake.go). A request with no
// explicit caller identity is permitted. Must be called with b.mu held.
func (b *InMemoryBackend) authorizeHandshakeCaller(ctx context.Context, h *Handshake, asOriginator bool) error {
	caller, ok := awsmeta.CallerAccount(ctx)
	if !ok || len(h.Parties) == 0 {
		return nil
	}

	originator := h.Parties[0].ID
	allowed := caller == originator

	if !asOriginator {
		allowed = caller != originator && b.isHandshakeRecipient(h, caller)

		if h.Action == handshakeActionEnableFeatures {
			allowed = caller != originator && b.accounts.Has(caller)
		}
	}

	if allowed {
		return nil
	}

	return fmt.Errorf("%w: account %s", ErrAccessDeniedHandshake, caller)
}

// isHandshakeRecipient matches caller against the non-originator parties; an
// EMAIL or ORGANIZATION party the backend cannot map to an account admits any
// account that is not a known member.
func (b *InMemoryBackend) isHandshakeRecipient(h *Handshake, caller string) bool {
	for _, p := range h.Parties[1:] {
		switch p.Type {
		case targetTypeAccount:
			if p.ID == caller {
				return true
			}
		case handshakeTargetEmail:
			if acct, ok := b.accounts.Get(caller); ok {
				if acct.Email == p.ID {
					return true
				}

				continue
			}

			return true
		default:
			return !b.accounts.Has(caller)
		}
	}

	return false
}
