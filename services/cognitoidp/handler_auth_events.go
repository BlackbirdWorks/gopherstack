package cognitoidp

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// handleAdminListUserAuthEvents returns the tracked adaptive-auth events for a user,
// paginated by MaxResults/NextToken.
func (h *Handler) handleAdminListUserAuthEvents(
	_ context.Context,
	in *adminListUserAuthEventsInput,
) (*adminListUserAuthEventsOutput, error) {
	limit, err := validateCognitoMaxResults(in.MaxResults)
	if err != nil {
		return nil, err
	}

	events, token, err := h.Backend.AdminListUserAuthEvents(in.UserPoolID, in.Username, limit, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]authEventOutputType, 0, len(events))
	for _, e := range events {
		item := authEventOutputType{
			EventID:       e.EventID,
			EventType:     e.EventType,
			CreationDate:  float64(e.CreatedAt.Unix()),
			EventResponse: e.EventResponse,
		}

		if e.FeedbackValue != "" {
			item.EventFeedback = &authEventFeedbackType{
				FeedbackValue: e.FeedbackValue,
				FeedbackDate:  float64(e.FeedbackDate.Unix()),
			}
		}

		out = append(out, item)
	}

	return &adminListUserAuthEventsOutput{AuthEvents: out, NextToken: token}, nil
}

func (h *Handler) handleAdminUpdateAuthEventFeedback(
	_ context.Context,
	in *adminUpdateAuthEventFeedbackInput,
) (*adminUpdateAuthEventFeedbackOutput, error) {
	if err := h.Backend.AdminUpdateAuthEventFeedback(
		in.UserPoolID, in.Username, in.EventID, in.FeedbackValue,
	); err != nil {
		return nil, err
	}

	return &adminUpdateAuthEventFeedbackOutput{}, nil
}

// handleUpdateAuthEventFeedback matches AWS: this op is unauthenticated (no
// AccessToken) and instead takes UserPoolId/Username directly plus a
// FeedbackToken issued out-of-band in a risk-notification email. This
// emulator does not mint or verify those tokens, so it requires one be
// present (matching the request's required-field contract) without
// cryptographically validating it.
func (h *Handler) handleUpdateAuthEventFeedback(
	_ context.Context,
	in *updateAuthEventFeedbackInput,
) (*updateAuthEventFeedbackOutput, error) {
	if in.FeedbackToken == "" {
		return nil, fmt.Errorf("%w: FeedbackToken is required", ErrInvalidParameter)
	}

	if err := h.Backend.UpdateAuthEventFeedback(in.UserPoolID, in.Username, in.EventID, in.FeedbackValue); err != nil {
		return nil, err
	}

	return &updateAuthEventFeedbackOutput{}, nil
}

func (h *Handler) authEventsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AdminListUserAuthEvents":      service.WrapOp(h.handleAdminListUserAuthEvents),
		"AdminUpdateAuthEventFeedback": service.WrapOp(h.handleAdminUpdateAuthEventFeedback),
		"UpdateAuthEventFeedback":      service.WrapOp(h.handleUpdateAuthEventFeedback),
	}
}
