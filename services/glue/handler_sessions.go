package glue

import (
	"context"
	"fmt"
)

// cancelStatementInput holds input for CancelStatement.
type cancelStatementInput struct {
	SessionID   string `json:"SessionId"`
	StatementID int32  `json:"Id"`
}

func (h *Handler) handleCancelStatement(
	_ context.Context,
	in *cancelStatementInput,
) (*emptyOutput, error) {
	if in.SessionID == "" {
		return nil, fmt.Errorf("%w: SessionId is required", ErrValidation)
	}

	return &emptyOutput{}, h.Backend.CancelStatement(in.SessionID, in.StatementID)
}

// createSessionInput holds input for CreateSession.
type createSessionInput struct {
	DefaultArguments map[string]string `json:"DefaultArguments,omitempty"`
	Command          SessionCommand    `json:"Command"`
	ID               string            `json:"Id"`
	Role             string            `json:"Role,omitempty"`
	Description      string            `json:"Description,omitempty"`
	Timeout          int32             `json:"Timeout,omitempty"`
	MaxCapacity      float64           `json:"MaxCapacity,omitempty"`
}

// createSessionOutput holds the result for CreateSession.
type createSessionOutput struct {
	Session *Session `json:"Session"`
}

func (h *Handler) handleCreateSession(
	_ context.Context,
	in *createSessionInput,
) (*createSessionOutput, error) {
	opts := Session{
		Timeout:          in.Timeout,
		MaxCapacity:      in.MaxCapacity,
		Description:      in.Description,
		DefaultArguments: in.DefaultArguments,
	}
	s, err := h.Backend.CreateSession(in.ID, in.Role, in.Command, opts)
	if err != nil {
		return nil, err
	}

	return &createSessionOutput{Session: s}, nil
}

// deleteSessionInput holds input for DeleteSession.
type deleteSessionInput struct {
	ID string `json:"Id"`
}

// deleteSessionOutput holds the result for DeleteSession.
type deleteSessionOutput struct {
	ID string `json:"Id"`
}

func (h *Handler) handleDeleteSession(
	_ context.Context,
	in *deleteSessionInput,
) (*deleteSessionOutput, error) {
	if err := h.Backend.DeleteSession(in.ID); err != nil {
		return nil, err
	}

	return &deleteSessionOutput{ID: in.ID}, nil
}

// getSessionInput holds input for GetSession.
type getSessionInput struct {
	ID string `json:"Id"`
}

// getSessionOutput holds the result for GetSession.
type getSessionOutput struct {
	Session *Session `json:"Session"`
}

func (h *Handler) handleGetSession(
	_ context.Context,
	in *getSessionInput,
) (*getSessionOutput, error) {
	s, err := h.Backend.GetSession(in.ID)
	if err != nil {
		return nil, err
	}

	return &getSessionOutput{Session: s}, nil
}

// getStatementInput holds input for GetStatement.
type getStatementInput struct {
	SessionID   string `json:"SessionId"`
	StatementID int32  `json:"Id"`
}

// getStatementOutput holds the result for GetStatement.
type getStatementOutput struct {
	Statement *Statement `json:"Statement"`
}

func (h *Handler) handleGetStatement(
	_ context.Context,
	in *getStatementInput,
) (*getStatementOutput, error) {
	st, err := h.Backend.GetStatement(in.SessionID, in.StatementID)
	if err != nil {
		return nil, err
	}

	return &getStatementOutput{Statement: st}, nil
}

// listSessionsInput holds input for ListSessions.
type listSessionsInput struct{}

// listSessionsOutput holds the result for ListSessions.
type listSessionsOutput struct {
	IDs      []string   `json:"Ids"`
	Sessions []*Session `json:"Sessions"`
}

func (h *Handler) handleListSessions(
	_ context.Context,
	_ *listSessionsInput,
) (*listSessionsOutput, error) {
	sessions := h.Backend.ListSessions()
	ids := make([]string, len(sessions))
	for i, s := range sessions {
		ids[i] = s.SessionID
	}

	return &listSessionsOutput{IDs: ids, Sessions: sessions}, nil
}

// listStatementsInput holds input for ListStatements.
type listStatementsInput struct {
	SessionID string `json:"SessionId"`
}

// listStatementsOutput holds the result for ListStatements.
type listStatementsOutput struct {
	Statements []*Statement `json:"Statements"`
}

func (h *Handler) handleListStatements(
	_ context.Context,
	in *listStatementsInput,
) (*listStatementsOutput, error) {
	stmts, err := h.Backend.GetStatements(in.SessionID)
	if err != nil {
		return nil, err
	}
	if stmts == nil {
		stmts = []*Statement{}
	}

	return &listStatementsOutput{Statements: stmts}, nil
}

// runStatementInput holds input for RunStatement.
type runStatementInput struct {
	SessionID string `json:"SessionId"`
	Code      string `json:"Code"`
}

// runStatementOutput holds the result for RunStatement.
type runStatementOutput struct {
	ID int32 `json:"Id"`
}

func (h *Handler) handleRunStatement(
	_ context.Context,
	in *runStatementInput,
) (*runStatementOutput, error) {
	st, err := h.Backend.RunStatement(in.SessionID, in.Code)
	if err != nil {
		return nil, err
	}

	return &runStatementOutput{ID: st.Id}, nil
}

// stopSessionInput holds input for StopSession.
type stopSessionInput struct {
	ID string `json:"Id"`
}

// stopSessionOutput holds the result for StopSession.
type stopSessionOutput struct {
	ID string `json:"Id"`
}

func (h *Handler) handleStopSession(
	_ context.Context,
	in *stopSessionInput,
) (*stopSessionOutput, error) {
	if err := h.Backend.StopSession(in.ID); err != nil {
		return nil, err
	}

	return &stopSessionOutput{ID: in.ID}, nil
}
