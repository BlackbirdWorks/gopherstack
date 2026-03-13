package qldbsession

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	qldbSessionService = "qldb-session"
	// qldbSessionTarget is the X-Amz-Target value for all QLDB Session SendCommand requests.
	qldbSessionTarget = "QLDBSession.SendCommand"
)

var (
	errInvalidRequest = errors.New("invalid request")
	errUnknownCommand = errors.New("unknown command in SendCommand body")
)

// Handler is the HTTP handler for the QLDB Session JSON 1.0 API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new QLDB Session handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "QLDBSession" }

// GetSupportedOperations returns the list of supported QLDB Session operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"SendCommand",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return qldbSessionService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches QLDB Session API requests.
// Routes are matched by the X-Amz-Target header value QLDBSession.SendCommand.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return c.Request().Header.Get("X-Amz-Target") == qldbSessionTarget
	}
}

// MatchPriority returns the routing priority for header-based matching.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation always returns "SendCommand" for the QLDB Session service.
func (h *Handler) ExtractOperation(_ *echo.Context) string { return "SendCommand" }

// ExtractResource returns the ledger name from the request body, if available.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req sendCommandRequest

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	if req.StartSession != nil {
		return req.StartSession.LedgerName
	}

	return ""
}

// Handler returns the Echo handler function for QLDB Session requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "qldbsession: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		result, dispErr := h.dispatch(ctx, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

// sendCommandRequest is the top-level request body for SendCommand.
type sendCommandRequest struct {
	SessionToken      *string                   `json:"SessionToken,omitempty"`
	StartSession      *startSessionRequest      `json:"StartSession,omitempty"`
	StartTransaction  *startTransactionRequest  `json:"StartTransaction,omitempty"`
	ExecuteStatement  *executeStatementRequest  `json:"ExecuteStatement,omitempty"`
	FetchPage         *fetchPageRequest         `json:"FetchPage,omitempty"`
	CommitTransaction *commitTransactionRequest `json:"CommitTransaction,omitempty"`
	AbortTransaction  *abortTransactionRequest  `json:"AbortTransaction,omitempty"`
	EndSession        *endSessionRequest        `json:"EndSession,omitempty"`
}

type startSessionRequest struct {
	LedgerName string `json:"LedgerName"`
}

type startTransactionRequest struct{}

type executeStatementRequest struct {
	Statement     string        `json:"Statement"`
	TransactionID string        `json:"TransactionId"`
	Parameters    []valueHolder `json:"Parameters,omitempty"`
}

type fetchPageRequest struct {
	TransactionID string `json:"TransactionId"`
	NextPageToken string `json:"NextPageToken"`
}

type commitTransactionRequest struct {
	TransactionID string `json:"TransactionId"`
	CommitDigest  []byte `json:"CommitDigest"`
}

type abortTransactionRequest struct{}

type endSessionRequest struct{}

// sendCommandResponse mirrors the SendCommand output structure.
type sendCommandResponse struct {
	StartSession      *startSessionResult      `json:"StartSession,omitempty"`
	StartTransaction  *startTransactionResult  `json:"StartTransaction,omitempty"`
	ExecuteStatement  *executeStatementResult  `json:"ExecuteStatement,omitempty"`
	FetchPage         *fetchPageResult         `json:"FetchPage,omitempty"`
	CommitTransaction *commitTransactionResult `json:"CommitTransaction,omitempty"`
	AbortTransaction  *abortTransactionResult  `json:"AbortTransaction,omitempty"`
	EndSession        *endSessionResult        `json:"EndSession,omitempty"`
}

type timingInformation struct {
	ProcessingTimeMilliseconds int64 `json:"ProcessingTimeMilliseconds"`
}

type ioUsage struct {
	ReadIOs  int64 `json:"ReadIOs"`
	WriteIOs int64 `json:"WriteIOs"`
}

type startSessionResult struct {
	TimingInformation *timingInformation `json:"TimingInformation,omitempty"`
	SessionToken      string             `json:"SessionToken"`
}

type startTransactionResult struct {
	TimingInformation *timingInformation `json:"TimingInformation,omitempty"`
	TransactionID     string             `json:"TransactionId"`
}

type page struct {
	NextPageToken *string       `json:"NextPageToken,omitempty"`
	Values        []valueHolder `json:"Values"`
}

type valueHolder struct {
	IonText   *string `json:"IonText,omitempty"`
	IonBinary []byte  `json:"IonBinary,omitempty"`
}

type executeStatementResult struct {
	FirstPage         *page              `json:"FirstPage,omitempty"`
	ConsumedIOs       *ioUsage           `json:"ConsumedIOs,omitempty"`
	TimingInformation *timingInformation `json:"TimingInformation,omitempty"`
}

type fetchPageResult struct {
	Page              *page              `json:"Page,omitempty"`
	ConsumedIOs       *ioUsage           `json:"ConsumedIOs,omitempty"`
	TimingInformation *timingInformation `json:"TimingInformation,omitempty"`
}

type commitTransactionResult struct {
	ConsumedIOs       *ioUsage           `json:"ConsumedIOs,omitempty"`
	TimingInformation *timingInformation `json:"TimingInformation,omitempty"`
	TransactionID     string             `json:"TransactionId,omitempty"`
	CommitDigest      []byte             `json:"CommitDigest,omitempty"`
}

type abortTransactionResult struct {
	TimingInformation *timingInformation `json:"TimingInformation,omitempty"`
}

type endSessionResult struct {
	TimingInformation *timingInformation `json:"TimingInformation,omitempty"`
}

func zeroTiming() *timingInformation {
	return &timingInformation{ProcessingTimeMilliseconds: 0}
}

func zeroIO() *ioUsage {
	return &ioUsage{ReadIOs: 0, WriteIOs: 0}
}

func (h *Handler) dispatch(ctx context.Context, body []byte) ([]byte, error) {
	var req sendCommandRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	token := ""
	if req.SessionToken != nil {
		token = *req.SessionToken
	}

	resp := &sendCommandResponse{}

	switch {
	case req.StartSession != nil:
		return h.handleStartSession(ctx, req.StartSession)
	case req.StartTransaction != nil:
		return h.handleStartTransaction(ctx, token)
	case req.ExecuteStatement != nil:
		return h.handleExecuteStatement(ctx, token, req.ExecuteStatement, resp)
	case req.FetchPage != nil:
		return h.handleFetchPage(ctx, token, req.FetchPage)
	case req.CommitTransaction != nil:
		return h.handleCommitTransaction(ctx, token, req.CommitTransaction)
	case req.AbortTransaction != nil:
		return h.handleAbortTransaction(ctx, token, req.AbortTransaction)
	case req.EndSession != nil:
		return h.handleEndSession(ctx, token)
	default:
		return nil, fmt.Errorf("%w: no command found in request body", errUnknownCommand)
	}
}

func (h *Handler) handleStartSession(_ context.Context, req *startSessionRequest) ([]byte, error) {
	if req.LedgerName == "" {
		return nil, fmt.Errorf("%w: LedgerName is required for StartSession", errInvalidRequest)
	}

	sess, err := h.Backend.StartSession(req.LedgerName)
	if err != nil {
		return nil, err
	}

	resp := &sendCommandResponse{
		StartSession: &startSessionResult{
			SessionToken:      sess.Token,
			TimingInformation: zeroTiming(),
		},
	}

	return json.Marshal(resp)
}

func (h *Handler) handleStartTransaction(_ context.Context, token string) ([]byte, error) {
	if token == "" {
		return nil, fmt.Errorf("%w: SessionToken is required for StartTransaction", errInvalidRequest)
	}

	txID, err := h.Backend.StartTransaction(token)
	if err != nil {
		return nil, err
	}

	resp := &sendCommandResponse{
		StartTransaction: &startTransactionResult{
			TransactionID:     txID,
			TimingInformation: zeroTiming(),
		},
	}

	return json.Marshal(resp)
}

func (h *Handler) handleExecuteStatement(
	_ context.Context,
	token string,
	req *executeStatementRequest,
	_ *sendCommandResponse,
) ([]byte, error) {
	if token == "" {
		return nil, fmt.Errorf("%w: SessionToken is required for ExecuteStatement", errInvalidRequest)
	}

	if req.TransactionID == "" {
		return nil, fmt.Errorf("%w: TransactionID is required for ExecuteStatement", errInvalidRequest)
	}

	// Validate the session exists.
	if _, err := h.Backend.GetSession(token); err != nil {
		return nil, err
	}

	resp := &sendCommandResponse{
		ExecuteStatement: &executeStatementResult{
			FirstPage: &page{
				Values: []valueHolder{},
			},
			ConsumedIOs:       zeroIO(),
			TimingInformation: zeroTiming(),
		},
	}

	return json.Marshal(resp)
}

func (h *Handler) handleFetchPage(_ context.Context, token string, req *fetchPageRequest) ([]byte, error) {
	if token == "" {
		return nil, fmt.Errorf("%w: SessionToken is required for FetchPage", errInvalidRequest)
	}

	if req.TransactionID == "" {
		return nil, fmt.Errorf("%w: TransactionID is required for FetchPage", errInvalidRequest)
	}

	// Validate the session exists.
	if _, err := h.Backend.GetSession(token); err != nil {
		return nil, err
	}

	resp := &sendCommandResponse{
		FetchPage: &fetchPageResult{
			Page: &page{
				Values: []valueHolder{},
			},
			ConsumedIOs:       zeroIO(),
			TimingInformation: zeroTiming(),
		},
	}

	return json.Marshal(resp)
}

func (h *Handler) handleCommitTransaction(
	_ context.Context,
	token string,
	req *commitTransactionRequest,
) ([]byte, error) {
	if token == "" {
		return nil, fmt.Errorf("%w: SessionToken is required for CommitTransaction", errInvalidRequest)
	}

	if req.TransactionID == "" {
		return nil, fmt.Errorf("%w: TransactionID is required for CommitTransaction", errInvalidRequest)
	}

	if err := h.Backend.CommitTransaction(token, req.TransactionID, req.CommitDigest); err != nil {
		return nil, err
	}

	resp := &sendCommandResponse{
		CommitTransaction: &commitTransactionResult{
			TransactionID:     req.TransactionID,
			CommitDigest:      req.CommitDigest,
			ConsumedIOs:       zeroIO(),
			TimingInformation: zeroTiming(),
		},
	}

	return json.Marshal(resp)
}

func (h *Handler) handleAbortTransaction(_ context.Context, token string, _ *abortTransactionRequest) ([]byte, error) {
	if token == "" {
		return nil, fmt.Errorf("%w: SessionToken is required for AbortTransaction", errInvalidRequest)
	}

	// For AbortTransaction, the transaction ID is not required in the AWS spec at this level.
	// We validate only that the session exists.
	if _, err := h.Backend.GetSession(token); err != nil {
		return nil, err
	}

	resp := &sendCommandResponse{
		AbortTransaction: &abortTransactionResult{
			TimingInformation: zeroTiming(),
		},
	}

	return json.Marshal(resp)
}

func (h *Handler) handleEndSession(_ context.Context, token string) ([]byte, error) {
	if token == "" {
		return nil, fmt.Errorf("%w: SessionToken is required for EndSession", errInvalidRequest)
	}

	if err := h.Backend.EndSession(token); err != nil {
		return nil, err
	}

	resp := &sendCommandResponse{
		EndSession: &endSessionResult{
			TimingInformation: zeroTiming(),
		},
	}

	return json.Marshal(resp)
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrSessionNotFound), errors.Is(err, ErrTransactionNotFound),
		errors.Is(err, ErrNoActiveTransaction):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "InvalidSessionException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownCommand),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "BadRequestException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"__type":  "InternalFailureException",
			"message": err.Error(),
		})
	}
}
