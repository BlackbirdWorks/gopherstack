package appsync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/validator/rules"
)

const (
	rtConnectionTimeoutMs = 300000
	rtWriteTimeout        = 5 * time.Second
	rtSubprotocol         = "graphql-ws"
	rtSegsRealtime        = 5
	rtMsgError            = "error"
	keyMessage            = "message"
	keyPayload            = "payload"
	rtErrBadRequest       = "BadRequestException"
	rtErrUnauthorized     = "UnauthorizedException"
)

type rtConn struct {
	ws *websocket.Conn
	mu sync.Mutex
}

func (c *rtConn) send(msg any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_ = c.ws.SetWriteDeadline(time.Now().Add(rtWriteTimeout))

	return c.ws.WriteJSON(msg)
}

type rtSub struct {
	conn      *rtConn
	args      map[string]any
	id        string
	alias     string
	apiID     string
	mutations []string
	set       ast.SelectionSet
}

type rtMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type rtStartPayload struct {
	Extensions struct {
		Authorization map[string]string `json:"authorization"`
	} `json:"extensions"`
	Data string `json:"data"`
}

func rtError(id, errType, msg string) map[string]any {
	return map[string]any{
		keyType: rtMsgError, "id": id,
		keyPayload: map[string]any{"errors": []map[string]string{{"errorType": errType, keyMessage: msg}}},
	}
}

// realtimeAuthFromQuery decodes the base64 "header" query parameter of the connection URL.
func realtimeAuthFromQuery(r *http.Request) map[string]string {
	out := map[string]string{}

	raw := r.URL.Query().Get("header")
	if raw == "" {
		return out
	}

	dec, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		dec, err = base64.RawURLEncoding.DecodeString(raw)
		if err != nil {
			return out
		}
	}

	_ = json.Unmarshal(dec, &out)

	return out
}

func (h *Handler) handleRealtime(ctx context.Context, c *echo.Context, apiID string) error {
	if !websocket.IsWebSocketUpgrade(c.Request()) {
		return c.JSON(http.StatusBadRequest, errorResponse(rtErrBadRequest, "websocket upgrade required"))
	}

	backend, isMem := h.Backend.(*InMemoryBackend)
	if !isMem {
		return c.JSON(http.StatusNotImplemented, errorResponse("NotImplemented", "real-time unsupported by backend"))
	}

	upgrader := websocket.Upgrader{
		Subprotocols: []string{rtSubprotocol},
		CheckOrigin:  func(*http.Request) bool { return true },
	}

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return nil
	}

	conn := &rtConn{ws: ws}
	defaultAuth := realtimeAuthFromQuery(c.Request())

	defer func() {
		backend.dropConn(conn)
		_ = ws.Close()
	}()

	for {
		var msg rtMessage
		if readErr := ws.ReadJSON(&msg); readErr != nil {
			return nil
		}

		switch msg.Type {
		case "connection_init":
			_ = conn.send(map[string]any{
				keyType:    "connection_ack",
				keyPayload: map[string]any{"connectionTimeoutMs": rtConnectionTimeoutMs},
			})
			_ = conn.send(map[string]string{keyType: "ka"})
		case "start":
			backend.startSubscription(ctx, apiID, conn, msg, defaultAuth, c.Request())
		case "stop":
			backend.stopSubscription(conn, msg.ID)
		}
	}
}

func (b *InMemoryBackend) startSubscription(
	ctx context.Context, apiID string, conn *rtConn, msg rtMessage, defaultAuth map[string]string, r *http.Request,
) {
	var payload rtStartPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		_ = conn.send(rtError(msg.ID, rtErrBadRequest, "invalid start payload"))

		return
	}

	var req graphqlRequest
	if err := json.Unmarshal([]byte(payload.Data), &req); err != nil {
		_ = conn.send(rtError(msg.ID, rtErrBadRequest, "payload.data must be a JSON string"))

		return
	}

	authz := payload.Extensions.Authorization
	if len(authz) == 0 {
		authz = defaultAuth
	}

	auth := GraphQLAuth{
		APIKey:    authz["x-api-key"],
		AuthToken: authz["Authorization"],
		Request:   r,
	}

	sub, err := b.prepareSubscription(ctx, apiID, req, auth)
	if err != nil {
		errType := rtErrBadRequest
		if errors.Is(err, ErrUnauthorized) {
			errType = rtErrUnauthorized
		}

		_ = conn.send(rtError(msg.ID, errType, err.Error()))

		return
	}

	sub.conn, sub.id = conn, msg.ID

	b.mu.Lock("StartSubscription")
	if b.rtSubs == nil {
		b.rtSubs = make(map[string]map[*rtSub]struct{})
	}

	if b.rtSubs[apiID] == nil {
		b.rtSubs[apiID] = make(map[*rtSub]struct{})
	}

	b.rtSubs[apiID][sub] = struct{}{}
	b.mu.Unlock()

	_ = conn.send(map[string]string{keyType: "start_ack", "id": msg.ID})
}

func (b *InMemoryBackend) prepareSubscription(
	ctx context.Context, apiID string, req graphqlRequest, auth GraphQLAuth,
) (*rtSub, error) {
	schema, err := b.authorizeRealtime(ctx, apiID, req, auth)
	if err != nil {
		return nil, err
	}

	if schema == nil || schema.SDL == "" {
		return nil, ErrNoSchema
	}

	gqlSchema := schema.parsedSchema

	if gqlSchema == nil {
		if gqlSchema, err = loadAppSyncSchema(schema.SDL); err != nil {
			return nil, fmt.Errorf("invalid schema: %w", err)
		}
	}

	doc, errs := gqlparser.LoadQueryWithRules(gqlSchema, req.Query, (*rules.Rules)(nil))
	if len(errs) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrQueryParse, errs[0].Message)
	}

	field, err := subscriptionField(findOperation(doc, req.OperationName))
	if err != nil {
		return nil, err
	}

	dir := field.Definition.Directives.ForName("aws_subscribe")
	if dir == nil {
		return nil, fmt.Errorf("%w: field %s has no @aws_subscribe directive", ErrQueryParse, field.Name)
	}

	var mutations []string

	if arg := dir.Arguments.ForName("mutations"); arg != nil && arg.Value != nil {
		for _, ch := range arg.Value.Children {
			mutations = append(mutations, ch.Value.Raw)
		}
	}

	return &rtSub{
		apiID:     apiID,
		alias:     field.Alias,
		mutations: mutations,
		set:       field.SelectionSet,
		args:      extractArguments(field, req.Variables),
	}, nil
}

func (b *InMemoryBackend) authorizeRealtime(
	ctx context.Context, apiID string, req graphqlRequest, auth GraphQLAuth,
) (*Schema, error) {
	b.mu.RLock("PrepareSubscription")
	api, apiOK := b.apis.Get(apiID)
	schema, _ := b.schemas.Get(apiID)

	var (
		primary    graphQLAuthConfig
		additional []AdditionalAuthenticationProvider
	)

	if apiOK {
		primary = graphQLAuthConfigFromAPI(api)
		additional = api.AdditionalAuthenticationProviders
	}

	b.mu.RUnlock()

	if !apiOK {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	_, err := b.authorizeGraphQL(ctx, apiID, primary, additional, req.Query, req.OperationName, req.Variables, auth)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func subscriptionField(op *ast.OperationDefinition) (*ast.Field, error) {
	if op == nil || op.Operation != ast.Subscription || len(op.SelectionSet) != 1 {
		return nil, fmt.Errorf("%w: expected a single subscription field", ErrQueryParse)
	}

	field, ok := op.SelectionSet[0].(*ast.Field)
	if !ok || field.Definition == nil {
		return nil, fmt.Errorf("%w: expected a single subscription field", ErrQueryParse)
	}

	return field, nil
}

func (b *InMemoryBackend) stopSubscription(conn *rtConn, id string) {
	b.mu.Lock("StopSubscription")
	for apiID, subs := range b.rtSubs {
		for s := range subs {
			if s.conn == conn && s.id == id {
				delete(b.rtSubs[apiID], s)
			}
		}
	}
	b.mu.Unlock()

	_ = conn.send(map[string]string{keyType: "complete", "id": id})
}

func (b *InMemoryBackend) dropConn(conn *rtConn) {
	b.mu.Lock("DropConnection")
	defer b.mu.Unlock()

	for apiID, subs := range b.rtSubs {
		for s := range subs {
			if s.conn == conn {
				delete(b.rtSubs[apiID], s)
			}
		}
	}
}

// publishMutation fans a mutation's result out to matching subscriptions.
func (b *InMemoryBackend) publishMutation(apiID string, op *ast.OperationDefinition, result map[string]any) {
	if op.Operation != ast.Mutation {
		return
	}

	b.mu.RLock("PublishMutation")
	subs := make([]*rtSub, 0, len(b.rtSubs[apiID]))

	for s := range b.rtSubs[apiID] {
		subs = append(subs, s)
	}
	b.mu.RUnlock()

	if len(subs) == 0 {
		return
	}

	for _, sel := range op.SelectionSet {
		field, ok := sel.(*ast.Field)
		if !ok {
			continue
		}

		val := result[field.Alias]
		for _, s := range subs {
			if slices.Contains(s.mutations, field.Name) && subscriptionArgsMatch(s.args, val) {
				_ = s.conn.send(map[string]any{
					keyType: "data", "id": s.id,
					keyPayload: map[string]any{"data": map[string]any{s.alias: projectSelection(val, s.set)}},
				})
			}
		}
	}
}

func subscriptionArgsMatch(args map[string]any, val any) bool {
	if len(args) == 0 {
		return true
	}

	m, ok := val.(map[string]any)
	if !ok {
		return false
	}

	for k, want := range args {
		got, has := m[k]
		if !has || fmt.Sprint(got) != fmt.Sprint(want) {
			return false
		}
	}

	return true
}
