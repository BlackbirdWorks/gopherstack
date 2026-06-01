package medialive

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority = service.PriorityPathVersioned

	pathPrefix              = "/prod/"
	pathChannels            = "/prod/channels"
	pathInputs              = "/prod/inputs"
	pathInputSecurityGroups = "/prod/inputSecurityGroups"
	pathTags                = "/prod/tags/"

	keyMessage = "Message"
	keyArn     = "Arn"
	keyID      = "Id"
	keyState   = "State"
	opUnknown  = "Unknown"

	opCreateChannel   = "CreateChannel"
	opDescribeChannel = "DescribeChannel"
	opUpdateChannel   = "UpdateChannel"
	opDeleteChannel   = "DeleteChannel"
	opListChannels    = "ListChannels"
	opStartChannel    = "StartChannel"
	opStopChannel     = "StopChannel"

	opCreateInput   = "CreateInput"
	opDescribeInput = "DescribeInput"
	opUpdateInput   = "UpdateInput"
	opDeleteInput   = "DeleteInput"
	opListInputs    = "ListInputs"

	opCreateInputSecurityGroup   = "CreateInputSecurityGroup"
	opDescribeInputSecurityGroup = "DescribeInputSecurityGroup"
	opUpdateInputSecurityGroup   = "UpdateInputSecurityGroup"
	opDeleteInputSecurityGroup   = "DeleteInputSecurityGroup"
	opListInputSecurityGroups    = "ListInputSecurityGroups"

	opCreateTags          = "CreateTags"
	opDeleteTags          = "DeleteTags"
	opListTagsForResource = "ListTagsForResource"
)

// Handler handles MediaLive HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaLive" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns all supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateChannel,
		opDescribeChannel,
		opUpdateChannel,
		opDeleteChannel,
		opListChannels,
		opStartChannel,
		opStopChannel,
		opCreateInput,
		opDescribeInput,
		opUpdateInput,
		opDeleteInput,
		opListInputs,
		opCreateInputSecurityGroup,
		opDescribeInputSecurityGroup,
		opUpdateInputSecurityGroup,
		opDeleteInputSecurityGroup,
		opListInputSecurityGroups,
		opCreateTags,
		opDeleteTags,
		opListTagsForResource,
	}
}

// RouteMatcher returns a function that matches MediaLive requests by path.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, pathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation classifies the request into an operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource returns the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := classifyPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func (h *Handler) handleREST(c *echo.Context) error {
	op, resource := classifyPath(c.Request().Method, c.Request().URL.Path)

	var body map[string]any
	if c.Request().ContentLength != 0 {
		if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && err.Error() != "EOF" {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "invalid JSON body"})
		}
	}

	if body == nil {
		body = map[string]any{}
	}

	handlers := map[string]func() error{
		opCreateChannel:              func() error { return h.handleCreateChannel(c, body) },
		opDescribeChannel:            func() error { return h.handleDescribeChannel(c, resource) },
		opUpdateChannel:              func() error { return h.handleUpdateChannel(c, resource, body) },
		opDeleteChannel:              func() error { return h.handleDeleteChannel(c, resource) },
		opListChannels:               func() error { return h.handleListChannels(c) },
		opStartChannel:               func() error { return h.handleStartChannel(c, resource) },
		opStopChannel:                func() error { return h.handleStopChannel(c, resource) },
		opCreateInput:                func() error { return h.handleCreateInput(c, body) },
		opDescribeInput:              func() error { return h.handleDescribeInput(c, resource) },
		opUpdateInput:                func() error { return h.handleUpdateInput(c, resource, body) },
		opDeleteInput:                func() error { return h.handleDeleteInput(c, resource) },
		opListInputs:                 func() error { return h.handleListInputs(c) },
		opCreateInputSecurityGroup:   func() error { return h.handleCreateInputSecurityGroup(c, body) },
		opDescribeInputSecurityGroup: func() error { return h.handleDescribeInputSecurityGroup(c, resource) },
		opUpdateInputSecurityGroup:   func() error { return h.handleUpdateInputSecurityGroup(c, resource, body) },
		opDeleteInputSecurityGroup:   func() error { return h.handleDeleteInputSecurityGroup(c, resource) },
		opListInputSecurityGroups:    func() error { return h.handleListInputSecurityGroups(c) },
		opCreateTags:                 func() error { return h.handleCreateTags(c, resource, body) },
		opDeleteTags:                 func() error { return h.handleDeleteTags(c, resource) },
		opListTagsForResource:        func() error { return h.handleListTagsForResource(c, resource) },
	}

	if fn, ok := handlers[op]; ok {
		return fn()
	}

	return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "unknown operation"})
}

// classifyPath maps (method, path) → (operation, resource).
func classifyPath(method, path string) (string, string) {
	if op, res, ok := classifyChannelPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyInputPath(method, path); ok {
		return op, res
	}

	if op, res, ok := classifyInputSecurityGroupPath(method, path); ok {
		return op, res
	}

	if strings.HasPrefix(path, pathTags) {
		return classifyTagPath(method, path)
	}

	return opUnknown, ""
}

func classifyChannelPath(method, path string) (string, string, bool) {
	const prefix = pathChannels + "/"

	switch {
	case path == pathChannels && method == http.MethodGet:
		return opListChannels, "", true
	case path == pathChannels && method == http.MethodPost:
		return opCreateChannel, "", true
	case matchSegment(path, prefix, "/start") && method == http.MethodPost:
		return opStartChannel, extractSegment(path, prefix, "/start"), true
	case matchSegment(path, prefix, "/stop") && method == http.MethodPost:
		return opStopChannel, extractSegment(path, prefix, "/stop"), true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opDescribeChannel, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodPut:
		return opUpdateChannel, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodDelete:
		return opDeleteChannel, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

func classifyInputPath(method, path string) (string, string, bool) {
	const prefix = pathInputs + "/"

	switch {
	case path == pathInputs && method == http.MethodGet:
		return opListInputs, "", true
	case path == pathInputs && method == http.MethodPost:
		return opCreateInput, "", true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opDescribeInput, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodPut:
		return opUpdateInput, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodDelete:
		return opDeleteInput, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

func classifyInputSecurityGroupPath(method, path string) (string, string, bool) {
	const prefix = pathInputSecurityGroups + "/"

	switch {
	case path == pathInputSecurityGroups && method == http.MethodGet:
		return opListInputSecurityGroups, "", true
	case path == pathInputSecurityGroups && method == http.MethodPost:
		return opCreateInputSecurityGroup, "", true
	case matchSegment(path, prefix, "") && method == http.MethodGet:
		return opDescribeInputSecurityGroup, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodPut:
		return opUpdateInputSecurityGroup, extractSegment(path, prefix, ""), true
	case matchSegment(path, prefix, "") && method == http.MethodDelete:
		return opDeleteInputSecurityGroup, extractSegment(path, prefix, ""), true
	}

	return "", "", false
}

func classifyTagPath(method, path string) (string, string) {
	resource := strings.TrimPrefix(path, pathTags)

	switch method {
	case http.MethodGet:
		return opListTagsForResource, resource
	case http.MethodPost:
		return opCreateTags, resource
	case http.MethodDelete:
		return opDeleteTags, resource
	}

	return opUnknown, ""
}

// matchSegment returns true when path has the form prefix+<id>+suffix.
func matchSegment(path, prefix, suffix string) bool {
	after, ok := strings.CutPrefix(path, prefix)
	if !ok {
		return false
	}

	if suffix == "" {
		return !strings.Contains(after, "/")
	}

	id, hasSuffix := strings.CutSuffix(after, suffix)

	return hasSuffix && !strings.Contains(id, "/")
}

// extractSegment extracts the <id> from prefix+<id>+suffix.
func extractSegment(path, prefix, suffix string) string {
	after, _ := strings.CutPrefix(path, prefix)
	if suffix == "" {
		return after
	}

	id, _ := strings.CutSuffix(after, suffix)

	return id
}

func errStatus(err error) int {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, awserr.ErrAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, awserr.ErrInvalidParameter):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func respondErr(c *echo.Context, err error) error {
	return c.JSON(errStatus(err), map[string]any{keyMessage: err.Error()})
}

// --- Channel handlers ---

// Tags first: reduces GC pointer scan from 104 to 96 bytes.
type channelOutput struct {
	Tags         map[string]string `json:"Tags"`
	Arn          string            `json:"Arn"`
	ID           string            `json:"Id"`
	Name         string            `json:"Name"`
	ChannelClass string            `json:"ChannelClass"`
	RoleArn      string            `json:"RoleArn"`
	State        string            `json:"State"`
}

func toChannelOutput(ch *Channel) channelOutput {
	tags := ch.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return channelOutput{
		Tags:         tags,
		Arn:          ch.ARN,
		ID:           ch.ID,
		Name:         ch.Name,
		ChannelClass: ch.ChannelClass,
		RoleArn:      ch.RoleARN,
		State:        ch.State,
	}
}

func (h *Handler) handleCreateChannel(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	channelClass, _ := body["ChannelClass"].(string)
	roleArn, _ := body["RoleArn"].(string)
	tags := extractTags(body)

	ch, err := h.Backend.CreateChannel(name, channelClass, roleArn, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"Channel": toChannelOutput(ch)})
}

func (h *Handler) handleDescribeChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.DescribeChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleUpdateChannel(c *echo.Context, channelID string, body map[string]any) error {
	name, _ := body["Name"].(string)
	roleArn, _ := body["RoleArn"].(string)

	ch, err := h.Backend.UpdateChannel(channelID, name, roleArn)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Channel": toChannelOutput(ch)})
}

func (h *Handler) handleDeleteChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.DeleteChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListChannels(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyArn:         s.ARN,
			keyID:          s.ID,
			"Name":         s.Name,
			"ChannelClass": s.ChannelClass,
			keyState:       s.State,
		})
	}

	resp := map[string]any{"Channels": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStartChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.StartChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleStopChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.StopChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

// --- Input handlers ---

// Tags first: reduces GC pointer scan from 104 to 96 bytes.
type inputOutput struct {
	Tags    map[string]string `json:"Tags"`
	Arn     string            `json:"Arn"`
	ID      string            `json:"Id"`
	Name    string            `json:"Name"`
	Type    string            `json:"Type"`
	RoleArn string            `json:"RoleArn"`
	State   string            `json:"State"`
}

func toInputOutput(inp *Input) inputOutput {
	tags := inp.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return inputOutput{
		Tags:    tags,
		Arn:     inp.ARN,
		ID:      inp.ID,
		Name:    inp.Name,
		Type:    inp.InputType,
		RoleArn: inp.RoleARN,
		State:   inp.State,
	}
}

func (h *Handler) handleCreateInput(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	inputType, _ := body["Type"].(string)
	roleArn, _ := body["RoleArn"].(string)
	tags := extractTags(body)

	inp, err := h.Backend.CreateInput(name, inputType, roleArn, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"Input": toInputOutput(inp)})
}

func (h *Handler) handleDescribeInput(c *echo.Context, inputID string) error {
	inp, err := h.Backend.DescribeInput(inputID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toInputOutput(inp))
}

func (h *Handler) handleUpdateInput(c *echo.Context, inputID string, body map[string]any) error {
	name, _ := body["Name"].(string)
	roleArn, _ := body["RoleArn"].(string)

	inp, err := h.Backend.UpdateInput(inputID, name, roleArn)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Input": toInputOutput(inp)})
}

func (h *Handler) handleDeleteInput(c *echo.Context, inputID string) error {
	if err := h.Backend.DeleteInput(inputID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInputs(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListInputs(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyArn:   s.ARN,
			keyID:    s.ID,
			"Name":   s.Name,
			"Type":   s.InputType,
			keyState: s.State,
		})
	}

	resp := map[string]any{"Inputs": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- InputSecurityGroup handlers ---

// Tags first, then strings, then slice: reduces GC pointer scan from 80 to 64 bytes.
type inputSecurityGroupOutput struct {
	Tags           map[string]string `json:"Tags"`
	Arn            string            `json:"Arn"`
	ID             string            `json:"Id"`
	State          string            `json:"State"`
	WhitelistRules []map[string]any  `json:"WhitelistRules"`
}

func toGroupOutput(g *InputSecurityGroup) inputSecurityGroupOutput {
	tags := g.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	rules := make([]map[string]any, 0, len(g.WhitelistRules))
	for _, r := range g.WhitelistRules {
		rules = append(rules, map[string]any{"Cidr": r.Cidr})
	}

	return inputSecurityGroupOutput{
		Tags:           tags,
		Arn:            g.ARN,
		ID:             g.ID,
		State:          g.State,
		WhitelistRules: rules,
	}
}

func extractWhitelistRules(body map[string]any) []WhitelistRule {
	raw, _ := body["WhitelistRules"].([]any)
	rules := make([]WhitelistRule, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		cidr, _ := m["Cidr"].(string)
		if cidr != "" {
			rules = append(rules, WhitelistRule{Cidr: cidr})
		}
	}

	return rules
}

func (h *Handler) handleCreateInputSecurityGroup(c *echo.Context, body map[string]any) error {
	rules := extractWhitelistRules(body)
	tags := extractTags(body)

	g, err := h.Backend.CreateInputSecurityGroup(rules, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"SecurityGroup": toGroupOutput(g)})
}

func (h *Handler) handleDescribeInputSecurityGroup(c *echo.Context, groupID string) error {
	g, err := h.Backend.DescribeInputSecurityGroup(groupID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toGroupOutput(g))
}

func (h *Handler) handleUpdateInputSecurityGroup(c *echo.Context, groupID string, body map[string]any) error {
	rules := extractWhitelistRules(body)

	g, err := h.Backend.UpdateInputSecurityGroup(groupID, rules)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"SecurityGroup": toGroupOutput(g)})
}

func (h *Handler) handleDeleteInputSecurityGroup(c *echo.Context, groupID string) error {
	if err := h.Backend.DeleteInputSecurityGroup(groupID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInputSecurityGroups(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListInputSecurityGroups(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyArn:   s.ARN,
			keyID:    s.ID,
			keyState: s.State,
		})
	}

	resp := map[string]any{"InputSecurityGroups": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- Tag handlers ---

func (h *Handler) handleCreateTags(c *echo.Context, resourceARN string, body map[string]any) error {
	tags := extractTags(body)

	if err := h.Backend.CreateTags(resourceARN, tags); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleDeleteTags(c *echo.Context, resourceARN string) error {
	keys := extractTagKeys(c)

	if err := h.Backend.DeleteTags(resourceARN, keys); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return respondErr(c, err)
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, map[string]any{"Tags": tags})
}

func extractTags(body map[string]any) map[string]string {
	raw, _ := body["Tags"].(map[string]any)
	if len(raw) == 0 {
		return nil
	}

	tags := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			tags[k] = s
		}
	}

	return tags
}

func extractTagKeys(c *echo.Context) []string {
	return c.Request().URL.Query()["tagKeys"]
}
