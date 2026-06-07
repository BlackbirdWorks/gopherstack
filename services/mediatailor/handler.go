package mediatailor

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

	pathChannels         = "/channels"
	pathChannel          = "/channel/"
	pathPlaybackConfig   = "/playbackConfiguration"
	pathPlaybackConfigs  = "/playbackConfigurations"
	pathSourceLocations  = "/sourceLocations"
	pathSourceLocation   = "/sourceLocation/"
	pathTags             = "/tags/"
	pathPrefetchSchedule = "/prefetchSchedule/"
	pathFunction         = "/function/"
	pathFunctions        = "/functions"
	pathAlerts           = "/alerts"
	pathConfigureLogs    = "/configureLogs/"

	keyMessage            = "Message"
	keyTags               = "Tags"
	keyItems              = "Items"
	keyArn                = "Arn"
	keySourceLocationName = "SourceLocationName"
	keyName               = "Name"
	keyChannelName        = "ChannelName"
	keySourceGroup        = "SourceGroup"
	keyVodSourceName      = "VodSourceName"
	keyLiveSourceName     = "LiveSourceName"

	splitTwo   = 2
	splitThree = 3

	opUnknown = "Unknown"

	opPutPlaybackConfiguration    = "PutPlaybackConfiguration"
	opGetPlaybackConfiguration    = "GetPlaybackConfiguration"
	opDeletePlaybackConfiguration = "DeletePlaybackConfiguration"
	opListPlaybackConfigurations  = "ListPlaybackConfigurations"

	opCreateChannel   = "CreateChannel"
	opDescribeChannel = "DescribeChannel"
	opUpdateChannel   = "UpdateChannel"
	opDeleteChannel   = "DeleteChannel"
	opListChannels    = "ListChannels"
	opStartChannel    = "StartChannel"
	opStopChannel     = "StopChannel"

	opCreateSourceLocation   = "CreateSourceLocation"
	opDescribeSourceLocation = "DescribeSourceLocation"
	opUpdateSourceLocation   = "UpdateSourceLocation"
	opDeleteSourceLocation   = "DeleteSourceLocation"
	opListSourceLocations    = "ListSourceLocations"

	opCreateVodSource   = "CreateVodSource"
	opDescribeVodSource = "DescribeVodSource"
	opUpdateVodSource   = "UpdateVodSource"
	opDeleteVodSource   = "DeleteVodSource"
	opListVodSources    = "ListVodSources"

	opListTagsForResource = "ListTagsForResource"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"

	opCreateLiveSource   = "CreateLiveSource"
	opDescribeLiveSource = "DescribeLiveSource"
	opUpdateLiveSource   = "UpdateLiveSource"
	opDeleteLiveSource   = "DeleteLiveSource"
	opListLiveSources    = "ListLiveSources"

	opCreatePrefetchSchedule = "CreatePrefetchSchedule"
	opGetPrefetchSchedule    = "GetPrefetchSchedule"
	opDeletePrefetchSchedule = "DeletePrefetchSchedule"
	opListPrefetchSchedules  = "ListPrefetchSchedules"

	opCreateProgram      = "CreateProgram"
	opDescribeProgram    = "DescribeProgram"
	opUpdateProgram      = "UpdateProgram"
	opDeleteProgram      = "DeleteProgram"
	opGetChannelSchedule = "GetChannelSchedule"

	opPutChannelPolicy    = "PutChannelPolicy"
	opGetChannelPolicy    = "GetChannelPolicy"
	opDeleteChannelPolicy = "DeleteChannelPolicy"

	opPutFunction    = "PutFunction"
	opGetFunction    = "GetFunction"
	opDeleteFunction = "DeleteFunction"
	opListFunctions  = "ListFunctions"

	opListAlerts = "ListAlerts"

	opConfigureLogsForChannel               = "ConfigureLogsForChannel"
	opConfigureLogsForPlaybackConfiguration = "ConfigureLogsForPlaybackConfiguration"
)

// Handler handles MediaTailor HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MediaTailor" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns all supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opPutPlaybackConfiguration,
		opGetPlaybackConfiguration,
		opDeletePlaybackConfiguration,
		opListPlaybackConfigurations,
		opCreateChannel,
		opDescribeChannel,
		opUpdateChannel,
		opDeleteChannel,
		opListChannels,
		opStartChannel,
		opStopChannel,
		opCreateSourceLocation,
		opDescribeSourceLocation,
		opUpdateSourceLocation,
		opDeleteSourceLocation,
		opListSourceLocations,
		opCreateVodSource,
		opDescribeVodSource,
		opUpdateVodSource,
		opDeleteVodSource,
		opListVodSources,
		opListTagsForResource,
		opTagResource,
		opUntagResource,
		opCreateLiveSource,
		opDescribeLiveSource,
		opUpdateLiveSource,
		opDeleteLiveSource,
		opListLiveSources,
		opCreatePrefetchSchedule,
		opGetPrefetchSchedule,
		opDeletePrefetchSchedule,
		opListPrefetchSchedules,
		opCreateProgram,
		opDescribeProgram,
		opUpdateProgram,
		opDeleteProgram,
		opGetChannelSchedule,
		opPutChannelPolicy,
		opGetChannelPolicy,
		opDeleteChannelPolicy,
		opPutFunction,
		opGetFunction,
		opDeleteFunction,
		opListFunctions,
		opListAlerts,
		opConfigureLogsForChannel,
		opConfigureLogsForPlaybackConfiguration,
	}
}

// RouteMatcher returns a function that matches MediaTailor requests by path.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return isMediaTailorPath(path)
	}
}

func isMediaTailorPath(path string) bool {
	return path == pathPlaybackConfig ||
		strings.HasPrefix(path, pathPlaybackConfig+"/") ||
		path == pathPlaybackConfigs ||
		path == pathChannels ||
		strings.HasPrefix(path, pathChannel) ||
		path == pathSourceLocations ||
		strings.HasPrefix(path, pathSourceLocation) ||
		strings.HasPrefix(path, pathPrefetchSchedule) ||
		path == pathFunctions ||
		strings.HasPrefix(path, pathFunction) ||
		path == pathAlerts ||
		strings.HasPrefix(path, pathConfigureLogs) ||
		isMediaTailorTagPath(path)
}

// isMediaTailorTagPath reports whether path is a /tags/{arn} path for a MediaTailor ARN.
// Other services (e.g. FIS) also expose /tags/{arn} at the same path prefix; we must not
// steal their requests. MediaTailor ARNs always contain ":mediatailor:".
func isMediaTailorTagPath(path string) bool {
	arn, ok := strings.CutPrefix(path, pathTags)

	return ok && strings.Contains(arn, ":mediatailor:")
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation classifies the request into an operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource returns the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

func (h *Handler) handleREST(c *echo.Context) error {
	op, resource, extra := classifyPath(c.Request().Method, c.Request().URL.Path)

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
		opPutPlaybackConfiguration:    func() error { return h.handlePutPlaybackConfiguration(c, body) },
		opGetPlaybackConfiguration:    func() error { return h.handleGetPlaybackConfiguration(c, resource) },
		opDeletePlaybackConfiguration: func() error { return h.handleDeletePlaybackConfiguration(c, resource) },
		opListPlaybackConfigurations:  func() error { return h.handleListPlaybackConfigurations(c) },

		opCreateChannel:   func() error { return h.handleCreateChannel(c, resource, body) },
		opDescribeChannel: func() error { return h.handleDescribeChannel(c, resource) },
		opUpdateChannel:   func() error { return h.handleUpdateChannel(c, resource, body) },
		opDeleteChannel:   func() error { return h.handleDeleteChannel(c, resource) },
		opListChannels:    func() error { return h.handleListChannels(c) },
		opStartChannel:    func() error { return h.handleStartChannel(c, resource) },
		opStopChannel:     func() error { return h.handleStopChannel(c, resource) },

		opCreateSourceLocation:   func() error { return h.handleCreateSourceLocation(c, resource, body) },
		opDescribeSourceLocation: func() error { return h.handleDescribeSourceLocation(c, resource) },
		opUpdateSourceLocation:   func() error { return h.handleUpdateSourceLocation(c, resource, body) },
		opDeleteSourceLocation:   func() error { return h.handleDeleteSourceLocation(c, resource) },
		opListSourceLocations:    func() error { return h.handleListSourceLocations(c) },

		opCreateVodSource:   func() error { return h.handleCreateVodSource(c, resource, extra, body) },
		opDescribeVodSource: func() error { return h.handleDescribeVodSource(c, resource, extra) },
		opUpdateVodSource:   func() error { return h.handleUpdateVodSource(c, resource, extra, body) },
		opDeleteVodSource:   func() error { return h.handleDeleteVodSource(c, resource, extra) },
		opListVodSources:    func() error { return h.handleListVodSources(c, resource) },

		opListTagsForResource: func() error { return h.handleListTagsForResource(c, resource) },
		opTagResource:         func() error { return h.handleTagResource(c, resource, body) },
		opUntagResource:       func() error { return h.handleUntagResource(c, resource) },

		opCreateLiveSource:   func() error { return h.handleCreateLiveSource(c, resource, extra, body) },
		opDescribeLiveSource: func() error { return h.handleDescribeLiveSource(c, resource, extra) },
		opUpdateLiveSource:   func() error { return h.handleUpdateLiveSource(c, resource, extra, body) },
		opDeleteLiveSource:   func() error { return h.handleDeleteLiveSource(c, resource, extra) },
		opListLiveSources:    func() error { return h.handleListLiveSources(c, resource) },

		opCreatePrefetchSchedule: func() error { return h.handleCreatePrefetchSchedule(c, resource, extra) },
		opGetPrefetchSchedule:    func() error { return h.handleGetPrefetchSchedule(c, resource, extra) },
		opDeletePrefetchSchedule: func() error { return h.handleDeletePrefetchSchedule(c, resource, extra) },
		opListPrefetchSchedules:  func() error { return h.handleListPrefetchSchedules(c, resource) },

		opCreateProgram:      func() error { return h.handleCreateProgram(c, resource, extra, body) },
		opDescribeProgram:    func() error { return h.handleDescribeProgram(c, resource, extra) },
		opUpdateProgram:      func() error { return h.handleUpdateProgram(c, resource, extra) },
		opDeleteProgram:      func() error { return h.handleDeleteProgram(c, resource, extra) },
		opGetChannelSchedule: func() error { return h.handleGetChannelSchedule(c, resource) },

		opPutChannelPolicy:    func() error { return h.handlePutChannelPolicy(c, resource, body) },
		opGetChannelPolicy:    func() error { return h.handleGetChannelPolicy(c, resource) },
		opDeleteChannelPolicy: func() error { return h.handleDeleteChannelPolicy(c, resource) },

		opPutFunction:    func() error { return h.handlePutFunction(c, resource, body) },
		opGetFunction:    func() error { return h.handleGetFunction(c, resource) },
		opDeleteFunction: func() error { return h.handleDeleteFunction(c, resource) },
		opListFunctions:  func() error { return h.handleListFunctions(c) },

		opListAlerts: func() error { return h.handleListAlerts(c) },

		opConfigureLogsForChannel: func() error { return h.handleConfigureLogsForChannel(c, body) },
		opConfigureLogsForPlaybackConfiguration: func() error {
			return h.handleConfigureLogsForPlaybackConfiguration(c, body)
		},
	}

	if fn, ok := handlers[op]; ok {
		return fn()
	}

	return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "unknown operation"})
}

// classifyPath maps (method, path) → (operation, resource, extra).
// resource is the primary resource name; extra is a secondary name (e.g., vodSourceName).
func classifyPath(method, path string) (string, string, string) {
	if op, res, ok := classifyPlaybackConfigPath(method, path); ok {
		return op, res, ""
	}

	if op, res, extra, ok := classifyChannelPath(method, path); ok {
		return op, res, extra
	}

	if op, res, extra, ok := classifySourceLocationPath(method, path); ok {
		return op, res, extra
	}

	if op, res, extra, ok := classifyPrefetchSchedulePath(method, path); ok {
		return op, res, extra
	}

	if op, res, ok := classifyFunctionPath(method, path); ok {
		return op, res, ""
	}

	if op, ok := classifyConfigureLogsPath(method, path); ok {
		return op, "", ""
	}

	if path == pathAlerts && method == http.MethodGet {
		return opListAlerts, "", ""
	}

	if tagARN, ok := strings.CutPrefix(path, pathTags); ok {
		return classifyTagPath(method), tagARN, ""
	}

	return opUnknown, "", ""
}

func classifyPlaybackConfigPath(method, path string) (string, string, bool) {
	const prefix = pathPlaybackConfig + "/"

	switch {
	case path == pathPlaybackConfigs && method == http.MethodGet:
		return opListPlaybackConfigurations, "", true
	case path == pathPlaybackConfig && method == http.MethodPut:
		return opPutPlaybackConfiguration, "", true
	}

	name, ok := strings.CutPrefix(path, prefix)
	if !ok || strings.Contains(name, "/") {
		return "", "", false
	}

	switch method {
	case http.MethodGet:
		return opGetPlaybackConfiguration, name, true
	case http.MethodDelete:
		return opDeletePlaybackConfiguration, name, true
	}

	return "", "", false
}

func classifyChannelPath(method, path string) (string, string, string, bool) {
	if path == pathChannels && method == http.MethodGet {
		return opListChannels, "", "", true
	}

	after, ok := strings.CutPrefix(path, pathChannel)
	if !ok {
		return "", "", "", false
	}

	parts := strings.SplitN(after, "/", splitThree)
	channelName := parts[0]

	if len(parts) == 1 {
		return classifyChannelByMethod(method, channelName)
	}

	return classifyChannelSubPath(method, channelName, parts[1], strings.Join(parts[2:], "/"))
}

func classifyChannelByMethod(method, channelName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateChannel, channelName, "", true
	case http.MethodGet:
		return opDescribeChannel, channelName, "", true
	case http.MethodPut:
		return opUpdateChannel, channelName, "", true
	case http.MethodDelete:
		return opDeleteChannel, channelName, "", true
	}

	return "", "", "", false
}

func classifyChannelSubPath(method, channelName, subKey, extra string) (string, string, string, bool) {
	switch {
	case subKey == "start" && method == http.MethodPut:
		return opStartChannel, channelName, "", true
	case subKey == "stop" && method == http.MethodPut:
		return opStopChannel, channelName, "", true
	case subKey == "schedule" && method == http.MethodGet:
		return opGetChannelSchedule, channelName, "", true
	case subKey == "policy":
		return classifyChannelPolicyByMethod(method, channelName)
	case subKey == "program" && extra != "" && !strings.Contains(extra, "/"):
		return classifyProgramByMethod(method, channelName, extra)
	}

	return "", "", "", false
}

func classifyChannelPolicyByMethod(method, channelName string) (string, string, string, bool) {
	switch method {
	case http.MethodPut:
		return opPutChannelPolicy, channelName, "", true
	case http.MethodGet:
		return opGetChannelPolicy, channelName, "", true
	case http.MethodDelete:
		return opDeleteChannelPolicy, channelName, "", true
	}

	return "", "", "", false
}

func classifyProgramByMethod(method, channelName, programName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateProgram, channelName, programName, true
	case http.MethodGet:
		return opDescribeProgram, channelName, programName, true
	case http.MethodPut:
		return opUpdateProgram, channelName, programName, true
	case http.MethodDelete:
		return opDeleteProgram, channelName, programName, true
	}

	return "", "", "", false
}

// classifySourceLocationPath returns (op, sourceLocationName, secondaryName, ok).
func classifySourceLocationPath(method, path string) (string, string, string, bool) {
	if path == pathSourceLocations && method == http.MethodGet {
		return opListSourceLocations, "", "", true
	}

	after, ok := strings.CutPrefix(path, pathSourceLocation)
	if !ok {
		return "", "", "", false
	}

	// format: {slName} | {slName}/vodSources | {slName}/liveSources |
	//         {slName}/vodSource/{name} | {slName}/liveSource/{name}
	parts := strings.SplitN(after, "/", splitThree)
	slName := parts[0]

	switch len(parts) {
	case 1:
		return classifySourceLocationByMethod(method, slName)
	case splitTwo:
		if parts[1] == "vodSources" && method == http.MethodGet {
			return opListVodSources, slName, "", true
		}

		if parts[1] == "liveSources" && method == http.MethodGet {
			return opListLiveSources, slName, "", true
		}
	case splitThree:
		if parts[1] == "vodSource" {
			return classifyVodSourceByMethod(method, slName, parts[2])
		}

		if parts[1] == "liveSource" {
			return classifyLiveSourceByMethod(method, slName, parts[2])
		}
	}

	return "", "", "", false
}

func classifySourceLocationByMethod(method, slName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateSourceLocation, slName, "", true
	case http.MethodGet:
		return opDescribeSourceLocation, slName, "", true
	case http.MethodPut:
		return opUpdateSourceLocation, slName, "", true
	case http.MethodDelete:
		return opDeleteSourceLocation, slName, "", true
	}

	return "", "", "", false
}

func classifyVodSourceByMethod(method, slName, vodName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateVodSource, slName, vodName, true
	case http.MethodGet:
		return opDescribeVodSource, slName, vodName, true
	case http.MethodPut:
		return opUpdateVodSource, slName, vodName, true
	case http.MethodDelete:
		return opDeleteVodSource, slName, vodName, true
	}

	return "", "", "", false
}

func classifyLiveSourceByMethod(method, slName, lsName string) (string, string, string, bool) {
	switch method {
	case http.MethodPost:
		return opCreateLiveSource, slName, lsName, true
	case http.MethodGet:
		return opDescribeLiveSource, slName, lsName, true
	case http.MethodPut:
		return opUpdateLiveSource, slName, lsName, true
	case http.MethodDelete:
		return opDeleteLiveSource, slName, lsName, true
	}

	return "", "", "", false
}

// classifyPrefetchSchedulePath handles /prefetchSchedule/{pc} and /prefetchSchedule/{pc}/{name}.
func classifyPrefetchSchedulePath(method, path string) (string, string, string, bool) {
	after, ok := strings.CutPrefix(path, pathPrefetchSchedule)
	if !ok {
		return "", "", "", false
	}

	parts := strings.SplitN(after, "/", splitTwo)
	pcName := parts[0]

	if len(parts) == 1 {
		if method == http.MethodGet {
			return opListPrefetchSchedules, pcName, "", true
		}

		return "", "", "", false
	}

	scheduleName := parts[1]
	switch method {
	case http.MethodPost:
		return opCreatePrefetchSchedule, pcName, scheduleName, true
	case http.MethodGet:
		return opGetPrefetchSchedule, pcName, scheduleName, true
	case http.MethodDelete:
		return opDeletePrefetchSchedule, pcName, scheduleName, true
	}

	return "", "", "", false
}

// classifyFunctionPath handles /function/{id} and /functions.
func classifyFunctionPath(method, path string) (string, string, bool) {
	if path == pathFunctions && method == http.MethodGet {
		return opListFunctions, "", true
	}

	fnID, ok := strings.CutPrefix(path, pathFunction)
	if !ok || strings.Contains(fnID, "/") {
		return "", "", false
	}

	switch method {
	case http.MethodPut:
		return opPutFunction, fnID, true
	case http.MethodGet:
		return opGetFunction, fnID, true
	case http.MethodDelete:
		return opDeleteFunction, fnID, true
	}

	return "", "", false
}

// classifyConfigureLogsPath handles /configureLogs/channel and /configureLogs/playbackConfiguration.
func classifyConfigureLogsPath(method, path string) (string, bool) {
	if method != http.MethodPut {
		return "", false
	}

	suffix, ok := strings.CutPrefix(path, pathConfigureLogs)
	if !ok {
		return "", false
	}

	switch suffix {
	case "channel":
		return opConfigureLogsForChannel, true
	case "playbackConfiguration":
		return opConfigureLogsForPlaybackConfiguration, true
	}

	return "", false
}

func classifyTagPath(method string) string {
	switch method {
	case http.MethodGet:
		return opListTagsForResource
	case http.MethodPost:
		return opTagResource
	case http.MethodDelete:
		return opUntagResource
	}

	return opUnknown
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

// --- PlaybackConfiguration handlers ---

func (h *Handler) handlePutPlaybackConfiguration(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	adsURL, _ := body["AdDecisionServerUrl"].(string)
	videoURL, _ := body["VideoContentSourceUrl"].(string)
	tags := extractTags(body)

	cfg, err := h.Backend.PutPlaybackConfiguration(name, adsURL, videoURL, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toPlaybackConfigOutput(cfg))
}

func (h *Handler) handleGetPlaybackConfiguration(c *echo.Context, name string) error {
	cfg, err := h.Backend.GetPlaybackConfiguration(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toPlaybackConfigOutput(cfg))
}

func (h *Handler) handleDeletePlaybackConfiguration(c *echo.Context, name string) error {
	if err := h.Backend.DeletePlaybackConfiguration(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListPlaybackConfigurations(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListPlaybackConfigurations(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyName:                    s.Name,
			"PlaybackConfigurationArn": s.PlaybackConfigurationARN,
			"AdDecisionServerUrl":      s.AdDecisionServerURL,
			"VideoContentSourceUrl":    s.VideoContentSourceURL,
			keyTags:                    nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toPlaybackConfigOutput(cfg *PlaybackConfiguration) map[string]any {
	return map[string]any{
		keyName:                               cfg.Name,
		"PlaybackConfigurationArn":            cfg.PlaybackConfigurationARN,
		"AdDecisionServerUrl":                 cfg.AdDecisionServerURL,
		"VideoContentSourceUrl":               cfg.VideoContentSourceURL,
		"PlaybackEndpointPrefix":              cfg.PlaybackEndpointPrefix,
		"SessionInitializationEndpointPrefix": cfg.SessionInitializationPrefix,
		keyTags:                               nilToEmpty(cfg.Tags),
	}
}

// --- Channel handlers ---

func (h *Handler) handleCreateChannel(c *echo.Context, name string, body map[string]any) error {
	playbackMode, _ := body["PlaybackMode"].(string)
	outputs := extractOutputs(body)
	tags := extractTags(body)

	ch, err := h.Backend.CreateChannel(name, playbackMode, outputs, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleDescribeChannel(c *echo.Context, name string) error {
	ch, err := h.Backend.DescribeChannel(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleUpdateChannel(c *echo.Context, name string, body map[string]any) error {
	outputs := extractOutputs(body)

	ch, err := h.Backend.UpdateChannel(name, outputs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleDeleteChannel(c *echo.Context, name string) error {
	if err := h.Backend.DeleteChannel(name); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListChannels(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyChannelName: s.Name,
			keyArn:         s.ARN,
			"PlaybackMode": s.PlaybackMode,
			"ChannelState": s.ChannelState,
			keyTags:        nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStartChannel(c *echo.Context, name string) error {
	if err := h.Backend.StartChannel(name); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStopChannel(c *echo.Context, name string) error {
	if err := h.Backend.StopChannel(name); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func toChannelOutput(ch *Channel) map[string]any {
	outputs := make([]map[string]any, 0, len(ch.Outputs))
	for _, o := range ch.Outputs {
		out := map[string]any{
			"ManifestName": o.ManifestName,
			keySourceGroup: o.SourceGroup,
		}
		if o.HlsPlaylistSettings != nil {
			out["HlsPlaylistSettings"] = map[string]any{
				"ManifestWindowSeconds": o.HlsPlaylistSettings.ManifestWindowSeconds,
			}
		}
		outputs = append(outputs, out)
	}

	return map[string]any{
		keyChannelName: ch.Name,
		keyArn:         ch.ARN,
		"PlaybackMode": ch.PlaybackMode,
		"ChannelState": ch.ChannelState,
		"Outputs":      outputs,
		keyTags:        nilToEmpty(ch.Tags),
	}
}

// --- SourceLocation handlers ---

func (h *Handler) handleCreateSourceLocation(c *echo.Context, name string, body map[string]any) error {
	baseURL := extractBaseURL(body)
	tags := extractTags(body)

	sl, err := h.Backend.CreateSourceLocation(name, baseURL, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toSourceLocationOutput(sl))
}

func (h *Handler) handleDescribeSourceLocation(c *echo.Context, name string) error {
	sl, err := h.Backend.DescribeSourceLocation(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toSourceLocationOutput(sl))
}

func (h *Handler) handleUpdateSourceLocation(c *echo.Context, name string, body map[string]any) error {
	baseURL := extractBaseURL(body)

	sl, err := h.Backend.UpdateSourceLocation(name, baseURL)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toSourceLocationOutput(sl))
}

func (h *Handler) handleDeleteSourceLocation(c *echo.Context, name string) error {
	if err := h.Backend.DeleteSourceLocation(name); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListSourceLocations(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListSourceLocations(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keySourceLocationName: s.Name,
			keyArn:                s.ARN,
			"HttpConfiguration": map[string]any{
				"BaseUrl": s.HTTPConfigurationURL,
			},
			keyTags: nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toSourceLocationOutput(sl *SourceLocation) map[string]any {
	return map[string]any{
		keySourceLocationName: sl.Name,
		keyArn:                sl.ARN,
		"HttpConfiguration": map[string]any{
			"BaseUrl": sl.HTTPConfigurationURL,
		},
		keyTags: nilToEmpty(sl.Tags),
	}
}

// --- VodSource handlers ---

func (h *Handler) handleCreateVodSource(
	c *echo.Context,
	sourceLocationName, vodSourceName string,
	body map[string]any,
) error {
	cfgs := extractHTTPPackageConfigurations(body)
	tags := extractTags(body)

	vs, err := h.Backend.CreateVodSource(sourceLocationName, vodSourceName, cfgs, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toVodSourceOutput(vs))
}

func (h *Handler) handleDescribeVodSource(c *echo.Context, sourceLocationName, vodSourceName string) error {
	vs, err := h.Backend.DescribeVodSource(sourceLocationName, vodSourceName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toVodSourceOutput(vs))
}

func (h *Handler) handleUpdateVodSource(
	c *echo.Context,
	sourceLocationName, vodSourceName string,
	body map[string]any,
) error {
	cfgs := extractHTTPPackageConfigurations(body)

	vs, err := h.Backend.UpdateVodSource(sourceLocationName, vodSourceName, cfgs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toVodSourceOutput(vs))
}

func (h *Handler) handleDeleteVodSource(c *echo.Context, sourceLocationName, vodSourceName string) error {
	if err := h.Backend.DeleteVodSource(sourceLocationName, vodSourceName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListVodSources(c *echo.Context, sourceLocationName string) error {
	summaries, nextToken, err := h.Backend.ListVodSources(sourceLocationName, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyVodSourceName:      s.VodSourceName,
			keySourceLocationName: s.SourceLocationName,
			keyArn:                s.ARN,
			keyTags:               nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toVodSourceOutput(vs *VodSource) map[string]any {
	cfgs := make([]map[string]any, 0, len(vs.HTTPPackageConfigurations))
	for _, cfg := range vs.HTTPPackageConfigurations {
		cfgs = append(cfgs, map[string]any{
			"Path":         cfg.Path,
			keySourceGroup: cfg.SourceGroup,
			"Type":         cfg.Type,
		})
	}

	return map[string]any{
		keyVodSourceName:            vs.VodSourceName,
		keySourceLocationName:       vs.SourceLocationName,
		keyArn:                      vs.ARN,
		"HttpPackageConfigurations": cfgs,
		keyTags:                     nilToEmpty(vs.Tags),
	}
}

// --- Tag handlers ---

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyTags: nilToEmpty(tags)})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body map[string]any) error {
	tags := extractTags(body)

	if err := h.Backend.TagResource(resourceARN, tags); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	keys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, keys); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- helpers ---

func extractTags(body map[string]any) map[string]string {
	raw, _ := body[keyTags].(map[string]any)
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

func extractBaseURL(body map[string]any) string {
	httpConfig, _ := body["HttpConfiguration"].(map[string]any)
	if httpConfig == nil {
		return ""
	}

	baseURL, _ := httpConfig["BaseUrl"].(string)

	return baseURL
}

func extractOutputs(body map[string]any) []OutputItem {
	raw, _ := body["Outputs"].([]any)
	if len(raw) == 0 {
		return nil
	}

	outputs := make([]OutputItem, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out := OutputItem{
			ManifestName: stringField(m, "ManifestName"),
			SourceGroup:  stringField(m, keySourceGroup),
		}

		if hls, hlsOk := m["HlsPlaylistSettings"].(map[string]any); hlsOk {
			sec, _ := hls["ManifestWindowSeconds"].(float64)
			out.HlsPlaylistSettings = &HlsPlaylistSettings{
				ManifestWindowSeconds: int(sec),
			}
		}

		outputs = append(outputs, out)
	}

	return outputs
}

func extractHTTPPackageConfigurations(body map[string]any) []HTTPPackageConfiguration {
	raw, _ := body["HttpPackageConfigurations"].([]any)
	if len(raw) == 0 {
		return nil
	}

	cfgs := make([]HTTPPackageConfiguration, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		cfgs = append(cfgs, HTTPPackageConfiguration{
			Path:        stringField(m, "Path"),
			SourceGroup: stringField(m, keySourceGroup),
			Type:        stringField(m, "Type"),
		})
	}

	return cfgs
}

func stringField(m map[string]any, key string) string {
	v, _ := m[key].(string)

	return v
}

func nilToEmpty(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	return m
}

// --- LiveSource handlers ---

func (h *Handler) handleCreateLiveSource(
	c *echo.Context,
	sourceLocationName, liveSourceName string,
	body map[string]any,
) error {
	cfgs := extractHTTPPackageConfigurations(body)
	tags := extractTags(body)

	ls, err := h.Backend.CreateLiveSource(sourceLocationName, liveSourceName, cfgs, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toLiveSourceOutput(ls))
}

func (h *Handler) handleDescribeLiveSource(c *echo.Context, sourceLocationName, liveSourceName string) error {
	ls, err := h.Backend.DescribeLiveSource(sourceLocationName, liveSourceName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toLiveSourceOutput(ls))
}

func (h *Handler) handleUpdateLiveSource(
	c *echo.Context,
	sourceLocationName, liveSourceName string,
	body map[string]any,
) error {
	cfgs := extractHTTPPackageConfigurations(body)

	ls, err := h.Backend.UpdateLiveSource(sourceLocationName, liveSourceName, cfgs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toLiveSourceOutput(ls))
}

func (h *Handler) handleDeleteLiveSource(c *echo.Context, sourceLocationName, liveSourceName string) error {
	if err := h.Backend.DeleteLiveSource(sourceLocationName, liveSourceName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListLiveSources(c *echo.Context, sourceLocationName string) error {
	summaries, nextToken, err := h.Backend.ListLiveSources(sourceLocationName, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyLiveSourceName:     s.LiveSourceName,
			keySourceLocationName: s.SourceLocationName,
			keyArn:                s.ARN,
			keyTags:               nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toLiveSourceOutput(ls *LiveSource) map[string]any {
	cfgs := make([]map[string]any, 0, len(ls.HTTPPackageConfigurations))
	for _, cfg := range ls.HTTPPackageConfigurations {
		cfgs = append(cfgs, map[string]any{
			"Path":         cfg.Path,
			keySourceGroup: cfg.SourceGroup,
			"Type":         cfg.Type,
		})
	}

	return map[string]any{
		keyLiveSourceName:           ls.LiveSourceName,
		keySourceLocationName:       ls.SourceLocationName,
		keyArn:                      ls.ARN,
		"HttpPackageConfigurations": cfgs,
		keyTags:                     nilToEmpty(ls.Tags),
	}
}

// --- PrefetchSchedule handlers ---

func (h *Handler) handleCreatePrefetchSchedule(c *echo.Context, playbackConfigName, name string) error {
	ps, err := h.Backend.CreatePrefetchSchedule(playbackConfigName, name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toPrefetchScheduleOutput(ps))
}

func (h *Handler) handleGetPrefetchSchedule(c *echo.Context, playbackConfigName, name string) error {
	ps, err := h.Backend.GetPrefetchSchedule(playbackConfigName, name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toPrefetchScheduleOutput(ps))
}

func (h *Handler) handleDeletePrefetchSchedule(c *echo.Context, playbackConfigName, name string) error {
	if err := h.Backend.DeletePrefetchSchedule(playbackConfigName, name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListPrefetchSchedules(c *echo.Context, playbackConfigName string) error {
	schedules, nextToken, err := h.Backend.ListPrefetchSchedules(playbackConfigName, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(schedules))
	for _, ps := range schedules {
		out = append(out, toPrefetchScheduleOutput(ps))
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toPrefetchScheduleOutput(ps *PrefetchSchedule) map[string]any {
	return map[string]any{
		keyArn:                      ps.ARN,
		keyName:                     ps.Name,
		"PlaybackConfigurationName": ps.PlaybackConfigurationName,
	}
}

// --- Program handlers ---

func (h *Handler) handleCreateProgram(
	c *echo.Context,
	channelName, programName string,
	body map[string]any,
) error {
	sourceLocationName, _ := body["SourceLocationName"].(string)
	vodSourceName, _ := body[keyVodSourceName].(string)
	liveSourceName, _ := body[keyLiveSourceName].(string)
	tags := extractTags(body)

	prog, err := h.Backend.CreateProgram(
		channelName,
		programName,
		sourceLocationName,
		vodSourceName,
		liveSourceName,
		tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toProgramOutput(prog))
}

func (h *Handler) handleDescribeProgram(c *echo.Context, channelName, programName string) error {
	prog, err := h.Backend.DescribeProgram(channelName, programName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toProgramOutput(prog))
}

func (h *Handler) handleUpdateProgram(c *echo.Context, channelName, programName string) error {
	prog, err := h.Backend.UpdateProgram(channelName, programName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toProgramOutput(prog))
}

func (h *Handler) handleDeleteProgram(c *echo.Context, channelName, programName string) error {
	if err := h.Backend.DeleteProgram(channelName, programName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetChannelSchedule(c *echo.Context, channelName string) error {
	entries, nextToken, err := h.Backend.GetChannelSchedule(channelName, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]any{
			keyArn:         e.ARN,
			keyChannelName: e.ChannelName,
			"ProgramName":  e.ProgramName,
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toProgramOutput(prog *Program) map[string]any {
	return map[string]any{
		keyArn:               prog.ARN,
		keyChannelName:       prog.ChannelName,
		"ProgramName":        prog.ProgramName,
		"SourceLocationName": prog.SourceLocationName,
		keyVodSourceName:     prog.VodSourceName,
		keyLiveSourceName:    prog.LiveSourceName,
		keyTags:              nilToEmpty(prog.Tags),
	}
}

// --- ChannelPolicy handlers ---

func (h *Handler) handlePutChannelPolicy(c *echo.Context, channelName string, body map[string]any) error {
	policy, _ := body["Policy"].(string)

	if err := h.Backend.PutChannelPolicy(channelName, policy); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetChannelPolicy(c *echo.Context, channelName string) error {
	policy, err := h.Backend.GetChannelPolicy(channelName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Policy": policy})
}

func (h *Handler) handleDeleteChannelPolicy(c *echo.Context, channelName string) error {
	if err := h.Backend.DeleteChannelPolicy(channelName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- Function handlers ---

func (h *Handler) handlePutFunction(c *echo.Context, functionID string, body map[string]any) error {
	functionType, _ := body["FunctionType"].(string)
	description, _ := body["Description"].(string)
	tags := extractTags(body)

	fn, err := h.Backend.PutFunction(functionID, functionType, description, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toFunctionOutput(fn))
}

func (h *Handler) handleGetFunction(c *echo.Context, functionID string) error {
	fn, err := h.Backend.GetFunction(functionID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toFunctionOutput(fn))
}

func (h *Handler) handleDeleteFunction(c *echo.Context, functionID string) error {
	if err := h.Backend.DeleteFunction(functionID); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListFunctions(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListFunctions(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			"FunctionId":   s.FunctionID,
			"FunctionType": s.FunctionType,
			keyArn:         s.ARN,
			keyTags:        nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toFunctionOutput(fn *Function) map[string]any {
	return map[string]any{
		"FunctionId":   fn.FunctionID,
		"FunctionType": fn.FunctionType,
		keyArn:         fn.ARN,
		"Description":  fn.Description,
		keyTags:        nilToEmpty(fn.Tags),
	}
}

// --- Alerts handler ---

func (h *Handler) handleListAlerts(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{keyItems: []map[string]any{}})
}

// --- Logs handlers ---

func (h *Handler) handleConfigureLogsForChannel(c *echo.Context, body map[string]any) error {
	channelName, _ := body[keyChannelName].(string)
	logTypes := extractStringSlice(body, "LogTypes")

	name, types, err := h.Backend.ConfigureLogsForChannel(channelName, logTypes)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyChannelName: name,
		"LogTypes":     types,
	})
}

func (h *Handler) handleConfigureLogsForPlaybackConfiguration(c *echo.Context, body map[string]any) error {
	playbackConfigName, _ := body["PlaybackConfigurationName"].(string)
	pct, _ := body["PercentEnabled"].(float64)
	percentEnabled := int(pct)

	name, percent, err := h.Backend.ConfigureLogsForPlaybackConfiguration(playbackConfigName, percentEnabled)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"PlaybackConfigurationName": name,
		"PercentEnabled":            percent,
	})
}

func extractStringSlice(body map[string]any, key string) []string {
	raw, _ := body[key].([]any)
	if len(raw) == 0 {
		return nil
	}

	result := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}

	return result
}
