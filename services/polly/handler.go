package polly

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	pollyPathPrefix = "/v1/"
	pollyPriority   = service.PriorityPathVersioned
)

const (
	opDeleteLexicon              = "DeleteLexicon"
	opDescribeVoices             = "DescribeVoices"
	opGetLexicon                 = "GetLexicon"
	opGetSpeechSynthesisTask     = "GetSpeechSynthesisTask"
	opListLexicons               = "ListLexicons"
	opListSpeechSynthesisTasks   = "ListSpeechSynthesisTasks"
	opListTagsForResource        = "ListTagsForResource"
	opPutLexicon                 = "PutLexicon"
	opStartSpeechSynthesisStream = "StartSpeechSynthesisStream"
	opStartSpeechSynthesisTask   = "StartSpeechSynthesisTask"
	opSynthesizeSpeech           = "SynthesizeSpeech"
	opTagResource                = "TagResource"
	opUntagResource              = "UntagResource"
	opUnknown                    = "Unknown"
	queryEngine                  = "Engine"
	queryLanguageCode            = "LanguageCode"
	queryGender                  = "Gender"
	queryIncludeAdditional       = "IncludeAdditionalLanguageCodes"
	queryStatus                  = "Status"
	queryNextToken               = "NextToken"
	headerRequestCharacters      = "x-amzn-RequestCharacters"
	headerStreamEngine           = "X-Amzn-Engine"
	headerStreamLanguageCode     = "X-Amzn-Languagecode"
	headerStreamLexiconNames     = "X-Amzn-Lexiconnames"
	headerStreamOutputFormat     = "X-Amzn-Outputformat"
	headerStreamSampleRate       = "X-Amzn-Samplerate"
	headerStreamVoiceID          = "X-Amzn-Voiceid"
	eventTypeHeader              = ":event-type"
	messageTypeHeader            = ":message-type"
	contentTypeHeader            = ":content-type"
	eventMessageType             = "event"
)

// Handler implements Amazon Polly REST JSON operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates handler for backend.
func NewHandler(backend *InMemoryBackend) *Handler { return &Handler{Backend: backend} }

// Reset clears backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns service name.
func (h *Handler) Name() string { return "Polly" }

// GetSupportedOperations returns implemented SDK operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opDeleteLexicon,
		opDescribeVoices,
		opGetLexicon,
		opGetSpeechSynthesisTask,
		opListLexicons,
		opListSpeechSynthesisTasks,
		opListTagsForResource,
		opPutLexicon,
		opStartSpeechSynthesisStream,
		opStartSpeechSynthesisTask,
		opSynthesizeSpeech,
		opTagResource,
		opUntagResource,
	}
}

// ChaosServiceName returns service key.
func (h *Handler) ChaosServiceName() string { return "polly" }

// ChaosOperations returns injectable operation list.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns backend region.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher matches versioned Polly routes.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, pollyPathPrefix)
	}
}

// MatchPriority returns REST service routing priority.
func (h *Handler) MatchPriority() int { return pollyPriority }

// ExtractOperation derives operation from request method and path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.Path).operation
}

// ExtractResource derives task ID, lexicon name, or ARN from request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.Path).resource
}

// Handler returns request dispatcher.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		route := parseRoute(c.Request().Method, c.Request().URL.Path)
		if route.operation == opUnknown {
			return writeError(c, http.StatusNotFound, opUnknown, "unknown Polly route")
		}

		err := h.dispatch(c, route)
		if err == nil {
			return nil
		}

		return h.writeBackendError(c, err)
	}
}

type route struct {
	operation string
	resource  string
}

func parseRoute(method, path string) route {
	switch {
	case method == http.MethodPost && path == "/v1/speech":
		return route{operation: opSynthesizeSpeech}
	case method == http.MethodPost && path == "/v1/synthesisStream":
		return route{operation: opStartSpeechSynthesisStream}
	case method == http.MethodPost && path == "/v1/synthesisTasks":
		return route{operation: opStartSpeechSynthesisTask}
	case method == http.MethodGet && path == "/v1/synthesisTasks":
		return route{operation: opListSpeechSynthesisTasks}
	case method == http.MethodGet && strings.HasPrefix(path, "/v1/synthesisTasks/"):
		return route{operation: opGetSpeechSynthesisTask, resource: suffix(path, "/v1/synthesisTasks/")}
	case method == http.MethodGet && path == "/v1/voices":
		return route{operation: opDescribeVoices}
	case method == http.MethodGet && path == "/v1/lexicons":
		return route{operation: opListLexicons}
	case method == http.MethodPut && strings.HasPrefix(path, "/v1/lexicons/"):
		return route{operation: opPutLexicon, resource: suffix(path, "/v1/lexicons/")}
	case method == http.MethodGet && strings.HasPrefix(path, "/v1/lexicons/"):
		return route{operation: opGetLexicon, resource: suffix(path, "/v1/lexicons/")}
	case method == http.MethodDelete && strings.HasPrefix(path, "/v1/lexicons/"):
		return route{operation: opDeleteLexicon, resource: suffix(path, "/v1/lexicons/")}
	case method == http.MethodGet && strings.HasPrefix(path, "/v1/tags/"):
		return route{operation: opListTagsForResource, resource: suffix(path, "/v1/tags/")}
	case method == http.MethodPost && strings.HasPrefix(path, "/v1/tags/"):
		return route{operation: opTagResource, resource: suffix(path, "/v1/tags/")}
	case method == http.MethodDelete && strings.HasPrefix(path, "/v1/tags/"):
		return route{operation: opUntagResource, resource: suffix(path, "/v1/tags/")}
	default:
		return route{operation: opUnknown}
	}
}

func suffix(path, prefix string) string {
	value, err := url.PathUnescape(strings.TrimPrefix(path, prefix))
	if err != nil {
		return ""
	}

	return value
}

func (h *Handler) dispatch(c *echo.Context, r route) error {
	switch r.operation {
	case opSynthesizeSpeech:
		return h.synthesizeSpeech(c)
	case opStartSpeechSynthesisStream:
		return h.startSpeechSynthesisStream(c)
	case opStartSpeechSynthesisTask:
		return h.startTask(c)
	case opGetSpeechSynthesisTask:
		return h.getTask(c, r.resource)
	case opListSpeechSynthesisTasks:
		return h.listTasks(c)
	case opPutLexicon:
		return h.putLexicon(c, r.resource)
	case opGetLexicon:
		return h.getLexicon(c, r.resource)
	case opDeleteLexicon:
		return h.deleteLexicon(c, r.resource)
	case opListLexicons:
		return h.listLexicons(c)
	case opDescribeVoices:
		return h.describeVoices(c)
	case opTagResource:
		return h.tagResource(c, r.resource)
	case opUntagResource:
		return h.untagResource(c, r.resource)
	case opListTagsForResource:
		return h.listTags(c, r.resource)
	default:
		return fmt.Errorf("%w: unknown operation", ErrValidation)
	}
}

type synthesisInput struct {
	Engine          string   `json:"Engine"`
	LanguageCode    string   `json:"LanguageCode"`
	LexiconNames    []string `json:"LexiconNames"`
	OutputFormat    string   `json:"OutputFormat"`
	SampleRate      string   `json:"SampleRate"`
	SpeechMarkTypes []string `json:"SpeechMarkTypes"`
	Text            string   `json:"Text"`
	TextType        string   `json:"TextType"`
	VoiceID         string   `json:"VoiceId"`
}

func (in synthesisInput) options() SynthesisOptions {
	return SynthesisOptions{
		Engine:          in.Engine,
		LanguageCode:    in.LanguageCode,
		LexiconNames:    in.LexiconNames,
		OutputFormat:    in.OutputFormat,
		SampleRate:      in.SampleRate,
		SpeechMarkTypes: in.SpeechMarkTypes,
		Text:            in.Text,
		TextType:        in.TextType,
		VoiceID:         in.VoiceID,
	}
}

func (h *Handler) synthesizeSpeech(c *echo.Context) error {
	var in synthesisInput
	if err := decodeRequest(c, &in); err != nil {
		return err
	}

	result, err := h.Backend.SynthesizeSpeech(in.options())
	if err != nil {
		return err
	}

	c.Response().Header().Set("Content-Type", result.ContentType)
	c.Response().Header().Set(headerRequestCharacters, strconv.Itoa(result.RequestCharacters))

	return c.Blob(http.StatusOK, result.ContentType, result.Data)
}

type streamTextEvent struct {
	Text     string `json:"Text"`
	TextType string `json:"TextType"`
}

func (h *Handler) startSpeechSynthesisStream(c *echo.Context) error {
	options := SynthesisOptions{
		Engine:       c.Request().Header.Get(headerStreamEngine),
		LanguageCode: c.Request().Header.Get(headerStreamLanguageCode),
		LexiconNames: splitHeader(c.Request().Header.Values(headerStreamLexiconNames)),
		OutputFormat: c.Request().Header.Get(headerStreamOutputFormat),
		SampleRate:   c.Request().Header.Get(headerStreamSampleRate),
		VoiceID:      c.Request().Header.Get(headerStreamVoiceID),
	}

	text, textType, err := decodeStreamText(c.Request().Body)
	if err != nil {
		return fmt.Errorf("%w: invalid synthesis stream: %v", ErrValidation, err)
	}
	options.Text = text
	options.TextType = textType

	result, err := h.Backend.SynthesizeSpeech(options)
	if err != nil {
		return err
	}

	var stream bytes.Buffer
	encoder := eventstream.NewEncoder()
	if err := encodeEvent(encoder, &stream, "AudioEvent", result.Data); err != nil {
		return err
	}

	closed, err := json.Marshal(map[string]int{"RequestCharacters": result.RequestCharacters})
	if err != nil {
		return err
	}
	if err := encodeEvent(encoder, &stream, "StreamClosedEvent", closed); err != nil {
		return err
	}

	return c.Blob(http.StatusOK, "application/vnd.amazon.eventstream", stream.Bytes())
}

func splitHeader(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		for _, name := range strings.Split(value, ",") {
			if strings.TrimSpace(name) != "" {
				out = append(out, strings.Trim(strings.TrimSpace(name), `"`))
			}
		}
	}

	return out
}

func decodeStreamText(body io.Reader) (string, string, error) {
	decoder := eventstream.NewDecoder()
	textType := "text"
	var texts []string

	for {
		message, err := decoder.Decode(body, nil)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", "", err
		}

		eventType := message.Headers.Get(eventTypeHeader)
		if eventType == nil || eventType.String() != "TextEvent" {
			continue
		}

		var event streamTextEvent
		if err := json.Unmarshal(message.Payload, &event); err != nil {
			return "", "", err
		}
		texts = append(texts, event.Text)
		if event.TextType != "" {
			textType = event.TextType
		}
	}

	return strings.Join(texts, ""), textType, nil
}

func encodeEvent(encoder *eventstream.Encoder, out io.Writer, eventType string, payload []byte) error {
	message := eventstream.Message{Payload: payload}
	message.Headers.Set(eventTypeHeader, eventstream.StringValue(eventType))
	message.Headers.Set(messageTypeHeader, eventstream.StringValue(eventMessageType))
	if eventType == "StreamClosedEvent" {
		message.Headers.Set(contentTypeHeader, eventstream.StringValue("application/json"))
	}

	return encoder.Encode(out, message)
}

type startTaskInput struct {
	synthesisInput
	OutputS3BucketName string `json:"OutputS3BucketName"`
	SNSRoleArn         string `json:"SnsRoleArn"`
	SNSTopicArn        string `json:"SnsTopicArn"`
}

type taskOutput struct {
	CreationTime      float64  `json:"CreationTime"`
	Engine            string   `json:"Engine"`
	LanguageCode      string   `json:"LanguageCode,omitempty"`
	LexiconNames      []string `json:"LexiconNames,omitempty"`
	OutputFormat      string   `json:"OutputFormat"`
	OutputURI         string   `json:"OutputUri"`
	RequestCharacters int      `json:"RequestCharacters"`
	SampleRate        string   `json:"SampleRate"`
	SNSRoleArn        string   `json:"SnsRoleArn,omitempty"`
	SNSTopicArn       string   `json:"SnsTopicArn,omitempty"`
	SpeechMarkTypes   []string `json:"SpeechMarkTypes,omitempty"`
	TaskID            string   `json:"TaskId"`
	TaskStatus        string   `json:"TaskStatus"`
	TaskStatusReason  string   `json:"TaskStatusReason,omitempty"`
	TextType          string   `json:"TextType"`
	VoiceID           string   `json:"VoiceId"`
}

func buildTaskOutput(task *SpeechSynthesisTask) taskOutput {
	return taskOutput{
		CreationTime:      float64(task.CreationTime.UnixMilli()) / 1000,
		Engine:            task.Options.Engine,
		LanguageCode:      task.Options.LanguageCode,
		LexiconNames:      task.Options.LexiconNames,
		OutputFormat:      task.Options.OutputFormat,
		OutputURI:         task.OutputURI,
		RequestCharacters: len(task.Options.Text),
		SampleRate:        task.Options.SampleRate,
		SNSRoleArn:        task.SNSRoleArn,
		SNSTopicArn:       task.SNSTopicArn,
		SpeechMarkTypes:   task.Options.SpeechMarkTypes,
		TaskID:            task.TaskID,
		TaskStatus:        task.TaskStatus,
		TaskStatusReason:  task.TaskStatusReason,
		TextType:          task.Options.TextType,
		VoiceID:           task.Options.VoiceID,
	}
}

func (h *Handler) startTask(c *echo.Context) error {
	var in startTaskInput
	if err := decodeRequest(c, &in); err != nil {
		return err
	}

	task, err := h.Backend.StartSpeechSynthesisTask(
		in.options(), in.OutputS3BucketName, in.SNSRoleArn, in.SNSTopicArn,
	)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]any{"SynthesisTask": buildTaskOutput(task)})
}

func (h *Handler) getTask(c *echo.Context, id string) error {
	task, err := h.Backend.GetSpeechSynthesisTask(id)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]any{"SynthesisTask": buildTaskOutput(task)})
}

func (h *Handler) listTasks(c *echo.Context) error {
	tasks, token, err := h.Backend.ListSpeechSynthesisTasks(
		c.QueryParam(queryStatus),
		c.QueryParam(queryNextToken),
	)
	if err != nil {
		return err
	}

	out := make([]taskOutput, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, buildTaskOutput(task))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SynthesisTasks": out,
		"NextToken":      token,
	})
}

type putLexiconInput struct {
	Content string `json:"Content"`
}

func (h *Handler) putLexicon(c *echo.Context, name string) error {
	var in putLexiconInput
	if err := decodeRequest(c, &in); err != nil {
		return err
	}
	if err := h.Backend.PutLexicon(name, in.Content); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) getLexicon(c *echo.Context, name string) error {
	lexicon, err := h.Backend.GetLexicon(name)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Lexicon": map[string]string{
			"Content": lexicon.Content,
			"Name":    lexicon.Name,
		},
		"LexiconAttributes": lexiconAttributes(lexicon),
	})
}

func (h *Handler) deleteLexicon(c *echo.Context, name string) error {
	if err := h.Backend.DeleteLexicon(name); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) listLexicons(c *echo.Context) error {
	lexicons := h.Backend.ListLexicons()
	attributes := make([]map[string]any, 0, len(lexicons))
	for _, lexicon := range lexicons {
		attributes = append(attributes, lexiconAttributes(lexicon))
	}

	return c.JSON(http.StatusOK, map[string]any{"Lexicons": attributes})
}

func lexiconAttributes(lexicon *Lexicon) map[string]any {
	return map[string]any{
		"Alphabet":     lexicon.Alphabet,
		"LanguageCode": lexicon.LanguageCode,
		"LastModified": float64(lexicon.LastModified.UnixMilli()) / 1000,
		"LexemesCount": lexicon.LexemesCount,
		"LexiconArn":   lexicon.ARN,
		"Size":         lexicon.Size,
		"Name":         lexicon.Name,
	}
}

func (h *Handler) describeVoices(c *echo.Context) error {
	include, err := strconv.ParseBool(defaultQueryValue(c.QueryParam(queryIncludeAdditional), "false"))
	if err != nil {
		return fmt.Errorf("%w: invalid IncludeAdditionalLanguageCodes", ErrValidation)
	}

	voices, err := h.Backend.DescribeVoices(DescribeVoicesFilter{
		Engine:                         c.QueryParam(queryEngine),
		LanguageCode:                   c.QueryParam(queryLanguageCode),
		Gender:                         c.QueryParam(queryGender),
		IncludeAdditionalLanguageCodes: include,
	})
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]any{"Voices": voices})
}

type tagResourceInput struct {
	Tags []Tag `json:"Tags"`
}

func (h *Handler) tagResource(c *echo.Context, resource string) error {
	var in tagResourceInput
	if err := decodeRequest(c, &in); err != nil {
		return err
	}
	if err := h.Backend.TagResource(resource, in.Tags); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) untagResource(c *echo.Context, resource string) error {
	keys := c.QueryParams()["tagKeys"]
	if len(keys) == 0 {
		keys = c.QueryParams()["TagKeys"]
	}
	if err := h.Backend.UntagResource(resource, keys); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) listTags(c *echo.Context, resource string) error {
	tags, err := h.Backend.ListTagsForResource(resource)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]any{"Tags": tags})
}

func decodeRequest(c *echo.Context, value any) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return fmt.Errorf("%w: failed to read body", ErrValidation)
	}
	if err := json.Unmarshal(body, value); err != nil {
		return fmt.Errorf("%w: invalid JSON: %v", ErrValidation, err)
	}

	return nil
}

func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return writeError(c, http.StatusNotFound, "LexiconNotFoundException", err.Error())
	case errors.Is(err, ErrValidation):
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	default:
		return writeError(c, http.StatusInternalServerError, "ServiceFailureException", err.Error())
	}
}

func writeError(c *echo.Context, code int, typ, message string) error {
	return c.JSON(code, map[string]string{
		"__type":  typ,
		"message": message,
	})
}

func defaultQueryValue(got, fallback string) string {
	if got == "" {
		return fallback
	}

	return got
}
