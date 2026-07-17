package rekognition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	rekognitionTargetPrefix = "RekognitionService."
	rekognitionContentType  = "application/x-amz-json-1.1"
	rekognitionServiceName  = "Rekognition"

	keyTypeField    = "__type"
	keyMessageField = "message"

	// orientationRotate0 is the default (unrotated) OrientationCorrection
	// reported by every stateless image-analysis mock response (faces,
	// labels, celebrities).
	orientationRotate0 = "ROTATE_0"
)

// Handler handles Rekognition HTTP requests using X-Amz-Target routing.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return rekognitionServiceName }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// MatchPriority returns routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// RouteMatcher returns a matcher that accepts Rekognition X-Amz-Target headers.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), rekognitionTargetPrefix)
	}
}

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, rekognitionTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for name := range h.ops {
		ops = append(ops, name)
	}

	return ops
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			rekognitionServiceName, rekognitionContentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// buildOps assembles the full operation table from each op-family's
// contribution (see handler_<family>.go).
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	ops := make(map[string]service.JSONOpFunc)

	maps.Copy(ops, h.collectionOps())
	maps.Copy(ops, h.faceOps())
	maps.Copy(ops, h.streamProcessorOps())
	maps.Copy(ops, h.tagOps())
	maps.Copy(ops, h.projectOps())
	maps.Copy(ops, h.projectVersionOps())
	maps.Copy(ops, h.datasetOps())
	maps.Copy(ops, h.userOps())
	maps.Copy(ops, h.faceLivenessOps())
	maps.Copy(ops, h.labelOps())
	maps.Copy(ops, h.textDetectionOps())
	maps.Copy(ops, h.moderationOps())
	maps.Copy(ops, h.celebrityOps())
	maps.Copy(ops, h.customLabelOps())
	maps.Copy(ops, h.mediaAnalysisOps())

	return ops
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: operation %q not implemented", ErrUnknownOperation, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, ErrNameInUse):
		// Stream processors, projects, and project versions report a
		// name conflict as ResourceInUseException, not
		// ResourceAlreadyExistsException -- see ErrNameInUse.
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceInUseException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, ErrUserConflict):
		// CreateUser reports a duplicate UserId as ConflictException, not
		// ResourceAlreadyExistsException -- see ErrUserConflict.
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ConflictException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceAlreadyExistsException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "InvalidParameterException",
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "UnknownOperationException",
			keyMessageField: err.Error(),
		})
	}
}

// epochSeconds renders a timestamp as AWS JSON epoch seconds (with fractional
// nanoseconds), matching what the Rekognition SDK deserializer expects.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

// imageRef is the common AWS "Image" wire shape (either an S3 reference or
// inline bytes), shared by every image-analysis and face operation.
type imageRef struct {
	S3Object *struct {
		Bucket  string `json:"Bucket"`
		Name    string `json:"Name"`
		Version string `json:"Version"`
	} `json:"S3Object"`
	Bytes []byte `json:"Bytes"`
}

// imageRefKey returns a stable string derived from the image reference for similarity hashing.
func imageRefKey(img imageRef) string {
	if img.S3Object != nil {
		return img.S3Object.Bucket + "/" + img.S3Object.Name
	}
	// Use length of inline bytes as a cheap image-dependent discriminator.
	return fmt.Sprintf("bytes:%d", len(img.Bytes))
}

// videoRef is the common AWS "Video" wire shape for async video-job Start* requests.
type videoRef struct {
	S3Object *struct {
		Bucket  string `json:"Bucket"`
		Name    string `json:"Name"`
		Version string `json:"Version"`
	} `json:"S3Object"`
}

type startJobResp struct {
	JobId string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
}

type getJobReq struct { //nolint:govet // existing issue.
	JobId      string `json:"JobId"` //nolint:revive,staticcheck // existing issue.
	MaxResults int32  `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
}

type videoMetadata struct { //nolint:govet // existing issue.
	Codec          string  `json:"Codec"`
	DurationMillis int64   `json:"DurationMillis"`
	Format         string  `json:"Format"`
	FrameRate      float32 `json:"FrameRate"`
}

type getJobBaseResp struct { //nolint:govet // existing issue.
	JobId         string         `json:"JobId"` //nolint:revive,staticcheck // existing issue.
	JobStatus     string         `json:"JobStatus"`
	NextToken     string         `json:"NextToken,omitempty"`
	StatusMessage string         `json:"StatusMessage,omitempty"`
	VideoMetadata *videoMetadata `json:"VideoMetadata"`
}

// getJobBase fetches the common async-video-job fields shared by every
// Get<Family>Detection/Recognition/Search/Tracking/Moderation response.
func (h *Handler) getJobBase(jobID string) (*getJobBaseResp, error) {
	if jobID == "" {
		return nil, fmt.Errorf("%w: JobId is required", ErrValidation)
	}

	job, err := h.Backend.GetAsyncJob(jobID)
	if err != nil {
		return nil, err
	}

	return &getJobBaseResp{
		JobId:     job.JobID,
		JobStatus: job.JobStatus,
		VideoMetadata: &videoMetadata{
			Codec:          "H264",
			DurationMillis: 0,
			Format:         "QuickTime / MOV",
			FrameRate:      30, //nolint:mnd // existing issue.
		},
	}, nil
}
