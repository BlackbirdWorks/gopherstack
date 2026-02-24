package lambda

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// lambdaMatchPriority is the routing priority for the Lambda handler.
const lambdaMatchPriority = 95

// lambdaPathPrefix is the path prefix for Lambda REST API v1 endpoints.
const lambdaPathPrefix = "/2015-03-31/functions"

// routeSpec binds an HTTP method and path predicate to an operation name or handler.
type routeSpec struct {
	method string
	match  func(rest string) bool
	op     string
}

// lambdaOpRoutes maps HTTP method + path predicates to operation names.
//
//nolint:gochecknoglobals // intentional package-level route table
var lambdaOpRoutes = []routeSpec{
	{http.MethodPost, isEmptyRest, "CreateFunction"},
	{http.MethodGet, isEmptyRest, "ListFunctions"},
	{http.MethodGet, isNameOnly, "GetFunction"},
	{http.MethodDelete, isNameOnly, "DeleteFunction"},
	{http.MethodPut, hasSuffixCode, "UpdateFunctionCode"},
	{http.MethodPut, hasSuffixConfiguration, "UpdateFunctionConfiguration"},
	{http.MethodPost, hasSuffixInvocations, "InvokeFunction"},
}

func isEmptyRest(rest string) bool            { return rest == "" }
func hasSuffixCode(rest string) bool          { return strings.HasSuffix(rest, "/code") }
func hasSuffixConfiguration(rest string) bool { return strings.HasSuffix(rest, "/configuration") }
func hasSuffixInvocations(rest string) bool   { return strings.HasSuffix(rest, "/invocations") }

// Handler is the Echo HTTP handler for Lambda operations.
type Handler struct {
	Backend       StorageBackend
	Logger        *slog.Logger
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new Lambda handler with the given backend and logger.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{
		Backend: backend,
		Logger:  log,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Lambda" }

// GetSupportedOperations returns the list of supported Lambda operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateFunction",
		"GetFunction",
		"ListFunctions",
		"DeleteFunction",
		"UpdateFunctionCode",
		"UpdateFunctionConfiguration",
		"InvokeFunction",
	}
}

// RouteMatcher returns a function that identifies Lambda requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(path, lambdaPathPrefix) ||
			strings.HasPrefix(target, "AWSLambda")
	}
}

// MatchPriority returns the routing priority for the Lambda handler.
func (h *Handler) MatchPriority() int { return lambdaMatchPriority }

// ExtractOperation returns the Lambda operation name derived from the request method and path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	rest := strings.TrimPrefix(c.Request().URL.Path, lambdaPathPrefix)
	method := c.Request().Method

	for _, route := range lambdaOpRoutes {
		if route.method == method && route.match(rest) {
			return route.op
		}
	}

	return "Unknown"
}

// ExtractResource returns the function name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	rest := strings.TrimPrefix(c.Request().URL.Path, lambdaPathPrefix+"/")
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split into at most name + rest

	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}

	return ""
}

// handlerEntry binds a route to a handler function.
type handlerEntry struct {
	execute func(c *echo.Context, rest string) error
	match   func(rest string) bool
	method  string
}

func (h *Handler) buildRouteHandlers() []handlerEntry {
	return []handlerEntry{
		{
			method:  http.MethodPost,
			match:   isEmptyRest,
			execute: func(c *echo.Context, _ string) error { return h.handleCreateFunction(c) },
		},
		{
			method:  http.MethodGet,
			match:   isEmptyRest,
			execute: func(c *echo.Context, _ string) error { return h.handleListFunctions(c) },
		},
		{
			method:  http.MethodGet,
			match:   isNameOnly,
			execute: func(c *echo.Context, rest string) error { return h.handleGetFunction(c, nameFromRest(rest)) },
		},
		{
			method:  http.MethodDelete,
			match:   isNameOnly,
			execute: func(c *echo.Context, rest string) error { return h.handleDeleteFunction(c, nameFromRest(rest)) },
		},
		{
			method: http.MethodPut,
			match:  hasSuffixCode,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/code")

				return h.handleUpdateFunctionCode(c, name)
			},
		},
		{
			method: http.MethodPut,
			match:  hasSuffixConfiguration,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/configuration")

				return h.handleUpdateFunctionConfiguration(c, name)
			},
		},
		{
			method: http.MethodPost,
			match:  hasSuffixInvocations,
			execute: func(c *echo.Context, rest string) error {
				name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/invocations")

				return h.handleInvoke(c, name)
			},
		},
	}
}

// Handler returns the Echo handler function for Lambda operations.
func (h *Handler) Handler() echo.HandlerFunc {
	routes := h.buildRouteHandlers()

	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)
		rest := strings.TrimPrefix(c.Request().URL.Path, lambdaPathPrefix)
		method := c.Request().Method

		for _, route := range routes {
			if route.method == method && route.match(rest) {
				return route.execute(c, rest)
			}
		}

		log.DebugContext(ctx, "lambda: unknown route", "method", method, "path", c.Request().URL.Path)

		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func (h *Handler) handleCreateFunction(c *echo.Context) error {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	var input CreateFunctionInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	if input.FunctionName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "FunctionName is required")
	}

	if input.PackageType == "" {
		input.PackageType = PackageTypeImage
	}

	if input.PackageType != PackageTypeImage && input.PackageType != PackageTypeZip {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"PackageType must be Image or Zip")
	}

	if input.Code == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Code is required")
	}

	if input.PackageType == PackageTypeImage && input.Code.ImageURI == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Code.ImageUri is required for Image package type")
	}

	if input.PackageType == PackageTypeZip {
		if input.Runtime == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"Runtime is required for Zip package type")
		}

		if input.Code.ZipFile == nil && (input.Code.S3Bucket == "" || input.Code.S3Key == "") {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"Code.ZipFile or Code.S3Bucket+Code.S3Key is required for Zip package type")
		}
	}

	memorySize := input.MemorySize
	if memorySize <= 0 {
		memorySize = defaultMemorySize
	}

	timeout := input.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	now := time.Now().UTC()
	fn := &FunctionConfiguration{
		FunctionName: input.FunctionName,
		FunctionArn:  buildARN(h.DefaultRegion, h.AccountID, input.FunctionName),
		Description:  input.Description,
		ImageURI:     input.Code.ImageURI,
		PackageType:  input.PackageType,
		Runtime:      input.Runtime,
		Handler:      input.Handler,
		Role:         input.Role,
		MemorySize:   memorySize,
		Timeout:      timeout,
		Environment:  input.Environment,
		State:        FunctionStateActive,
		CreatedAt:    now,
		LastModified: now.Format(time.RFC3339),
		RevisionID:   uuid.New().String(),
		ZipData:      input.Code.ZipFile,
		S3BucketCode: input.Code.S3Bucket,
		S3KeyCode:    input.Code.S3Key,
	}

	if len(fn.ZipData) > 0 {
		fn.CodeSize = int64(len(fn.ZipData))
	}

	if createErr := h.Backend.CreateFunction(fn); createErr != nil {
		if errors.Is(createErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException", createErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	return c.JSON(http.StatusCreated, fn)
}

func (h *Handler) handleGetFunction(c *echo.Context, name string) error {
	fn, err := h.Backend.GetFunction(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &GetFunctionOutput{
		Configuration: fn,
		Code:          buildCodeLocation(fn),
	})
}

func (h *Handler) handleListFunctions(c *echo.Context) error {
	fns := h.Backend.ListFunctions()

	return c.JSON(http.StatusOK, &ListFunctionsOutput{
		Functions: fns,
	})
}

func (h *Handler) handleDeleteFunction(c *echo.Context, name string) error {
	if err := h.Backend.DeleteFunction(name); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateFunctionCode(c *echo.Context, name string) error {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	var input UpdateFunctionCodeInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	fn, getFnErr := h.Backend.GetFunction(name)
	if getFnErr != nil {
		if errors.Is(getFnErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", getFnErr.Error())
	}

	if fn.PackageType == PackageTypeImage || fn.PackageType == "" {
		if input.ImageURI == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"ImageUri is required for Image package type")
		}

		fn.ImageURI = input.ImageURI
	} else {
		// Zip package type: update zip data or S3 reference
		if input.ZipFile == nil && (input.S3Bucket == "" || input.S3Key == "") {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"ZipFile or S3Bucket+S3Key is required for Zip package type")
		}

		fn.ZipData = input.ZipFile
		fn.S3BucketCode = input.S3Bucket
		fn.S3KeyCode = input.S3Key

		if len(fn.ZipData) > 0 {
			fn.CodeSize = int64(len(fn.ZipData))
		}
	}

	fn.LastModified = time.Now().UTC().Format(time.RFC3339)
	fn.RevisionID = uuid.New().String()

	if updateErr := h.Backend.UpdateFunction(fn); updateErr != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, fn)
}

func (h *Handler) handleUpdateFunctionConfiguration(c *echo.Context, name string) error {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	var input UpdateFunctionConfigurationInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid request body")
	}

	fn, getFnErr := h.Backend.GetFunction(name)
	if getFnErr != nil {
		if errors.Is(getFnErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", getFnErr.Error())
	}

	if input.Description != "" {
		fn.Description = input.Description
	}

	if input.MemorySize > 0 {
		fn.MemorySize = input.MemorySize
	}

	if input.Timeout > 0 {
		fn.Timeout = input.Timeout
	}

	if input.Environment != nil {
		fn.Environment = input.Environment
	}

	if input.Role != "" {
		fn.Role = input.Role
	}

	if input.Runtime != "" {
		fn.Runtime = input.Runtime
	}

	if input.Handler != "" {
		fn.Handler = input.Handler
	}

	fn.LastModified = time.Now().UTC().Format(time.RFC3339)
	fn.RevisionID = uuid.New().String()

	if updateErr := h.Backend.UpdateFunction(fn); updateErr != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, fn)
}

func (h *Handler) handleInvoke(c *echo.Context, name string) error {
	ctx := c.Request().Context()

	invType := InvocationType(c.Request().Header.Get("X-Amz-Invocation-Type"))
	if invType == "" {
		invType = InvocationTypeRequestResponse
	}

	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	if body == nil {
		body = []byte("{}")
	}

	result, statusCode, invokeErr := h.Backend.InvokeFunction(ctx, name, invType, body)
	if invokeErr != nil {
		if errors.Is(invokeErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function not found: %s", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", invokeErr.Error())
	}

	if statusCode == http.StatusNoContent {
		return c.NoContent(http.StatusNoContent)
	}

	if statusCode == http.StatusAccepted {
		return c.NoContent(http.StatusAccepted)
	}

	if len(result) > 0 {
		return c.JSONBlob(http.StatusOK, result)
	}

	return c.NoContent(http.StatusOK)
}

// writeError writes a Lambda-formatted JSON error response.
func (h *Handler) writeError(c *echo.Context, status int, errType, message string) error {
	return c.JSON(status, &Error{
		Type:    errType,
		Message: message,
	})
}

// isNameOnly returns true when rest is a single path segment (/{name} with no sub-paths).
func isNameOnly(rest string) bool {
	trimmed := strings.TrimPrefix(rest, "/")

	return trimmed != "" && !strings.Contains(trimmed, "/")
}

// nameFromRest strips the leading slash from a single-segment path like /{name}.
func nameFromRest(rest string) string {
	return strings.TrimPrefix(rest, "/")
}

// buildCodeLocation constructs the FunctionCodeLocation response for a function.
func buildCodeLocation(fn *FunctionConfiguration) *FunctionCodeLocation {
	if fn.PackageType == PackageTypeZip {
		loc := &FunctionCodeLocation{RepositoryType: "S3"}
		if fn.S3BucketCode != "" && fn.S3KeyCode != "" {
			loc.Location = fmt.Sprintf("s3://%s/%s", fn.S3BucketCode, fn.S3KeyCode)
		}

		return loc
	}

	return &FunctionCodeLocation{
		ImageURI:       fn.ImageURI,
		RepositoryType: "ECR",
	}
}

// buildARN constructs a Lambda function ARN.
func buildARN(region, accountID, functionName string) string {
	return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", region, accountID, functionName)
}

// defaultMemorySize is the default Lambda function memory in MB.
const defaultMemorySize = 128

// defaultTimeout is the default Lambda function timeout in seconds.
const defaultTimeout = 3
