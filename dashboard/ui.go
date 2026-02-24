package dashboard

import (
	"embed"
	"html/template"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/labstack/echo/v5"

	apigwbackend "github.com/blackbirdworks/gopherstack/apigateway"
	cwbackend "github.com/blackbirdworks/gopherstack/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	ebbackend "github.com/blackbirdworks/gopherstack/eventbridge"
	iambackend "github.com/blackbirdworks/gopherstack/iam"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/lambda"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	pkgslogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/stepfunctions"
	stsbackend "github.com/blackbirdworks/gopherstack/sts"
)

const (
	pathPartsCount = 2
)

// OperationsProvider defines an interface for retrieving supported operations.
type OperationsProvider interface {
	GetSupportedOperations() []string
}

//go:embed static/*
var staticFS embed.FS

//go:embed templates/*
var templateFS embed.FS

// PageData represents common page data.
type PageData struct {
	Title     string
	ActiveTab string
}

// DashboardHandler handles HTTP requests for the Dashboard web interface.
// It automatically discovers and integrates services that implement DashboardProvider.
// During transition, it also supports the old pattern of direct SDK client injection.
//
//nolint:revive // Stuttering preferred here for clarity per Plan.md
type DashboardHandler struct {
	SNSOps            *snsbackend.Handler
	KMSOps            *kmsbackend.Handler
	SSM               *ssmsdk.Client
	DDBOps            *ddbbackend.DynamoDBHandler
	S3Ops             *s3backend.S3Handler
	SSMOps            *ssmbackend.Handler
	IAMOps            *iambackend.Handler
	STSOps            *stsbackend.Handler
	S3                *s3.Client
	DynamoDB          *dynamodb.Client
	SQSOps            *sqsbackend.Handler
	SecretsManagerOps *secretsmanagerbackend.Handler
	LambdaOps         *lambdabackend.Handler
	EventBridgeOps    *ebbackend.Handler
	APIGatewayOps     *apigwbackend.Handler
	CloudWatchLogsOps *cwlogsbackend.Handler
	StepFunctionsOps  *sfnbackend.Handler
	CloudWatchOps     *cwbackend.Handler
	SubRouter         *echo.Echo
	ddbProvider       *ddbbackend.DashboardProvider
	s3Provider        *s3backend.DashboardProvider
	Logger            *slog.Logger
	layout            *template.Template
	GlobalConfig      config.GlobalConfig
}

// Config holds all dependencies for the Dashboard handler.
type Config struct {
	DDBClient *dynamodb.Client
	S3Client  *s3.Client
	SSMClient *ssmsdk.Client
	DDBOps    *ddbbackend.DynamoDBHandler
	S3Ops     *s3backend.S3Handler
	SSMOps    *ssmbackend.Handler
	IAMOps    *iambackend.Handler
	STSOps    *stsbackend.Handler
	SNSOps    *snsbackend.Handler
	SQSOps    *sqsbackend.Handler
	// KMSOps provides access to the KMS backend.
	KMSOps *kmsbackend.Handler
	// SecretsManagerOps provides access to the Secrets Manager backend.
	SecretsManagerOps *secretsmanagerbackend.Handler
	// LambdaOps provides access to the Lambda backend.
	LambdaOps *lambdabackend.Handler
	// EventBridgeOps provides access to the EventBridge backend.
	EventBridgeOps *ebbackend.Handler
	// APIGatewayOps provides access to the API Gateway backend.
	APIGatewayOps *apigwbackend.Handler
	// CloudWatchLogsOps provides access to the CloudWatch Logs backend.
	CloudWatchLogsOps *cwlogsbackend.Handler
	// StepFunctionsOps provides access to the Step Functions backend.
	StepFunctionsOps *sfnbackend.Handler
	// CloudWatchOps provides access to the CloudWatch Metrics backend.
	CloudWatchOps *cwbackend.Handler
	// Logger is the structured logger for dashboard operations.
	Logger *slog.Logger
	// GlobalConfig holds the centralized account and region configuration shown on the settings page.
	GlobalConfig config.GlobalConfig
}

// NewHandler creates a new Dashboard handler.
func NewHandler(cfg Config) *DashboardHandler {
	funcMap := template.FuncMap{
		"safeID": func(s string) string {
			s = strings.ReplaceAll(s, "/", "-")
			s = strings.ReplaceAll(s, " ", "-")
			s = strings.ReplaceAll(s, ".", "-")
			s = strings.ReplaceAll(s, ":", "-")
			s = strings.ReplaceAll(s, "%", "-")

			return s
		},
	}

	// Parse layout and components
	tmpl := template.Must(template.New("layout").Funcs(funcMap).ParseFS(templateFS,
		"templates/layout.html",
		"templates/components/*.html",
		"templates/s3/*.html",
		"templates/dynamodb/*.html",
		"templates/ssm/*.html",
		"templates/iam/*.html",
		"templates/sts/*.html",
		"templates/sns/*.html",
		"templates/sqs/*.html",
		"templates/kms/*.html",
		"templates/secretsmanager/*.html",
		"templates/lambda/*.html",
		"templates/eventbridge/*.html",
		"templates/apigateway/*.html",
		"templates/cloudwatchlogs/*.html",
		"templates/stepfunctions/*.html",
		"templates/cloudwatch/*.html",
		"templates/metrics.html",
		"templates/doc.html",
		"templates/settings.html",
	))

	// Create service-specific dashboard providers
	ddbProvider := ddbbackend.NewDashboardProvider()
	s3Provider := s3backend.NewDashboardProvider()

	h := &DashboardHandler{
		DynamoDB:          cfg.DDBClient,
		S3:                cfg.S3Client,
		SSM:               cfg.SSMClient,
		DDBOps:            cfg.DDBOps,
		S3Ops:             cfg.S3Ops,
		SSMOps:            cfg.SSMOps,
		IAMOps:            cfg.IAMOps,
		STSOps:            cfg.STSOps,
		SNSOps:            cfg.SNSOps,
		SQSOps:            cfg.SQSOps,
		KMSOps:            cfg.KMSOps,
		SecretsManagerOps: cfg.SecretsManagerOps,
		LambdaOps:         cfg.LambdaOps,
		EventBridgeOps:    cfg.EventBridgeOps,
		APIGatewayOps:     cfg.APIGatewayOps,
		CloudWatchLogsOps: cfg.CloudWatchLogsOps,
		StepFunctionsOps:  cfg.StepFunctionsOps,
		CloudWatchOps:     cfg.CloudWatchOps,
		GlobalConfig:      cfg.GlobalConfig,
		Logger:            cfg.Logger,
		layout:            tmpl,
		ddbProvider:       ddbProvider,
		s3Provider:        s3Provider,
		SubRouter:         echo.New(),
	}

	h.SubRouter.Pre(pkgslogger.EchoMiddleware(cfg.Logger))

	// Set up handler functions for providers
	h.ddbProvider.Handlers.HandleDynamoDB = h.handleDynamoDB
	h.s3Provider.Handlers.HandleS3 = h.handleS3

	h.setupSubRouter()

	return h
}

func (h *DashboardHandler) setupStaticAndRootRoutes() {
	h.SubRouter.GET("/dashboard/static/*", func(c *echo.Context) error {
		http.StripPrefix("/dashboard", http.FileServer(http.FS(staticFS))).
			ServeHTTP(c.Response(), c.Request())

		return nil
	})
	h.SubRouter.GET("/dashboard", func(c *echo.Context) error {
		return c.Redirect(http.StatusFound, "/dashboard/dynamodb")
	})
	h.SubRouter.GET("/dashboard/", func(c *echo.Context) error {
		return c.Redirect(http.StatusFound, "/dashboard/dynamodb")
	})
}

func (h *DashboardHandler) setupProviderRoutes() {
	if h.ddbProvider != nil {
		ddbGroup := h.SubRouter.Group("/dashboard/dynamodb")
		h.ddbProvider.RegisterDashboardRoutes(ddbGroup, nil, "")
	}
	if h.s3Provider != nil {
		s3Group := h.SubRouter.Group("/dashboard/s3")
		h.s3Provider.RegisterDashboardRoutes(s3Group, nil, "")
	}
}

func (h *DashboardHandler) setupSSMRoutes() {
	h.SubRouter.GET("/dashboard/ssm", h.ssmIndex)
	h.SubRouter.GET("/dashboard/ssm/history", h.ssmParameterHistory)
	h.SubRouter.GET("/dashboard/ssm/modal/put", h.ssmPutModal)
	h.SubRouter.POST("/dashboard/ssm/put", h.ssmPutParameter)
	h.SubRouter.DELETE("/dashboard/ssm/delete", h.ssmDeleteParameter)
}

func (h *DashboardHandler) setupIAMRoutes() {
	h.SubRouter.GET("/dashboard/iam", h.iamIndex)
	h.SubRouter.POST("/dashboard/iam/user", h.iamCreateUser)
	h.SubRouter.DELETE("/dashboard/iam/user", h.iamDeleteUser)
	h.SubRouter.POST("/dashboard/iam/role", h.iamCreateRole)
	h.SubRouter.DELETE("/dashboard/iam/role", h.iamDeleteRole)
	h.SubRouter.POST("/dashboard/iam/policy", h.iamCreatePolicy)
	h.SubRouter.DELETE("/dashboard/iam/policy", h.iamDeletePolicy)
	h.SubRouter.POST("/dashboard/iam/group", h.iamCreateGroup)
	h.SubRouter.DELETE("/dashboard/iam/group", h.iamDeleteGroup)
	h.SubRouter.GET("/dashboard/sts", h.stsIndex)
}

func (h *DashboardHandler) setupSNSRoutes() {
	h.SubRouter.GET("/dashboard/sns", h.snsIndex)
	h.SubRouter.POST("/dashboard/sns/create", h.snsCreateTopic)
	h.SubRouter.DELETE("/dashboard/sns/delete", h.snsDeleteTopic)
	h.SubRouter.GET("/dashboard/sns/topic", h.snsTopicDetail)
	h.SubRouter.POST("/dashboard/sns/topic/subscribe", h.snsSubscribeToTopic)
	h.SubRouter.DELETE("/dashboard/sns/topic/subscribe", h.snsUnsubscribeFromTopic)
	h.SubRouter.POST("/dashboard/sns/topic/publish", h.snsPublishMessage)
}

func (h *DashboardHandler) setupSQSRoutes() {
	h.SubRouter.GET("/dashboard/sqs", h.sqsIndex)
	h.SubRouter.GET("/dashboard/sqs/create", h.sqsCreateQueueModal)
	h.SubRouter.POST("/dashboard/sqs/create", h.sqsCreateQueue)
	h.SubRouter.DELETE("/dashboard/sqs/delete", h.sqsDeleteQueue)
	h.SubRouter.POST("/dashboard/sqs/purge", h.sqsPurgeQueue)
	h.SubRouter.GET("/dashboard/sqs/queue", h.sqsQueueDetail)
	h.SubRouter.POST("/dashboard/sqs/message", h.sqsSendMessage)
	h.SubRouter.GET("/dashboard/sqs/messages", h.sqsReceiveMessages)
}

func (h *DashboardHandler) setupKMSRoutes() {
	h.SubRouter.GET("/dashboard/kms", h.kmsIndex)
	h.SubRouter.POST("/dashboard/kms/create", h.kmsCreateKey)
	h.SubRouter.GET("/dashboard/kms/key", h.kmsKeyDetail)
	h.SubRouter.POST("/dashboard/kms/encrypt", h.kmsEncrypt)
	h.SubRouter.POST("/dashboard/kms/decrypt", h.kmsDecrypt)
}

func (h *DashboardHandler) setupSecretsManagerRoutes() {
	h.SubRouter.GET("/dashboard/secretsmanager", h.secretsManagerIndex)
	h.SubRouter.POST("/dashboard/secretsmanager/create", h.secretsManagerCreate)
	h.SubRouter.POST("/dashboard/secretsmanager/update", h.secretsManagerUpdate)
	h.SubRouter.DELETE("/dashboard/secretsmanager/delete", h.secretsManagerDelete)
	h.SubRouter.GET("/dashboard/secretsmanager/secret", h.secretsManagerDetail)
}

func (h *DashboardHandler) setupLambdaRoutes() {
	h.SubRouter.GET("/dashboard/lambda", h.lambdaIndex)
	h.SubRouter.GET("/dashboard/lambda/function", h.lambdaFunctionDetail)
	h.SubRouter.POST("/dashboard/lambda/invoke", h.lambdaInvoke)
}

func (h *DashboardHandler) setupEventBridgeRoutes() {
	h.SubRouter.GET("/dashboard/eventbridge", h.eventBridgeIndex)
	h.SubRouter.GET("/dashboard/eventbridge/rules", h.eventBridgeRules)
	h.SubRouter.GET("/dashboard/eventbridge/events", h.eventBridgeEventLog)
}

func (h *DashboardHandler) setupAPIGatewayRoutes() {
	h.SubRouter.GET("/dashboard/apigateway", h.apiGatewayIndex)
	h.SubRouter.GET("/dashboard/apigateway/api", h.apiGatewayDetail)
}

func (h *DashboardHandler) setupCloudWatchLogsRoutes() {
	h.SubRouter.GET("/dashboard/cloudwatchlogs", h.cloudWatchLogsIndex)
	h.SubRouter.GET("/dashboard/cloudwatchlogs/group", h.cloudWatchLogsGroupDetail)
}

func (h *DashboardHandler) setupStepFunctionsRoutes() {
	h.SubRouter.GET("/dashboard/stepfunctions", h.stepFunctionsIndex)
	h.SubRouter.GET("/dashboard/stepfunctions/statemachine", h.stepFunctionsStateMachineDetail)
}

func (h *DashboardHandler) setupCloudWatchRoutes() {
	h.SubRouter.GET("/dashboard/cloudwatch", h.cloudWatchIndex)
}

func (h *DashboardHandler) setupMetaRoutes() {
	dashboardGroup := h.SubRouter.Group("/dashboard")
	RegisterMetricsHandlers(dashboardGroup, h)
	h.SubRouter.GET("/dashboard/settings", h.settingsIndex)
}

func (h *DashboardHandler) setupSubRouter() {
	h.setupStaticAndRootRoutes()
	h.setupProviderRoutes()
	h.setupSSMRoutes()
	h.setupIAMRoutes()
	h.setupSNSRoutes()
	h.setupSQSRoutes()
	h.setupKMSRoutes()
	h.setupSecretsManagerRoutes()
	h.setupLambdaRoutes()
	h.setupEventBridgeRoutes()
	h.setupAPIGatewayRoutes()
	h.setupCloudWatchLogsRoutes()
	h.setupStepFunctionsRoutes()
	h.setupCloudWatchRoutes()
	h.setupMetaRoutes()
}

// Handler returns the Echo handler function for dashboard requests.
func (h *DashboardHandler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		h.SubRouter.ServeHTTP(c.Response(), c.Request())

		return nil
	}
}

// Name returns the service identifier.
const dashboardName = "Dashboard"

func (h *DashboardHandler) Name() string {
	return dashboardName
}

// RouteMatcher returns a matcher for dashboard requests (by path prefix).
func (h *DashboardHandler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		method := c.Request().Method

		// Dashboard UI uses GET, POST, PUT and DELETE (for purge operations).
		if method != http.MethodGet && method != http.MethodPost && method != http.MethodPut &&
			method != http.MethodDelete {
			return false
		}

		return path == "/dashboard" || strings.HasPrefix(path, "/dashboard/")
	}
}

// MatchPriority returns the priority for the Dashboard matcher.
// Path-based matchers have medium priority (50).
func (h *DashboardHandler) MatchPriority() int {
	const priority = 50

	return priority
}

// dashboardPathPrefixes maps URL path prefixes to operation names.
var dashboardPathPrefixes = []struct { //nolint:gochecknoglobals // lookup table for ExtractOperation
	prefix string
	name   string
}{
	{"/dynamodb", "DynamoDB"},
	{"/s3", "S3"},
	{"/ssm", "SSM"},
	{"/iam", "IAM"},
	{"/sts", "STS"},
	{"/sns", "SNS"},
	{"/sqs", "SQS"},
	{"/kms", "KMS"},
	{"/secretsmanager", "SecretsManager"},
	{"/lambda", "Lambda"},
	{"/eventbridge", "EventBridge"},
	{"/apigateway", "APIGateway"},
	{"/cloudwatchlogs", "CloudWatchLogs"},
	{"/stepfunctions", "StepFunctions"},
	{"/cloudwatch", "CloudWatch"},
	{"/metrics", "Metrics"},
	{"/docs", "Docs"},
}

// ExtractOperation returns the dashboard operation based on path.
func (h *DashboardHandler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	path, _ = strings.CutPrefix(path, "/dashboard")

	for _, p := range dashboardPathPrefixes {
		if strings.HasPrefix(path, p.prefix) {
			return p.name
		}
	}

	return "Dashboard"
}

// ExtractResource returns empty string for dashboard (not resource-specific).
func (h *DashboardHandler) ExtractResource(_ *echo.Context) string {
	return ""
}

// GetSupportedOperations returns an empty list (dashboard is not a primary service).
func (h *DashboardHandler) GetSupportedOperations() []string {
	return []string{}
}

// renderTemplate renders a page template by cloning the layout and parsing the specific page.
func (h *DashboardHandler) renderTemplate(w http.ResponseWriter, pageFile string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Clone the layout (which includes components)
	tmpl, err := h.layout.Clone()
	if err != nil {
		h.Logger.Error("Failed to clone layout template", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	// Parse the specific page template
	// pageFile should be relative to FS root, e.g., "templates/dynamodb/dynamodb_index.html"
	_, err = tmpl.ParseFS(templateFS, "templates/"+pageFile)
	if err != nil {
		h.Logger.Error("Failed to parse page template", "page", pageFile, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	// Execute layout.html which should include the content block
	if err = tmpl.ExecuteTemplate(w, "layout.html", data); err != nil {
		h.Logger.Error("Failed to execute template", "page", pageFile, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// renderFragment renders a shared component/fragment.
func (h *DashboardHandler) renderFragment(w http.ResponseWriter, name string, data any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	}

	// Must clone even for fragments to avoid marking h.layout as executed
	tmpl, err := h.layout.Clone()
	if err != nil {
		h.Logger.Error("Failed to clone layout for fragment", "fragment", name, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	if err = tmpl.ExecuteTemplate(w, name, data); err != nil {
		h.Logger.Error("Failed to render fragment", "fragment", name, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// renderTableDetailFragment renders a fragment from the DynamoDB table detail template.
func (h *DashboardHandler) renderTableDetailFragment(w http.ResponseWriter, fragmentName string, data any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	}

	tmpl, err := h.layout.Clone()
	if err != nil {
		h.Logger.Error("Failed to clone layout for page fragment", "fragment", fragmentName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	_, err = tmpl.ParseFS(templateFS, "templates/dynamodb/table_detail.html")
	if err != nil {
		h.Logger.Error(
			"Failed to parse page template for fragment",
			"page",
			"dynamodb/table_detail.html",
			"fragment",
			fragmentName,
			"error",
			err,
		)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	if err = tmpl.ExecuteTemplate(w, fragmentName, data); err != nil {
		h.Logger.Error("Failed to render page fragment",
			"page", "dynamodb/table_detail.html", "fragment", fragmentName, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// handleDynamoDB routes DynamoDB UI requests.
func (h *DashboardHandler) handleDynamoDB(w http.ResponseWriter, r *http.Request, path string) {
	switch {
	case path == "" || path == "/":
		h.dynamoDBIndex(w, r)
	case path == "/tables":
		h.dynamoDBTableList(w, r)
	case path == "/create":
		h.dynamoDBCreateTable(w, r)
	case path == "/search":
		h.dynamoDBSearch(w, r)
	case path == "/purge":
		h.dynamoDBPurge(w, r)
	case strings.HasPrefix(path, "/table/"):
		tablePath := strings.TrimPrefix(path, "/table/")
		parts := strings.SplitN(tablePath, "/", pathPartsCount)
		tableName := parts[0]

		if len(parts) == 1 {
			h.handleDynamoDBTableRoot(w, r, tableName)

			return
		}

		h.handleDynamoDBTableAction(w, r, tableName, parts[1])
	default:
		http.NotFound(w, r)
	}
}

func (h *DashboardHandler) handleDynamoDBTableRoot(
	w http.ResponseWriter,
	r *http.Request,
	tableName string,
) {
	if r.Method == http.MethodDelete {
		h.dynamoDBDeleteTable(w, r, tableName)

		return
	}
	h.dynamoDBTableDetail(w, r, tableName)
}

func (h *DashboardHandler) handleDynamoDBTableAction(
	w http.ResponseWriter,
	r *http.Request,
	tableName, action string,
) {
	switch action {
	case "query":
		h.dynamoDBQuery(w, r, tableName)
	case "scan":
		h.dynamoDBScan(w, r, tableName)
	case "partiql":
		h.dynamoDBPartiQL(w, r, tableName)
	case "item":
		h.handleDynamoDBItem(w, r, tableName)
	case "export":
		h.dynamoDBExportTable(w, r, tableName)
	case "import":
		h.dynamoDBImportTable(w, r, tableName)
	case "ttl":
		h.dynamoDBUpdateTTL(w, r, tableName)
	case "streams":
		h.dynamoDBUpdateStreams(w, r, tableName)
	case "stream-events":
		h.dynamoDBStreamEvents(w, r, tableName)
	default:
		http.NotFound(w, r)
	}
}

func (h *DashboardHandler) handleDynamoDBItem(w http.ResponseWriter, r *http.Request, tableName string) {
	switch r.Method {
	case http.MethodDelete:
		h.dynamoDBDeleteItem(w, r, tableName)
	case http.MethodPost:
		h.dynamoDBCreateItem(w, r, tableName)
	case http.MethodGet:
		h.dynamoDBItemDetail(w, r, tableName)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleS3 routes S3 UI requests.
func (h *DashboardHandler) handleS3(w http.ResponseWriter, r *http.Request, path string) {
	switch {
	case path == "" || path == "/":
		h.s3Index(w, r)
	case path == "/buckets":
		h.s3BucketList(w, r)
	case path == "/create":
		h.s3CreateBucket(w, r)
	case path == "/purge":
		h.s3Purge(w, r)
	case strings.HasPrefix(path, "/bucket/"):
		h.handleS3Bucket(w, r, strings.TrimPrefix(path, "/bucket/"))
	default:
		http.NotFound(w, r)
	}
}

// handleS3Bucket routes specific bucket operations.
func (h *DashboardHandler) handleS3Bucket(w http.ResponseWriter, r *http.Request, bucketPath string) {
	parts := strings.SplitN(bucketPath, "/", pathPartsCount)
	bucketName := parts[0]

	if len(parts) == 1 {
		if r.Method == http.MethodDelete {
			h.s3DeleteBucket(w, r, bucketName)
		} else {
			h.s3BucketDetail(w, r, bucketName)
		}

		return
	}

	action := parts[1]
	switch {
	case action == "tree":
		h.s3FileTree(w, r, bucketName)
	case action == "upload":
		h.s3Upload(w, r, bucketName)
	case action == "versioning":
		h.s3Versioning(w, r, bucketName)
	case strings.HasPrefix(action, "file/"):
		h.handleS3File(w, r, bucketName, strings.TrimPrefix(action, "file/"))
	case strings.HasPrefix(action, "download/"):
		h.handleS3File(w, r, bucketName, action)
	default:
		http.NotFound(w, r)
	}
}

// handleS3File handles file-specific operations.
func (h *DashboardHandler) handleS3File(w http.ResponseWriter, r *http.Request, bucketName, action string) {
	if key, cut := strings.CutPrefix(action, "download/"); cut {
		h.s3Download(w, r, bucketName, key)

		return
	}

	key := action
	// Check for specific sub-actions on files
	switch {
	case strings.HasSuffix(key, "/preview"):
		h.s3Preview(w, r, bucketName, strings.TrimSuffix(key, "/preview"))
	case strings.HasSuffix(key, "/metadata"):
		h.s3UpdateMetadata(w, r, bucketName, strings.TrimSuffix(key, "/metadata"))
	case strings.HasSuffix(key, "/export"):
		h.s3ExportJSON(w, r, bucketName, strings.TrimSuffix(key, "/export"))
	case strings.HasSuffix(key, "/tag"):
		if r.Method == http.MethodDelete {
			h.s3DeleteTag(w, r, bucketName, strings.TrimSuffix(key, "/tag"))
		} else {
			h.s3UpdateTag(w, r, bucketName, strings.TrimSuffix(key, "/tag"))
		}
	default:
		if r.Method == http.MethodDelete {
			h.s3DeleteFile(w, r, bucketName, key)
		} else {
			h.s3FileDetail(w, r, bucketName, key)
		}
	}
}

// SettingsPageData holds the data rendered by the settings page template.
type SettingsPageData struct {
	PageData

	AccountID string
	Region    string
}

// settingsIndex renders the read-only settings/config page.
func (h *DashboardHandler) settingsIndex(c *echo.Context) error {
	data := SettingsPageData{
		PageData:  PageData{Title: "Settings", ActiveTab: "settings"},
		AccountID: h.GlobalConfig.AccountID,
		Region:    h.GlobalConfig.Region,
	}

	h.renderTemplate(c.Response(), "settings.html", data)

	return nil
}
