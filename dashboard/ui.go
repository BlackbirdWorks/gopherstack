package dashboard

import (
	"embed"
	"html/template"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"

	"Gopherstack/pkgs/service"
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
//
//nolint:revive // Stuttering preferred here for clarity per Plan.md
type DashboardHandler struct {
	DynamoDB  *dynamodb.Client
	S3        *s3.Client
	DDBOps    OperationsProvider
	S3Ops     OperationsProvider
	Logger    *slog.Logger
	layout    *template.Template
	subRouter *echo.Echo
}

// NewHandler creates a new Dashboard handler.
func NewHandler(
	db *dynamodb.Client,
	s3Client *s3.Client,
	ddbOps, s3Ops OperationsProvider,
	logger *slog.Logger,
) *DashboardHandler {
	// Parse layout and components
	tmpl := template.Must(template.ParseFS(templateFS,
		"templates/layout.html",
		"templates/components/*.html",
	))

	h := &DashboardHandler{
		DynamoDB: db,
		S3:       s3Client,
		DDBOps:   ddbOps,
		S3Ops:    s3Ops,
		Logger:   logger,
		layout:   tmpl,
	}

	h.setupSubRouter()

	return h
}

func (h *DashboardHandler) setupSubRouter() {
	h.subRouter = echo.New()

	// Static files
	h.subRouter.GET("/dashboard/static/*", func(c *echo.Context) error {
		http.StripPrefix("/dashboard", http.FileServer(http.FS(staticFS))).
			ServeHTTP(c.Response(), c.Request())

		return nil
	})

	// Redirect root to DynamoDB
	h.subRouter.GET("/dashboard", func(c *echo.Context) error {
		return c.Redirect(http.StatusFound, "/dashboard/dynamodb")
	})
	h.subRouter.GET("/dashboard/", func(c *echo.Context) error {
		return c.Redirect(http.StatusFound, "/dashboard/dynamodb")
	})

	// DynamoDB routes
	h.subRouter.Any("/dashboard/dynamodb*", func(c *echo.Context) error {
		path := strings.TrimPrefix(c.Request().URL.Path, "/dashboard/dynamodb")
		h.handleDynamoDB(c.Response(), c.Request(), path)

		return nil
	})

	// S3 routes
	h.subRouter.Any("/dashboard/s3*", func(c *echo.Context) error {
		path := strings.TrimPrefix(c.Request().URL.Path, "/dashboard/s3")
		h.handleS3(c.Response(), c.Request(), path)

		return nil
	})

	// Metrics & Docs
	h.subRouter.GET("/dashboard/metrics", func(c *echo.Context) error {
		h.metricsIndex(c.Response(), c.Request())

		return nil
	})
	h.subRouter.GET("/dashboard/docs", func(c *echo.Context) error {
		h.docIndex(c.Response(), c.Request())

		return nil
	})
}

// Handler returns the Echo handler function for dashboard requests.
func (h *DashboardHandler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		h.subRouter.ServeHTTP(c.Response(), c.Request())

		return nil
	}
}

// Name returns the service identifier.
func (h *DashboardHandler) Name() string {
	return "Dashboard"
}

// RouteMatcher returns a matcher for dashboard requests (by path prefix).
func (h *DashboardHandler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, "/dashboard")
	}
}

// ExtractOperation returns the dashboard operation based on path.
func (h *DashboardHandler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	path, _ = strings.CutPrefix(path, "/dashboard")

	switch {
	case strings.HasPrefix(path, "/dynamodb"):
		return "DynamoDB"
	case strings.HasPrefix(path, "/s3"):
		return "S3"
	case strings.HasPrefix(path, "/metrics"):
		return "Metrics"
	case strings.HasPrefix(path, "/docs"):
		return "Docs"
	default:
		return "Dashboard"
	}
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
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

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
	case "item":
		h.handleDynamoDBItem(w, r, tableName)
	case "export":
		h.dynamoDBExportTable(w, r, tableName)
	case "import":
		h.dynamoDBImportTable(w, r, tableName)
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
