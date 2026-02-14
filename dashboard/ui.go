package dashboard

import (
	"embed"
	"html/template"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// OperationsProvider defines an interface for retrieving supported operations.
type OperationsProvider interface {
	GetSupportedOperations() []string
}

//go:embed static/*
var staticFS embed.FS

//go:embed templates/*
var templateFS embed.FS

// Handler handles HTTP requests for the Dashboard.
type Handler struct {
	DynamoDB *dynamodb.Client
	S3       *s3.Client
	DDBOps   OperationsProvider
	S3Ops    OperationsProvider
	Logger   *slog.Logger
	layout   *template.Template
}

// NewHandler creates a new Dashboard handler.
func NewHandler(db *dynamodb.Client, s3Client *s3.Client, ddbOps, s3Ops OperationsProvider) *Handler {
	// Parse layout and components
	tmpl := template.Must(template.ParseFS(templateFS,
		"templates/layout.html",
		"templates/components/*.html",
	))

	return &Handler{
		DynamoDB: db,
		S3:       s3Client,
		DDBOps:   ddbOps,
		S3Ops:    s3Ops,
		Logger:   slog.Default(),
		layout:   tmpl,
	}
}

// ServeHTTP implements http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/dashboard")

	// Serve static files
	if strings.HasPrefix(path, "/static/") {
		http.FileServer(http.FS(staticFS)).ServeHTTP(w, r)
		return
	}

	// Route to appropriate handler
	switch {
	case path == "" || path == "/":
		http.Redirect(w, r, "/dashboard/dynamodb", http.StatusFound)
	case strings.HasPrefix(path, "/dynamodb"):
		h.handleDynamoDB(w, r, strings.TrimPrefix(path, "/dynamodb"))
	case strings.HasPrefix(path, "/s3"):
		h.handleS3(w, r, strings.TrimPrefix(path, "/s3"))
	case strings.HasPrefix(path, "/docs"):
		h.docIndex(w, r)
	default:
		http.NotFound(w, r)
	}
}

// renderTemplate renders a page template by cloning the layout and parsing the specific page.
func (h *Handler) renderTemplate(w http.ResponseWriter, pageFile string, data any) {
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
	if err := tmpl.ExecuteTemplate(w, "layout.html", data); err != nil {
		h.Logger.Error("Failed to execute template", "page", pageFile, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// renderFragment renders a shared component/fragment.
// renderFragment renders a shared component/fragment.
func (h *Handler) renderFragment(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Must clone even for fragments to avoid marking h.layout as executed
	tmpl, err := h.layout.Clone()
	if err != nil {
		h.Logger.Error("Failed to clone layout for fragment", "fragment", name, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		h.Logger.Error("Failed to render fragment", "fragment", name, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// handleDynamoDB routes DynamoDB UI requests.
func (h *Handler) handleDynamoDB(w http.ResponseWriter, r *http.Request, path string) {
	switch {
	case path == "" || path == "/":
		h.dynamoDBIndex(w, r)
	case path == "/tables":
		h.dynamoDBTableList(w, r)
	case path == "/create":
		h.dynamoDBCreateTable(w, r)
	case path == "/search":
		h.dynamoDBSearch(w, r)
	case strings.HasPrefix(path, "/table/"):
		tablePath := strings.TrimPrefix(path, "/table/")
		parts := strings.SplitN(tablePath, "/", 2)
		tableName := parts[0]

		if len(parts) == 1 {
			if r.Method == http.MethodDelete {
				h.dynamoDBDeleteTable(w, r, tableName)
			} else {
				h.dynamoDBTableDetail(w, r, tableName)
			}
		} else {
			action := parts[1]
			switch action {
			case "query":
				h.dynamoDBQuery(w, r, tableName)
			case "scan":
				h.dynamoDBScan(w, r, tableName)
			default:
				http.NotFound(w, r)
			}
		}
	default:
		http.NotFound(w, r)
	}
}

// handleS3 routes S3 UI requests.
func (h *Handler) handleS3(w http.ResponseWriter, r *http.Request, path string) {
	switch {
	case path == "" || path == "/":
		h.s3Index(w, r)
	case path == "/buckets":
		h.s3BucketList(w, r)
	case path == "/create":
		h.s3CreateBucket(w, r)
	case strings.HasPrefix(path, "/bucket/"):
		bucketPath := strings.TrimPrefix(path, "/bucket/")
		parts := strings.SplitN(bucketPath, "/", 2)
		bucketName := parts[0]

		if len(parts) == 1 {
			if r.Method == http.MethodDelete {
				h.s3DeleteBucket(w, r, bucketName)
			} else {
				h.s3BucketDetail(w, r, bucketName)
			}
		} else {
			action := parts[1]
			switch {
			case action == "tree":
				h.s3FileTree(w, r, bucketName)
			case action == "upload":
				h.s3Upload(w, r, bucketName)
			case action == "versioning":
				h.s3Versioning(w, r, bucketName)
			case strings.HasPrefix(action, "file/"):
				key := strings.TrimPrefix(action, "file/")
				if r.Method == http.MethodDelete {
					h.s3DeleteFile(w, r, bucketName, key)
				} else {
					h.s3FileDetail(w, r, bucketName, key)
				}
			case strings.HasPrefix(action, "download/"):
				key := strings.TrimPrefix(action, "download/")
				h.s3Download(w, r, bucketName, key)
			default:
				http.NotFound(w, r)
			}
		}
	default:
		http.NotFound(w, r)
	}
}
