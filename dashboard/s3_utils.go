package dashboard

import (
	"fmt"
)

const (
	maxKeys            = 1000
	maxMultipartMemory = 32 << 20 // 32 MB
	maxFormBodySize    = 1 << 20  // 1 MB
	constStrTrue       = "true"
)

// S3FileVersion represents a version of an S3 object.
type S3FileVersion struct {
	VersionID    string
	LastModified string
}

// BucketInfo represents bucket information for display.
type BucketInfo struct {
	PageData

	Name              string
	CreationDate      string
	Prefix            string
	PathParts         []string
	ObjectCount       int
	VersioningEnabled bool
}

// FileTreeItem represents a file or folder in the tree.
type FileTreeItem struct {
	Name         string
	FullPath     string
	Size         string
	LastModified string
	BucketName   string
	IsFolder     bool
}

// s3FileDetailData holds data for the s3/file_detail.html template.
type s3FileDetailData struct {
	PageData

	BucketName        string
	Key               string
	Size              string
	LastModified      string
	ContentType       string
	ETag              string
	Tags              map[string]string
	Versions          []S3FileVersion
	IsImage           bool
	IsPDF             bool
	IsText            bool
	VersioningEnabled bool
}

// formatBytes formats bytes into human-readable format.
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
