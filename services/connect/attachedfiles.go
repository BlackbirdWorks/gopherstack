package connect

import (
	"context"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	labelAttachedFile     = "AttachedFile"
	labelAttachedConfig   = "AttachedFilesConfiguration"
	fileStatusProcessing  = "PROCESSING"
	fileStatusApproved    = "APPROVED"
	useCaseAttachment     = "ATTACHMENT"
	useCaseVoiceRecording = "VOICE_RECORDING"
	scopeEmail            = "EMAIL"
	scopeChat             = "CHAT"
	scopeCase             = "CASE"
	scopeTask             = "TASK"
	defaultAttachLimit    = 20 * 1024 * 1024
	maxAttachLimit        = 104857600
	maxFileNameLen        = 256
	maxExtensionLen       = 10
	defaultURLExpiry      = 300
	minURLExpiry          = 5
	maxBatchFiles         = 100
	isoMillis             = "2006-01-02T15:04:05.000Z"
	attachedContentPath   = "/attached-files-content/"
)

var (
	extensionPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	casesARNPattern  = regexp.MustCompile(`^arn:[^:]+:cases:[^:]+:\d{12}:domain/[^/]+/case/[^/]+$`)
)

// attachmentScopes are the places a file can be attached.
func attachmentScopes() []string { return []string{scopeEmail, scopeChat, scopeCase, scopeTask} }

func defaultExtensions() []string {
	return []string{
		"csv",
		"doc",
		"docx",
		"gif",
		"jpeg",
		"jpg",
		"pdf",
		"png",
		"ppt",
		"pptx",
		"tif",
		"tiff",
		"txt",
		"wav",
		"xls",
		"xlsx",
	}
}

// CreatedByInfo identifies who created an attached file: exactly one member is set.
type CreatedByInfo struct {
	ConnectUserArn string `json:"ConnectUserArn,omitempty"`
	AWSIdentityArn string `json:"AWSIdentityArn,omitempty"`
}

type attachedFileRec struct {
	CreatedBy    *CreatedByInfo    `json:"createdBy,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	InstanceID   string            `json:"instanceId"`
	FileID       string            `json:"fileId"`
	Arn          string            `json:"arn"`
	Name         string            `json:"name"`
	Status       string            `json:"status"`
	UseCase      string            `json:"useCase"`
	Resource     string            `json:"resource"`
	CreationTime string            `json:"creationTime"`
	Source       string            `json:"source,omitempty"`
	Content      []byte            `json:"content,omitempty"`
	Size         int64             `json:"size"`
	Uploaded     bool              `json:"uploaded,omitempty"`
}

type attachedConfigRec struct {
	LastModified *awstime.EpochTime `json:"lastModified,omitempty"`
	InstanceID   string             `json:"instanceId"`
	Scope        string             `json:"scope"`
	Extensions   []string           `json:"extensions"`
	MaxSize      int64              `json:"maxSize"`
}

// AllowedExtension is one permitted file extension.
type AllowedExtension struct {
	Extension string `json:"Extension"`
}

// ExtensionConfiguration lists the permitted file extensions.
type ExtensionConfiguration struct {
	AllowedExtensions []AllowedExtension `json:"AllowedExtensions"`
}

// AttachedFilesConfiguration is the attachment policy of one scope.
type AttachedFilesConfiguration struct {
	LastModifiedTime        *awstime.EpochTime      `json:"LastModifiedTime,omitempty"`
	ExtensionConfiguration  *ExtensionConfiguration `json:"ExtensionConfiguration"`
	InstanceID              string                  `json:"InstanceId"`
	AttachmentScope         string                  `json:"AttachmentScope"`
	MaximumSizeLimitInBytes int64                   `json:"MaximumSizeLimitInBytes"`
}

// AttachedFile is the metadata of an attached file.
type AttachedFile struct {
	CreatedBy             *CreatedByInfo    `json:"CreatedBy,omitempty"`
	Tags                  map[string]string `json:"Tags,omitempty"`
	CreationTime          string            `json:"CreationTime"`
	FileArn               string            `json:"FileArn"`
	FileID                string            `json:"FileId"`
	FileName              string            `json:"FileName"`
	FileStatus            string            `json:"FileStatus"`
	FileUseCaseType       string            `json:"FileUseCaseType"`
	AssociatedResourceArn string            `json:"AssociatedResourceArn"`
	FileSizeInBytes       int64             `json:"FileSizeInBytes"`
}

// AttachedFileError reports a file a batch call could not return.
type AttachedFileError struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
	FileID       string `json:"FileId"`
}

// URLMetadata is a pre-signed content URL.
type URLMetadata struct {
	HeadersToInclude map[string]string `json:"HeadersToInclude,omitempty"`
	URL              string            `json:"Url"`
	URLExpiry        string            `json:"UrlExpiry"`
}

type fileRef struct {
	AssociatedResourceArn string `json:"-" query:"associatedResourceArn"`
	FileID                string `json:"-"                               path:"FileId"`
	instPath
}

// CreateAttachedFileInput is the CreateAttachedFile request.
type CreateAttachedFileInput struct {
	Tags                  map[string]string
	ClientToken           string
	FileSourceURI         string `json:"FileSourceUri"`
	FileUseCaseType       string
	AssociatedResourceArn string `json:"-"             query:"associatedResourceArn"`
	instPath
}

// StartAttachedFileUploadInput is the StartAttachedFileUpload request.
type StartAttachedFileUploadInput struct {
	CreatedBy             *CreatedByInfo
	Tags                  map[string]string
	ClientToken           string
	FileName              string
	FileUseCaseType       string
	AssociatedResourceArn string `json:"-"                  query:"associatedResourceArn"`
	instPath
	FileSizeInBytes    int64
	URLExpiryInSeconds int32 `json:"UrlExpiryInSeconds"`
}

// GetAttachedFileInput is the GetAttachedFile request.
type GetAttachedFileInput struct {
	fileRef
	URLExpiryInSeconds int32 `json:"-" query:"urlExpiryInSeconds"`
}

// BatchGetAttachedFileMetadataInput is the BatchGetAttachedFileMetadata request.
type BatchGetAttachedFileMetadataInput struct {
	AssociatedResourceArn string `json:"-"       query:"associatedResourceArn"`
	instPath
	FileIDs []string `json:"FileIds"`
}

// AttachedFileCreated is the StartAttachedFileUpload response.
type AttachedFileCreated struct {
	UploadURLMetadata *URLMetadata   `json:"UploadUrlMetadata,omitempty"`
	CreatedBy         *CreatedByInfo `json:"CreatedBy,omitempty"`
	CreationTime      string         `json:"CreationTime"`
	FileArn           string         `json:"FileArn"`
	FileID            string         `json:"FileId"`
	FileStatus        string         `json:"FileStatus"`
}

// AttachedFileOut is the attached-file metadata response.
type AttachedFileOut struct {
	DownloadURLMetadata *URLMetadata `json:"DownloadUrlMetadata,omitempty"`
	AttachedFile
}

// AttachedBatchOut is the batch attached-file response.
type AttachedBatchOut struct {
	Files  []AttachedFile      `json:"Files"`
	Errors []AttachedFileError `json:"Errors"`
}

// UpdateAttachedFilesConfigurationInput is the UpdateAttachedFilesConfiguration request.
type UpdateAttachedFilesConfigurationInput struct {
	ExtensionConfiguration  *ExtensionConfiguration
	MaximumSizeLimitInBytes *int64
	AttachmentScope         string `json:"-" path:"AttachmentScope"`
	instPath
}

type configRef struct {
	AttachmentScope string `json:"-" path:"AttachmentScope"`
	instPath
}

func isoTime(t time.Time) string { return t.UTC().Format(isoMillis) }

func (r *attachedFileRec) view() AttachedFile {
	return AttachedFile{
		CreatedBy: r.CreatedBy, Tags: cloneTags(r.Tags), CreationTime: r.CreationTime, FileArn: r.Arn,
		FileID: r.FileID, FileName: r.Name, FileStatus: r.Status, FileUseCaseType: r.UseCase,
		AssociatedResourceArn: r.Resource, FileSizeInBytes: r.Size,
	}
}

func checkCreatedBy(c *CreatedByInfo) error {
	if c != nil && (c.ConnectUserArn == "") == (c.AWSIdentityArn == "") {
		return invalidParamErr("CreatedBy needs exactly one of ConnectUserArn and AWSIdentityArn")
	}

	return nil
}

func checkURLExpiry(secs int32) (time.Duration, error) {
	if secs == 0 {
		secs = defaultURLExpiry
	}

	if secs < minURLExpiry || secs > defaultURLExpiry {
		return 0, invalidParamErr("UrlExpiryInSeconds must be %d-%d", minURLExpiry, defaultURLExpiry)
	}

	return time.Duration(secs) * time.Second, nil
}

func checkExtensions(c *ExtensionConfiguration) ([]string, error) {
	if c == nil || len(c.AllowedExtensions) == 0 {
		return nil, invalidParamErr("ExtensionConfiguration.AllowedExtensions is required")
	}

	var out []string

	for _, e := range c.AllowedExtensions {
		if len(e.Extension) < 1 || len(e.Extension) > maxExtensionLen || !extensionPattern.MatchString(e.Extension) {
			return nil, invalidParamErr("extension %q must be 1-%d alphanumeric, hyphen or underscore characters",
				e.Extension, maxExtensionLen)
		}

		if !slices.Contains(out, strings.ToLower(e.Extension)) {
			out = append(out, strings.ToLower(e.Extension))
		}
	}

	return out, nil
}

func (r *attachedConfigRec) view() *AttachedFilesConfiguration {
	cfg := &ExtensionConfiguration{AllowedExtensions: make([]AllowedExtension, 0, len(r.Extensions))}
	for _, e := range r.Extensions {
		cfg.AllowedExtensions = append(cfg.AllowedExtensions, AllowedExtension{Extension: e})
	}

	return &AttachedFilesConfiguration{
		InstanceID: r.InstanceID, AttachmentScope: r.Scope, MaximumSizeLimitInBytes: r.MaxSize,
		ExtensionConfiguration: cfg, LastModifiedTime: r.LastModified,
	}
}

func (b *Backend) attachedConfig(inst, scope string) *attachedConfigRec {
	if r, ok := b.attachedConfigs.table.Get(inst + "/" + scope); ok {
		return r
	}

	return &attachedConfigRec{
		InstanceID: inst,
		Scope:      scope,
		Extensions: defaultExtensions(),
		MaxSize:    defaultAttachLimit,
	}
}

// resolveAttachTarget returns the attachment scope of an ARN a file can be attached to.
func (b *Backend) resolveAttachTarget(inst, resourceARN string) (string, error) {
	if casesARNPattern.MatchString(resourceARN) {
		return scopeCase, nil
	}

	c, err := b.contactByARN(inst, resourceARN)
	if err != nil {
		return "", err
	}

	switch c.Contact.Channel {
	case channelChat:
		return scopeChat, nil
	case channelTask:
		return scopeTask, nil
	case emailValue:
		return scopeEmail, nil
	}

	return "", invalidParamErr("files cannot be attached to %s contacts", c.Contact.Channel)
}

func (b *Backend) contactByARN(inst, resourceARN string) (*contactRec, error) {
	_, id, ok := strings.Cut(resourceARN, ":instance/"+inst+"/contact/")
	if !ok || id == "" {
		return nil, invalidParamErr("AssociatedResourceArn %q is not a contact of this instance", resourceARN)
	}

	return b.contacts.get(inst, id)
}

func (b *Backend) newAttachedFile(
	ctx context.Context,
	inst, name, useCase, resource string,
	tags map[string]string,
) *attachedFileRec {
	id := newID()

	return &attachedFileRec{
		InstanceID: inst, FileID: id, Arn: b.buildARN(ctx, "instance/"+inst+"/attached-file/"+id), Name: name,
		UseCase: useCase, Resource: resource, CreationTime: isoTime(b.now()), Tags: cloneTags(tags),
	}
}

func (b *Backend) checkAttachUpload(inst string, in *StartAttachedFileUploadInput) (time.Duration, error) {
	if err := firstError(
		checkName("FileName", in.FileName, maxFileNameLen), checkCreatedBy(in.CreatedBy), checkTags(in.Tags),
		oneOf("FileUseCaseType", in.FileUseCaseType, useCaseAttachment),
	); err != nil {
		return 0, err
	}

	if in.FileSizeInBytes < 1 {
		return 0, invalidParamErr("FileSizeInBytes must be at least 1")
	}

	expiry, err := checkURLExpiry(in.URLExpiryInSeconds)
	if err != nil {
		return 0, err
	}

	scope, err := b.resolveAttachTarget(inst, in.AssociatedResourceArn)
	if err != nil {
		return 0, err
	}

	cfg := b.attachedConfig(inst, scope)
	ext := strings.ToLower(strings.TrimPrefix(path.Ext(in.FileName), "."))

	if !slices.Contains(cfg.Extensions, ext) {
		return 0, invalidParamErr("files with the extension %q cannot be attached to %s resources", ext, scope)
	}

	if in.FileSizeInBytes > cfg.MaxSize {
		return 0, invalidParamErr("FileSizeInBytes exceeds the %d byte limit of %s attachments", cfg.MaxSize, scope)
	}

	return expiry, nil
}

// StartAttachedFileUpload registers a file and hands out a pre-signed upload URL.
func (b *Backend) StartAttachedFileUpload(
	ctx context.Context,
	in *StartAttachedFileUploadInput,
) (*AttachedFileCreated, error) {
	return writeInstance(b, "StartAttachedFileUpload", in.InstanceID, func(inst string) (*AttachedFileCreated, error) {
		expiry, err := b.checkAttachUpload(inst, in)
		if err != nil {
			return nil, err
		}

		rec := b.newAttachedFile(ctx, inst, in.FileName, in.FileUseCaseType, in.AssociatedResourceArn, in.Tags)
		rec.Size, rec.Status, rec.CreatedBy = in.FileSizeInBytes, fileStatusProcessing, in.CreatedBy
		b.attachedFiles.put(rec)

		return &AttachedFileCreated{
			FileArn: rec.Arn, FileID: rec.FileID, CreationTime: rec.CreationTime, FileStatus: rec.Status,
			CreatedBy:         rec.CreatedBy,
			UploadURLMetadata: b.contentURL(ctx, "put", inst, rec.FileID, expiry),
		}, nil
	})
}

// CreateAttachedFile copies an S3 object into an attached voice recording.
func (b *Backend) CreateAttachedFile(ctx context.Context, in *CreateAttachedFileInput) (*AttachedFileCreated, error) {
	bucket, key, ok := parseS3URI(in.FileSourceURI)
	if !ok {
		return nil, invalidParamErr("FileSourceUri must be an s3:// URI")
	}

	if err := firstError(
		oneOf("FileUseCaseType", in.FileUseCaseType, useCaseVoiceRecording),
		checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateAttachedFile", in.InstanceID, func(inst string) (*AttachedFileCreated, error) {
		c, err := b.contactByARN(inst, in.AssociatedResourceArn)
		if err != nil {
			return nil, err
		}

		if c.Contact.Channel != channelVoice || c.active() {
			return nil, invalidRequestErr("AssociatedResourceArn must be a completed voice contact")
		}

		data, err := b.readSource(ctx, bucket, key)
		if err != nil {
			return nil, err
		}

		rec := b.newAttachedFile(ctx, inst, path.Base(key), in.FileUseCaseType, in.AssociatedResourceArn, in.Tags)
		rec.Size, rec.Status, rec.Source, rec.Content, rec.Uploaded = int64(
			len(data),
		), fileStatusApproved, in.FileSourceURI, data, true
		b.attachedFiles.put(rec)

		return &AttachedFileCreated{
			FileArn: rec.Arn, FileID: rec.FileID, CreationTime: rec.CreationTime, FileStatus: rec.Status,
		}, nil
	})
}

func parseS3URI(uri string) (string, string, bool) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return "", "", false
	}

	bucket, key, found := strings.Cut(rest, "/")

	return bucket, key, found && bucket != "" && key != ""
}

func (b *Backend) readSource(ctx context.Context, bucket, key string) ([]byte, error) {
	reader, ok := b.resolvers.(ObjectReader)
	if !ok {
		return nil, invalidRequestErr("S3 is not available to read FileSourceUri")
	}

	if !b.resolvers.ObjectExists(ctx, bucket, key) {
		return nil, notFoundErr("S3 object", "s3://"+bucket+"/"+key)
	}

	data, err := reader.ReadObject(ctx, bucket, key)
	if err != nil {
		return nil, invalidRequestErr("s3://%s/%s could not be read: %v", bucket, key, err)
	}

	return data, nil
}

func (b *Backend) fileOf(inst, id, resource string) (*attachedFileRec, error) {
	r, err := b.attachedFiles.get(inst, id)
	if err != nil {
		return nil, err
	}

	if r.Resource != resource {
		return nil, notFoundErr(labelAttachedFile, id)
	}

	return r, nil
}

// CompleteAttachedFileUpload approves a file once its content was uploaded.
func (b *Backend) CompleteAttachedFileUpload(in *fileRef) (*EmptyOutput, error) {
	return writeInstance(b, "CompleteAttachedFileUpload", in.InstanceID, func(inst string) (*EmptyOutput, error) {
		r, err := b.fileOf(inst, in.FileID, in.AssociatedResourceArn)
		if err != nil {
			return nil, err
		}

		if r.Status != fileStatusProcessing {
			return nil, invalidRequestErr("file %q is already %s", in.FileID, r.Status)
		}

		if !r.Uploaded || int64(len(r.Content)) != r.Size {
			return nil, invalidRequestErr("the content of file %q was not uploaded with %d bytes", in.FileID, r.Size)
		}

		r.Status = fileStatusApproved

		return &EmptyOutput{}, nil
	})
}

// GetAttachedFile returns the metadata of a file and, once approved, a download URL.
func (b *Backend) GetAttachedFile(ctx context.Context, in *GetAttachedFileInput) (*AttachedFileOut, error) {
	expiry, err := checkURLExpiry(in.URLExpiryInSeconds)
	if err != nil {
		return nil, err
	}

	return readInstance(b, "GetAttachedFile", in.InstanceID, func(inst string) (*AttachedFileOut, error) {
		r, ferr := b.fileOf(inst, in.FileID, in.AssociatedResourceArn)
		if ferr != nil {
			return nil, ferr
		}

		out := &AttachedFileOut{AttachedFile: r.view()}
		if r.Status == fileStatusApproved {
			out.DownloadURLMetadata = b.contentURL(ctx, "get", inst, r.FileID, expiry)
		}

		return out, nil
	})
}

// BatchGetAttachedFileMetadata returns the metadata of up to 100 files; the rest are reported as errors.
func (b *Backend) BatchGetAttachedFileMetadata(in *BatchGetAttachedFileMetadataInput) (*AttachedBatchOut, error) {
	if n := len(in.FileIDs); n < 1 || n > maxBatchFiles {
		return nil, invalidParamErr("FileIds must hold 1-%d items", maxBatchFiles)
	}

	return readInstance(b, "BatchGetAttachedFileMetadata", in.InstanceID, func(inst string) (*AttachedBatchOut, error) {
		out := &AttachedBatchOut{Files: []AttachedFile{}, Errors: []AttachedFileError{}}

		for _, id := range in.FileIDs {
			r, err := b.fileOf(inst, id, in.AssociatedResourceArn)
			if err != nil {
				out.Errors = append(out.Errors, AttachedFileError{
					ErrorCode: "ResourceNotFoundException", ErrorMessage: err.Error(), FileID: id,
				})

				continue
			}

			out.Files = append(out.Files, r.view())
		}

		return out, nil
	})
}

// DeleteAttachedFile removes a file and its content.
func (b *Backend) DeleteAttachedFile(in *fileRef) (*EmptyOutput, error) {
	return writeInstance(b, "DeleteAttachedFile", in.InstanceID, func(inst string) (*EmptyOutput, error) {
		if _, err := b.fileOf(inst, in.FileID, in.AssociatedResourceArn); err != nil {
			return nil, err
		}

		return nil, b.attachedFiles.del(inst, in.FileID)
	})
}

// DescribeAttachedFilesConfiguration returns the attachment policy of a scope, defaults when never updated.
func (b *Backend) DescribeAttachedFilesConfiguration(in *configRef) (*AttachedConfigOut, error) {
	if err := oneOf("AttachmentScope", in.AttachmentScope, attachmentScopes()...); err != nil {
		return nil, err
	}

	return readInstance(
		b,
		"DescribeAttachedFilesConfiguration",
		in.InstanceID,
		func(inst string) (*AttachedConfigOut, error) {
			return &AttachedConfigOut{
				AttachedFilesConfiguration: b.attachedConfig(inst, in.AttachmentScope).view(),
			}, nil
		},
	)
}

// AttachedConfigOut is the attached-file config response.
type AttachedConfigOut struct {
	AttachedFilesConfiguration *AttachedFilesConfiguration `json:"AttachedFilesConfiguration"`
}

// ListAttachedFilesConfigurations lists the attachment policy of every scope.
func (b *Backend) ListAttachedFilesConfigurations(
	inst, token string,
	maxResults int32,
) ([]AttachedFilesConfiguration, string, error) {
	all, err := readInstance(
		b,
		"ListAttachedFilesConfigurations",
		inst,
		func(inst string) ([]AttachedFilesConfiguration, error) {
			out := make([]AttachedFilesConfiguration, 0, len(attachmentScopes()))
			for _, s := range attachmentScopes() {
				out = append(out, *b.attachedConfig(inst, s).view())
			}

			return out, nil
		},
	)
	if err != nil {
		return nil, "", err
	}

	return paged(all, token, maxResults)
}

// UpdateAttachedFilesConfiguration changes the size limit and allowed extensions of a scope.
func (b *Backend) UpdateAttachedFilesConfiguration(
	_ context.Context,
	in *UpdateAttachedFilesConfigurationInput,
) (*AttachedFilesConfiguration, error) {
	if err := oneOf("AttachmentScope", in.AttachmentScope, attachmentScopes()...); err != nil {
		return nil, err
	}

	if in.MaximumSizeLimitInBytes == nil && in.ExtensionConfiguration == nil {
		return nil, invalidParamErr("MaximumSizeLimitInBytes or ExtensionConfiguration is required")
	}

	if m := in.MaximumSizeLimitInBytes; m != nil && (*m < 1 || *m > maxAttachLimit) {
		return nil, invalidParamErr("MaximumSizeLimitInBytes must be 1-%d", maxAttachLimit)
	}

	var exts []string

	if in.ExtensionConfiguration != nil {
		var err error
		if exts, err = checkExtensions(in.ExtensionConfiguration); err != nil {
			return nil, err
		}
	}

	return writeInstance(
		b,
		"UpdateAttachedFilesConfiguration",
		in.InstanceID,
		func(inst string) (*AttachedFilesConfiguration, error) {
			rec := *b.attachedConfig(inst, in.AttachmentScope)
			if exts != nil {
				rec.Extensions = exts
			}

			if in.MaximumSizeLimitInBytes != nil {
				rec.MaxSize = *in.MaximumSizeLimitInBytes
			}

			t := awstime.NewEpochTime(b.now())
			rec.LastModified = &t
			b.attachedConfigs.put(&rec)

			return rec.view(), nil
		},
	)
}
