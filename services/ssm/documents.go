package ssm

import (
	"context"
	"crypto/sha1" //nolint:gosec // Sha1 is a real DocumentDescription field kept for backward compatibility, not for security
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const documentHashTypeSha256 = "Sha256"

// documentHashes computes DocumentDescription's Hash/HashType/Sha1 members
// from a document's content. Real AWS computes Hash as the SHA256 (default
// HashType) of the document content, and separately populates the deprecated
// Sha1 field for backward compatibility (aws-sdk-go-v2/service/ssm@v1.73.4
// types/enums.go:708-724, DocumentHashType Sha256|Sha1).
func documentHashes(content string) (string, string) {
	sum256 := sha256.Sum256([]byte(content))
	sum1 := sha1.Sum([]byte(content)) //nolint:gosec // content-integrity hash, not a security boundary

	return hex.EncodeToString(sum256[:]), hex.EncodeToString(sum1[:])
}

// attachmentsInformation projects CreateDocument/UpdateDocument's Attachments
// source list down to the Name-only shape real AWS returns in
// DocumentDescription.AttachmentsInformation (types.AttachmentInformation).
func attachmentsInformation(sources []AttachmentsSource) []AttachmentInformation {
	if len(sources) == 0 {
		return nil
	}

	out := make([]AttachmentInformation, 0, len(sources))
	for _, s := range sources {
		out = append(out, AttachmentInformation{Name: s.Name})
	}

	return out
}

func (b *InMemoryBackend) documentsStore(region string) *store.Table[Document] {
	return getOrCreateTable(b, b.documents, "documents", region, documentKeyFn)
}
func (b *InMemoryBackend) documentVersionsStore(region string) map[string][]DocumentVersion {
	return b.documentVersions[region]
}
func (b *InMemoryBackend) documentPermissionsStore(region string) map[string][]string {
	return b.documentPermissions[region]
}

// registerDefaultDocuments pre-registers the built-in AWS documents.
func (b *InMemoryBackend) registerDefaultDocuments(region string) {
	now := UnixTimeFloat(time.Now())
	defaults := []struct {
		name     string
		docType  string
		content  string
		platform []string
	}{
		{
			name:    "AWS-RunShellScript",
			docType: DocumentTypeCommand,
			content: `{"schemaVersion":"2.2","description":"Run shell script",` +
				`"parameters":{"commands":{"type":"StringList"}},` +
				`"mainSteps":[{"action":"aws:runShellScript","name":"runShellScript",` +
				`"inputs":{"commands":["{{ commands }}"]}}]}`,
			platform: []string{"Linux"},
		},
		{
			name:    "AWS-RunPowerShellScript",
			docType: DocumentTypeCommand,
			content: `{"schemaVersion":"2.2","description":"Run PowerShell script",` +
				`"parameters":{"commands":{"type":"StringList"}},` +
				`"mainSteps":[{"action":"aws:runPowerShellScript","name":"runPowerShellScript",` +
				`"inputs":{"commands":["{{ commands }}"]}}]}`,
			platform: []string{"Windows"},
		},
	}

	if b.documentVersions[region] == nil {
		b.documentVersions[region] = make(map[string][]DocumentVersion)
	}
	documents := b.documentsStore(region)
	documentVersions := b.documentVersionsStore(region)

	for _, d := range defaults {
		doc := Document{
			Name:            d.name,
			Content:         d.content,
			DocumentType:    d.docType,
			DocumentFormat:  documentFormatJSON,
			Status:          statusActive,
			SchemaVersion:   "2.2",
			PlatformTypes:   d.platform,
			CreatedDate:     now,
			DocumentVersion: "1",
			LatestVersion:   "1",
			DefaultVersion:  "1",
		}
		documents.Put(&doc)
		documentVersions[d.name] = []DocumentVersion{
			{
				Name:             d.name,
				DocumentVersion:  "1",
				CreatedDate:      now,
				IsDefaultVersion: true,
				DocumentFormat:   documentFormatJSON,
				Status:           statusActive,
				Content:          d.content,
			},
		}
	}
}

const defaultListDocMaxResults = 50

// CreateDocument stores a new SSM document.
func (b *InMemoryBackend) CreateDocument(
	ctx context.Context,
	input *CreateDocumentInput,
) (*CreateDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("CreateDocument")
	defer b.mu.Unlock()

	documentsTable := b.documentsStore(region)
	if documentsTable.Has(input.Name) {
		return nil, ErrDocumentAlreadyExists
	}

	format := input.DocumentFormat
	if format == "" {
		format = documentFormatJSON
	}

	docType := input.DocumentType
	if docType == "" {
		docType = DocumentTypeCommand
	}

	now := UnixTimeFloat(time.Now())
	hash, sha1Hex := documentHashes(input.Content)
	doc := Document{
		Name:                   input.Name,
		DisplayName:            input.DisplayName,
		Content:                input.Content,
		DocumentType:           docType,
		DocumentFormat:         format,
		Status:                 statusActive,
		TargetType:             input.TargetType,
		Description:            input.Description,
		PlatformTypes:          input.PlatformTypes,
		SchemaVersion:          "2.2",
		CreatedDate:            now,
		DocumentVersion:        "1",
		LatestVersion:          "1",
		DefaultVersion:         "1",
		Requires:               input.Requires,
		Hash:                   hash,
		HashType:               documentHashTypeSha256,
		Sha1:                   sha1Hex,
		AttachmentsInformation: attachmentsInformation(input.Attachments),
	}

	documentsTable.Put(&doc)
	if b.documentVersions[region] == nil {
		b.documentVersions[region] = make(map[string][]DocumentVersion)
	}
	versionStore := b.documentVersionsStore(region)
	versionStore[input.Name] = []DocumentVersion{
		{
			Name:             input.Name,
			DisplayName:      input.DisplayName,
			DocumentVersion:  "1",
			CreatedDate:      now,
			IsDefaultVersion: true,
			DocumentFormat:   format,
			Status:           statusActive,
			Content:          input.Content,
		},
	}

	if len(input.Tags) > 0 {
		if b.miscResourceTags[region] == nil {
			b.miscResourceTags[region] = make(map[string]map[string]string)
		}
		miscTags := b.miscResourceTagsStore(region)
		if miscTags[input.Name] == nil {
			miscTags[input.Name] = make(map[string]string)
		}
		for _, t := range input.Tags {
			miscTags[input.Name][t.Key] = t.Value
		}
	}

	return &CreateDocumentOutput{
		DocumentDescription: doc.asDocumentDescription(b.miscResourceTagList(region, doc.Name)),
	}, nil
}

// asDocumentDescription converts an internal Document to the wire-accurate DocumentDescription shape
// returned by CreateDocument/UpdateDocument/DescribeDocument. Real AWS never
// includes Content in these metadata responses.
func (d Document) asDocumentDescription(docTags []Tag) DocumentDescription {
	return DocumentDescription{
		TargetType:             d.TargetType,
		LatestVersion:          d.LatestVersion,
		DocumentType:           d.DocumentType,
		DocumentFormat:         d.DocumentFormat,
		Status:                 d.Status,
		StatusInformation:      d.StatusInformation,
		DefaultVersion:         d.DefaultVersion,
		Name:                   d.Name,
		DisplayName:            d.DisplayName,
		SchemaVersion:          d.SchemaVersion,
		Description:            d.Description,
		DocumentVersion:        d.DocumentVersion,
		Hash:                   d.Hash,
		HashType:               d.HashType,
		Sha1:                   d.Sha1,
		PlatformTypes:          d.PlatformTypes,
		AttachmentsInformation: d.AttachmentsInformation,
		Requires:               d.Requires,
		Tags:                   docTags,
		CreatedDate:            d.CreatedDate,
	}
}

// resolveDocumentVersionSelector resolves the "$LATEST"/"$DEFAULT" selectors
// to a concrete version string. An explicit "$DEFAULT" always resolves to
// the document's DefaultVersion (set by UpdateDocumentDefaultVersion), which
// can genuinely differ from LatestVersion — this emulator previously
// conflated the two, always serving the latest content even when $DEFAULT
// was explicitly requested. An omitted DocumentVersion is treated the same
// as this emulator has always treated it (latest), since AWS's own docs
// don't state a default and existing callers depend on that behavior.
func resolveDocumentVersionSelector(doc Document, requested string) string {
	switch requested {
	case "":
		return doc.LatestVersion
	case "$DEFAULT":
		return doc.DefaultVersion
	case "$LATEST":
		return doc.LatestVersion
	default:
		return requested
	}
}

// evictOldestDocumentVersions trims vers (oldest-first, insertion order) down
// to at most maxCap entries, evicting the oldest first — except the version
// currently pinned as the document's DefaultVersion, which is never evicted.
//
// Without this guard, a long-lived document (1000+ UpdateDocument calls after
// UpdateDocumentDefaultVersion pinned an old version) could silently evict
// the very version $DEFAULT points at: GetDocument/DescribeDocument would
// then return ErrInvalidDocumentVersion for an explicit or omitted $DEFAULT
// selector instead of resolving it, orphaning the pointer. Fixes bd
// gopherstack-1hg. Mirrors PutParameter's labeled-parameter-version eviction
// guard (parameters.go) — same "never silently destroy a version a caller
// has pinned a reference to" principle, applied to documents. When the
// protected version happens to be among the oldest, the store may retain one
// extra entry beyond maxCap; that is the accepted tradeoff for never
// orphaning $DEFAULT.
func evictOldestDocumentVersions(vers []DocumentVersion, defaultVersion string, maxCap int) []DocumentVersion {
	if len(vers) <= maxCap {
		return vers
	}

	excess := len(vers) - maxCap
	kept := make([]DocumentVersion, 0, len(vers))
	evicted := 0

	for _, v := range vers {
		if evicted < excess && v.DocumentVersion != defaultVersion {
			evicted++

			continue
		}

		kept = append(kept, v)
	}

	return kept
}

// GetDocument retrieves a document's content.
func (b *InMemoryBackend) GetDocument(
	ctx context.Context,
	input *GetDocumentInput,
) (*GetDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetDocument")
	defer b.mu.RUnlock()

	docPtr, exists := b.documentsStore(region).Get(input.Name)
	if !exists {
		return nil, ErrDocumentNotFound
	}

	doc := *docPtr

	target := resolveDocumentVersionSelector(doc, input.DocumentVersion)

	versions := b.documentVersionsStore(region)[input.Name]
	for _, v := range versions {
		if v.DocumentVersion != target {
			continue
		}

		return &GetDocumentOutput{
			Name:              doc.Name,
			Content:           v.Content,
			DisplayName:       doc.DisplayName,
			DocumentType:      doc.DocumentType,
			DocumentFormat:    v.DocumentFormat,
			DocumentVersion:   v.DocumentVersion,
			Status:            v.Status,
			StatusInformation: doc.StatusInformation,
			Requires:          doc.Requires,
			CreatedDate:       v.CreatedDate,
		}, nil
	}

	return nil, ErrInvalidDocumentVersion
}

// documentMatchesFilters returns true when doc satisfies all provided DocumentFilters.
// Supported filter keys: DocumentType, Name.
func documentMatchesFilters(doc Document, filters []DocumentFilter) bool {
	for _, f := range filters {
		var fieldValue string

		switch f.Key {
		case "DocumentType":
			fieldValue = doc.DocumentType
		case filterKeyName:
			fieldValue = doc.Name
		default:
			continue
		}

		if !slices.Contains(f.Values, fieldValue) {
			return false
		}
	}

	return true
}

// DescribeDocument returns document metadata.
func (b *InMemoryBackend) DescribeDocument(
	ctx context.Context,
	input *DescribeDocumentInput,
) (*DescribeDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeDocument")
	defer b.mu.RUnlock()

	docPtr, exists := b.documentsStore(region).Get(input.Name)
	if !exists {
		return nil, ErrDocumentNotFound
	}

	doc := *docPtr

	description := doc.asDocumentDescription(b.miscResourceTagList(region, doc.Name))

	// Honor a specific/$LATEST/$DEFAULT DocumentVersion selector: the
	// per-version fields (DocumentVersion, DocumentFormat, Status) must
	// reflect the resolved version, not always the latest.
	target := resolveDocumentVersionSelector(doc, input.DocumentVersion)
	if target != doc.DocumentVersion {
		found := false

		for _, v := range b.documentVersionsStore(region)[input.Name] {
			if v.DocumentVersion == target {
				description.DocumentVersion = v.DocumentVersion
				description.DocumentFormat = v.DocumentFormat
				description.Status = v.Status
				description.DisplayName = v.DisplayName
				description.Hash, description.Sha1 = documentHashes(v.Content)
				description.HashType = documentHashTypeSha256
				found = true

				break
			}
		}

		if !found {
			return nil, ErrInvalidDocumentVersion
		}
	}

	return &DescribeDocumentOutput{Document: description}, nil
}

// ListDocuments returns a list of document identifiers filtered by key-value criteria.
func (b *InMemoryBackend) ListDocuments(
	ctx context.Context,
	input *ListDocumentsInput,
) (*ListDocumentsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListDocuments")
	defer b.mu.RUnlock()

	// Merge Filters and DocumentFilters (both carry the same shape).
	allFilters := make([]DocumentFilter, 0, len(input.Filters)+len(input.DocumentFilters))
	allFilters = append(allFilters, input.Filters...)
	allFilters = append(allFilters, input.DocumentFilters...)

	docsTable := b.documentsStore(region)
	all := make([]DocumentIdentifier, 0, docsTable.Len())
	for _, docPtr := range docsTable.All() {
		doc := *docPtr
		if !documentMatchesFilters(doc, allFilters) {
			continue
		}

		all = append(all, DocumentIdentifier{
			Name:            doc.Name,
			DisplayName:     doc.DisplayName,
			DocumentType:    doc.DocumentType,
			DocumentFormat:  doc.DocumentFormat,
			DocumentVersion: doc.DocumentVersion,
			SchemaVersion:   doc.SchemaVersion,
			PlatformTypes:   doc.PlatformTypes,
			Requires:        doc.Requires,
			Tags:            b.miscResourceTagList(region, doc.Name),
			TargetType:      doc.TargetType,
			CreatedDate:     doc.CreatedDate,
		})
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultListDocMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &ListDocumentsOutput{DocumentIdentifiers: []DocumentIdentifier{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &ListDocumentsOutput{
		DocumentIdentifiers: all[startIdx:end],
		NextToken:           nextToken,
	}, nil
}

// UpdateDocument increments the document version and updates content.
func (b *InMemoryBackend) UpdateDocument(
	ctx context.Context,
	input *UpdateDocumentInput,
) (*UpdateDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateDocument")
	defer b.mu.Unlock()

	docsTable := b.documentsStore(region)
	docPtr, exists := docsTable.Get(input.Name)
	if !exists {
		return nil, ErrDocumentNotFound
	}

	doc := *docPtr

	// Validate DocumentVersion if provided.
	if input.DocumentVersion != "" {
		switch input.DocumentVersion {
		case "$LATEST", "$DEFAULT", doc.LatestVersion:
			// accepted versions
		default:
			return nil, ErrInvalidDocumentVersion
		}
	}

	latestVer, _ := strconv.Atoi(doc.LatestVersion)
	newVer := strconv.Itoa(latestVer + 1)

	format := input.DocumentFormat
	if format == "" {
		format = doc.DocumentFormat
	}

	now := UnixTimeFloat(time.Now())
	hash, sha1Hex := documentHashes(input.Content)
	doc.Content = input.Content
	doc.DocumentVersion = newVer
	doc.LatestVersion = newVer
	doc.DocumentFormat = format
	doc.Hash = hash
	doc.HashType = documentHashTypeSha256
	doc.Sha1 = sha1Hex

	if input.DisplayName != "" {
		doc.DisplayName = input.DisplayName
	}

	if input.TargetType != "" {
		doc.TargetType = input.TargetType
	}

	if input.Attachments != nil {
		doc.AttachmentsInformation = attachmentsInformation(input.Attachments)
	}

	docsTable.Put(&doc)

	versionStore := b.documentVersionsStore(region)
	versionStore[input.Name] = append(versionStore[input.Name], DocumentVersion{
		Name:             input.Name,
		DisplayName:      doc.DisplayName,
		DocumentVersion:  newVer,
		CreatedDate:      now,
		IsDefaultVersion: false,
		DocumentFormat:   format,
		Status:           statusActive,
		Content:          input.Content,
	})

	if len(versionStore[input.Name]) > maxDocumentVersionCap {
		versionStore[input.Name] = evictOldestDocumentVersions(
			versionStore[input.Name], doc.DefaultVersion, maxDocumentVersionCap,
		)
	}

	return &UpdateDocumentOutput{
		DocumentDescription: doc.asDocumentDescription(b.miscResourceTagList(region, doc.Name)),
	}, nil
}

// DeleteDocument removes a document and all its versions and permissions.
func (b *InMemoryBackend) DeleteDocument(
	ctx context.Context,
	input *DeleteDocumentInput,
) (*DeleteDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteDocument")
	defer b.mu.Unlock()

	if !b.documentsStore(region).Has(input.Name) {
		return nil, ErrDocumentNotFound
	}

	b.documentsStore(region).Delete(input.Name)
	delete(b.documentVersionsStore(region), input.Name)
	delete(b.documentPermissionsStore(region), input.Name)

	// b.documents itself is not pruned here — see the comment on
	// cleanupEmptyParamRegion above for why a Table-backed region entry must
	// never be removed from its outer map once registered.
	cleanupEmptyInnerMap(b.documentVersions, region)
	cleanupEmptyInnerMap(b.documentPermissions, region)

	return &DeleteDocumentOutput{}, nil
}

// DescribeDocumentPermission returns the sharing permissions for a document.
func (b *InMemoryBackend) DescribeDocumentPermission(
	ctx context.Context,
	input *DescribeDocumentPermissionInput,
) (*DescribeDocumentPermissionOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeDocumentPermission")
	defer b.mu.RUnlock()

	if !b.documentsStore(region).Has(input.Name) {
		return nil, ErrDocumentNotFound
	}

	accountIDs := b.documentPermissionsStore(region)[input.Name]
	if accountIDs == nil {
		accountIDs = []string{}
	}

	return &DescribeDocumentPermissionOutput{
		AccountIDs:             accountIDs,
		AccountSharingInfoList: []any{},
	}, nil
}

// ModifyDocumentPermission updates the sharing permissions for a document.
func (b *InMemoryBackend) ModifyDocumentPermission(
	ctx context.Context,
	input *ModifyDocumentPermissionInput,
) (*ModifyDocumentPermissionOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("ModifyDocumentPermission")
	defer b.mu.Unlock()

	if !b.documentsStore(region).Has(input.Name) {
		return nil, ErrDocumentNotFound
	}

	if b.documentPermissions[region] == nil {
		b.documentPermissions[region] = make(map[string][]string)
	}
	permStore := b.documentPermissionsStore(region)
	current := permStore[input.Name]

	for _, id := range input.AccountIDsToAdd {
		if !slices.Contains(current, id) {
			current = append(current, id)
		}
	}

	for _, id := range input.AccountIDsToRemove {
		current = slices.DeleteFunc(current, func(v string) bool { return v == id })
	}

	permStore[input.Name] = current

	return &ModifyDocumentPermissionOutput{}, nil
}

// ListDocumentVersions returns all versions of a document.
func (b *InMemoryBackend) ListDocumentVersions(
	ctx context.Context,
	input *ListDocumentVersionsInput,
) (*ListDocumentVersionsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListDocumentVersions")
	defer b.mu.RUnlock()

	if !b.documentsStore(region).Has(input.Name) {
		return nil, ErrDocumentNotFound
	}

	versions := b.documentVersionsStore(region)[input.Name]

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultListDocMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(versions) {
		return &ListDocumentVersionsOutput{DocumentVersions: []DocumentVersionInfo{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(versions) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(versions)
	}

	page := make([]DocumentVersionInfo, 0, end-startIdx)
	for _, v := range versions[startIdx:end] {
		page = append(page, DocumentVersionInfo{
			Name:             v.Name,
			DisplayName:      v.DisplayName,
			DocumentVersion:  v.DocumentVersion,
			DocumentFormat:   v.DocumentFormat,
			Status:           v.Status,
			CreatedDate:      v.CreatedDate,
			IsDefaultVersion: v.IsDefaultVersion,
		})
	}

	return &ListDocumentVersionsOutput{
		DocumentVersions: page,
		NextToken:        nextToken,
	}, nil
}

// UpdateDocumentDefaultVersion sets the DefaultVersion field on an existing document.
// It fails if the document or the requested version does not exist.
// Returns a no-op success when Name or DocumentVersion is empty (legacy stub compat).
func (b *InMemoryBackend) UpdateDocumentDefaultVersion(
	ctx context.Context,
	input *UpdateDocumentDefaultVersionInput,
) (*UpdateDocumentDefaultVersionOutput, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidationException)
	}

	if input.DocumentVersion == "" {
		return nil, fmt.Errorf("%w: DocumentVersion is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("UpdateDocumentDefaultVersion")
	defer b.mu.Unlock()

	docs := b.documentsStore(region)
	docPtr, exists := docs.Get(input.Name)
	if !exists {
		return nil, fmt.Errorf("%w: document %q not found", ErrDocumentNotFound, input.Name)
	}

	doc := *docPtr

	// Verify the requested version exists in documentVersions.
	found := false

	docVersions := b.documentVersionsStore(region)
	for _, dv := range docVersions[input.Name] {
		if dv.DocumentVersion == input.DocumentVersion {
			found = true

			break
		}
	}

	if !found {
		return nil, fmt.Errorf("%w: version %q not found for document %q",
			ErrInvalidDocumentVersion, input.DocumentVersion, input.Name)
	}

	doc.DefaultVersion = input.DocumentVersion
	docs.Put(&doc)

	// Also mark the version entry.
	versions := docVersions[input.Name]
	for i := range versions {
		versions[i].IsDefaultVersion = versions[i].DocumentVersion == input.DocumentVersion
	}

	docVersions[input.Name] = versions

	return &UpdateDocumentDefaultVersionOutput{
		Description: &DocumentDefaultVersionDescription{
			Name:           input.Name,
			DefaultVersion: input.DocumentVersion,
		},
	}, nil
}

// UpdateDocumentMetadata updates document reviews metadata.
// This is a lightweight implementation that acknowledges the request and
// returns success without modifying stored state (the AWS API is complex
// and review state is not tracked in this in-memory implementation).
func (b *InMemoryBackend) UpdateDocumentMetadata(
	ctx context.Context,
	input *UpdateDocumentMetadataInput,
) (*UpdateDocumentMetadataOutput, error) {
	if err := validateUpdateDocumentMetadataInput(input); err != nil {
		return nil, err
	}

	region := getRegion(ctx)
	b.mu.RLock("UpdateDocumentMetadata")
	defer b.mu.RUnlock()

	if !b.documentsStore(region).Has(input.Name) {
		return nil, fmt.Errorf("%w: document %q not found", ErrDocumentNotFound, input.Name)
	}

	return &UpdateDocumentMetadataOutput{}, nil
}

// validateUpdateDocumentMetadataInput enforces UpdateDocumentMetadata's
// required fields: Name and DocumentReviews (with its own required Action)
// are both required on the real op, but were previously entirely
// unvalidated -- an empty body silently succeeded instead of rejecting with
// ValidationException.
func validateUpdateDocumentMetadataInput(input *UpdateDocumentMetadataInput) error {
	if input.Name == "" {
		return fmt.Errorf("%w: Name is required", ErrValidationException)
	}

	if input.DocumentReviews == nil {
		return fmt.Errorf("%w: DocumentReviews is required", ErrValidationException)
	}

	// Real DocumentReviewAction enum values (aws-sdk-go-v2/service/ssm@v1.73.4 types/enums.go:780-798).
	validActions := []string{"SendForReview", "UpdateReview", "Approve", "Reject"}
	if !slices.Contains(validActions, input.DocumentReviews.Action) {
		return fmt.Errorf("%w: DocumentReviews.Action must be one of %v", ErrValidationException, validActions)
	}

	return nil
}

// ListDocumentMetadataHistory returns an empty approval history.
// The in-memory backend does not track document review history; this returns
// a well-formed empty response consistent with the stateless stub approach.
func (b *InMemoryBackend) ListDocumentMetadataHistory(
	ctx context.Context,
	input *ListDocumentMetadataHistoryInput,
) (*ListDocumentMetadataHistoryOutput, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidationException)
	}

	// DocumentMetadataEnum has exactly one real value (types/enums.go:727-745).
	if input.Metadata != "DocumentReviews" {
		return nil, fmt.Errorf(`%w: Metadata must be "DocumentReviews"`, ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.RLock("ListDocumentMetadataHistory")
	defer b.mu.RUnlock()

	doc, exists := b.documentsStore(region).Get(input.Name)
	if !exists {
		return nil, fmt.Errorf("%w: document %q not found", ErrDocumentNotFound, input.Name)
	}

	return &ListDocumentMetadataHistoryOutput{
		Name:            doc.Name,
		DocumentVersion: doc.DocumentVersion,
		Metadata: &DocumentMetadataResponseInfo{
			ReviewerResponse: []DocumentReviewerResponseSource{},
		},
	}, nil
}
