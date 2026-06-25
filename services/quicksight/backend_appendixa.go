package quicksight

import (
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Errors for Appendix-A resource families. They reuse the same AWS error codes
// as the rest of the service so httpErr maps them to the correct HTTP status.
var (
	// ErrFolderNotFound is returned when a folder does not exist.
	ErrFolderNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrFolderAlreadyExists is returned when a folder already exists.
	ErrFolderAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrTemplateNotFound is returned when a template does not exist.
	ErrTemplateNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrTemplateAlreadyExists is returned when a template already exists.
	ErrTemplateAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrThemeNotFound is returned when a theme does not exist.
	ErrThemeNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrThemeAlreadyExists is returned when a theme already exists.
	ErrThemeAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrVPCConnectionNotFound is returned when a VPC connection does not exist.
	ErrVPCConnectionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrVPCConnectionAlreadyExists is returned when a VPC connection already exists.
	ErrVPCConnectionAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
	// ErrBrandNotFound is returned when a brand does not exist.
	ErrBrandNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrBrandAlreadyExists is returned when a brand already exists.
	ErrBrandAlreadyExists = awserr.New(errConflictException, awserr.ErrAlreadyExists)
)

// ---- stored types ----

type storedFolder struct {
	CreatedTime     time.Time `json:"createdTime"`
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	FolderID        string    `json:"folderId"`
	Arn             string    `json:"arn"`
	Name            string    `json:"name"`
	FolderType      string    `json:"folderType"`
}

func (f *storedFolder) toFolder() *Folder {
	return &Folder{
		CreatedTime:     f.CreatedTime,
		LastUpdatedTime: f.LastUpdatedTime,
		FolderID:        f.FolderID,
		Arn:             f.Arn,
		Name:            f.Name,
		FolderType:      f.FolderType,
	}
}

type storedTemplate struct {
	CreatedTime     time.Time `json:"createdTime"`
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	TemplateID      string    `json:"templateId"`
	Arn             string    `json:"arn"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
	VersionNumber   int64     `json:"versionNumber"`
}

func (t *storedTemplate) toTemplate() *Template {
	return &Template{
		CreatedTime:     t.CreatedTime,
		LastUpdatedTime: t.LastUpdatedTime,
		TemplateID:      t.TemplateID,
		Arn:             t.Arn,
		Name:            t.Name,
		Status:          t.Status,
		VersionNumber:   t.VersionNumber,
	}
}

type storedTheme struct {
	CreatedTime     time.Time `json:"createdTime"`
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	ThemeID         string    `json:"themeId"`
	Arn             string    `json:"arn"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
	VersionNumber   int64     `json:"versionNumber"`
}

func (t *storedTheme) toTheme() *Theme {
	return &Theme{
		CreatedTime:     t.CreatedTime,
		LastUpdatedTime: t.LastUpdatedTime,
		ThemeID:         t.ThemeID,
		Arn:             t.Arn,
		Name:            t.Name,
		Status:          t.Status,
		VersionNumber:   t.VersionNumber,
	}
}

type storedVPCConnection struct {
	CreatedTime       time.Time `json:"createdTime"`
	LastUpdatedTime   time.Time `json:"lastUpdatedTime"`
	VPCConnectionID   string    `json:"vpcConnectionId"`
	Arn               string    `json:"arn"`
	Name              string    `json:"name"`
	Status            string    `json:"status"`
	AvailabilityState string    `json:"availabilityState"`
}

func (v *storedVPCConnection) toVPCConnection() *VPCConnection {
	return &VPCConnection{
		CreatedTime:       v.CreatedTime,
		LastUpdatedTime:   v.LastUpdatedTime,
		VPCConnectionID:   v.VPCConnectionID,
		Arn:               v.Arn,
		Name:              v.Name,
		Status:            v.Status,
		AvailabilityState: v.AvailabilityState,
	}
}

type storedBrand struct {
	CreatedTime     time.Time `json:"createdTime"`
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	BrandID         string    `json:"brandId"`
	Arn             string    `json:"arn"`
	Name            string    `json:"name"`
	Status          string    `json:"status"`
}

func (b *storedBrand) toBrand() *Brand {
	return &Brand{
		CreatedTime:     b.CreatedTime,
		LastUpdatedTime: b.LastUpdatedTime,
		BrandID:         b.BrandID,
		Arn:             b.Arn,
		Name:            b.Name,
		Status:          b.Status,
	}
}

// ---- key helpers ----

func appendixKey(accountID, resID string) string {
	return accountID + "/" + resID
}

// ---- Folders ----

func (b *InMemoryBackend) CreateFolder(accountID, folderID, name, folderType string) (*Folder, error) {
	if folderID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateFolder")
	defer b.mu.Unlock()

	key := appendixKey(accountID, folderID)
	if _, exists := b.folders[key]; exists {
		return nil, ErrFolderAlreadyExists
	}

	if folderType == "" {
		folderType = "SHARED"
	}

	now := time.Now().UTC()
	f := &storedFolder{
		CreatedTime:     now,
		LastUpdatedTime: now,
		FolderID:        folderID,
		Arn:             b.buildARN("folder", folderID),
		Name:            name,
		FolderType:      folderType,
	}
	b.folders[key] = f

	return f.toFolder(), nil
}

func (b *InMemoryBackend) DescribeFolder(accountID, folderID string) (*Folder, error) {
	b.mu.RLock("DescribeFolder")
	defer b.mu.RUnlock()

	f, ok := b.folders[appendixKey(accountID, folderID)]
	if !ok {
		return nil, ErrFolderNotFound
	}

	return f.toFolder(), nil
}

func (b *InMemoryBackend) UpdateFolder(accountID, folderID, name string) (*Folder, error) {
	b.mu.Lock("UpdateFolder")
	defer b.mu.Unlock()

	f, ok := b.folders[appendixKey(accountID, folderID)]
	if !ok {
		return nil, ErrFolderNotFound
	}

	if name != "" {
		f.Name = name
	}
	f.LastUpdatedTime = time.Now().UTC()

	return f.toFolder(), nil
}

func (b *InMemoryBackend) DeleteFolder(accountID, folderID string) error {
	b.mu.Lock("DeleteFolder")
	defer b.mu.Unlock()

	key := appendixKey(accountID, folderID)
	f, ok := b.folders[key]
	if !ok {
		return ErrFolderNotFound
	}

	delete(b.tags, f.Arn)
	delete(b.folders, key)

	return nil
}

func (b *InMemoryBackend) ListFolders(accountID string, maxResults int32, nextToken string) ([]*Folder, string, error) {
	b.mu.RLock("ListFolders")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	all := make([]*Folder, 0)
	for k, f := range b.folders {
		if strings.HasPrefix(k, prefix) {
			all = append(all, f.toFolder())
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].FolderID < all[j].FolderID })

	result, next := paginateSlice(len(all), maxResults, nextToken, func(i int) *Folder { return all[i] })

	return result, next, nil
}

// ---- Templates ----

func (b *InMemoryBackend) CreateTemplate(accountID, templateID, name string) (*Template, error) {
	if templateID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTemplate")
	defer b.mu.Unlock()

	key := appendixKey(accountID, templateID)
	if _, exists := b.templates[key]; exists {
		return nil, ErrTemplateAlreadyExists
	}

	now := time.Now().UTC()
	t := &storedTemplate{
		CreatedTime:     now,
		LastUpdatedTime: now,
		TemplateID:      templateID,
		Arn:             b.buildARN("template", templateID),
		Name:            name,
		Status:          statusCreationSuccessful,
		VersionNumber:   1,
	}
	b.templates[key] = t

	return t.toTemplate(), nil
}

func (b *InMemoryBackend) DescribeTemplate(accountID, templateID string) (*Template, error) {
	b.mu.RLock("DescribeTemplate")
	defer b.mu.RUnlock()

	t, ok := b.templates[appendixKey(accountID, templateID)]
	if !ok {
		return nil, ErrTemplateNotFound
	}

	return t.toTemplate(), nil
}

func (b *InMemoryBackend) UpdateTemplate(accountID, templateID, name string) (*Template, error) {
	b.mu.Lock("UpdateTemplate")
	defer b.mu.Unlock()

	t, ok := b.templates[appendixKey(accountID, templateID)]
	if !ok {
		return nil, ErrTemplateNotFound
	}

	if name != "" {
		t.Name = name
	}
	t.LastUpdatedTime = time.Now().UTC()
	t.VersionNumber++
	t.Status = statusUpdateSuccessful

	return t.toTemplate(), nil
}

func (b *InMemoryBackend) DeleteTemplate(accountID, templateID string) error {
	b.mu.Lock("DeleteTemplate")
	defer b.mu.Unlock()

	key := appendixKey(accountID, templateID)
	t, ok := b.templates[key]
	if !ok {
		return ErrTemplateNotFound
	}

	delete(b.tags, t.Arn)
	delete(b.templates, key)

	return nil
}

func (b *InMemoryBackend) ListTemplates(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Template, string, error) {
	b.mu.RLock("ListTemplates")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	all := make([]*Template, 0)
	for k, t := range b.templates {
		if strings.HasPrefix(k, prefix) {
			all = append(all, t.toTemplate())
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].TemplateID < all[j].TemplateID })

	result, next := paginateSlice(len(all), maxResults, nextToken, func(i int) *Template { return all[i] })

	return result, next, nil
}

// ---- Themes ----

func (b *InMemoryBackend) CreateTheme(accountID, themeID, name string) (*Theme, error) {
	if themeID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTheme")
	defer b.mu.Unlock()

	key := appendixKey(accountID, themeID)
	if _, exists := b.themes[key]; exists {
		return nil, ErrThemeAlreadyExists
	}

	now := time.Now().UTC()
	t := &storedTheme{
		CreatedTime:     now,
		LastUpdatedTime: now,
		ThemeID:         themeID,
		Arn:             b.buildARN("theme", themeID),
		Name:            name,
		Status:          statusCreationSuccessful,
		VersionNumber:   1,
	}
	b.themes[key] = t

	return t.toTheme(), nil
}

func (b *InMemoryBackend) DescribeTheme(accountID, themeID string) (*Theme, error) {
	b.mu.RLock("DescribeTheme")
	defer b.mu.RUnlock()

	t, ok := b.themes[appendixKey(accountID, themeID)]
	if !ok {
		return nil, ErrThemeNotFound
	}

	return t.toTheme(), nil
}

func (b *InMemoryBackend) UpdateTheme(accountID, themeID, name string) (*Theme, error) {
	b.mu.Lock("UpdateTheme")
	defer b.mu.Unlock()

	t, ok := b.themes[appendixKey(accountID, themeID)]
	if !ok {
		return nil, ErrThemeNotFound
	}

	if name != "" {
		t.Name = name
	}
	t.LastUpdatedTime = time.Now().UTC()
	t.VersionNumber++
	t.Status = statusUpdateSuccessful

	return t.toTheme(), nil
}

func (b *InMemoryBackend) DeleteTheme(accountID, themeID string) error {
	b.mu.Lock("DeleteTheme")
	defer b.mu.Unlock()

	key := appendixKey(accountID, themeID)
	t, ok := b.themes[key]
	if !ok {
		return ErrThemeNotFound
	}

	delete(b.tags, t.Arn)
	delete(b.themes, key)

	return nil
}

func (b *InMemoryBackend) ListThemes(accountID string, maxResults int32, nextToken string) ([]*Theme, string, error) {
	b.mu.RLock("ListThemes")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	all := make([]*Theme, 0)
	for k, t := range b.themes {
		if strings.HasPrefix(k, prefix) {
			all = append(all, t.toTheme())
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ThemeID < all[j].ThemeID })

	result, next := paginateSlice(len(all), maxResults, nextToken, func(i int) *Theme { return all[i] })

	return result, next, nil
}

// ---- VPC Connections ----

func (b *InMemoryBackend) CreateVPCConnection(accountID, vpcConnectionID, name string) (*VPCConnection, error) {
	if vpcConnectionID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateVPCConnection")
	defer b.mu.Unlock()

	key := appendixKey(accountID, vpcConnectionID)
	if _, exists := b.vpcConnections[key]; exists {
		return nil, ErrVPCConnectionAlreadyExists
	}

	now := time.Now().UTC()
	v := &storedVPCConnection{
		CreatedTime:       now,
		LastUpdatedTime:   now,
		VPCConnectionID:   vpcConnectionID,
		Arn:               b.buildARN("vpcConnection", vpcConnectionID),
		Name:              name,
		Status:            statusCreationInProgress,
		AvailabilityState: "UNAVAILABLE",
	}
	b.vpcConnections[key] = v

	return v.toVPCConnection(), nil
}

func (b *InMemoryBackend) DescribeVPCConnection(accountID, vpcConnectionID string) (*VPCConnection, error) {
	b.mu.RLock("DescribeVPCConnection")
	defer b.mu.RUnlock()

	v, ok := b.vpcConnections[appendixKey(accountID, vpcConnectionID)]
	if !ok {
		return nil, ErrVPCConnectionNotFound
	}

	return v.toVPCConnection(), nil
}

func (b *InMemoryBackend) UpdateVPCConnection(accountID, vpcConnectionID, name string) (*VPCConnection, error) {
	b.mu.Lock("UpdateVPCConnection")
	defer b.mu.Unlock()

	v, ok := b.vpcConnections[appendixKey(accountID, vpcConnectionID)]
	if !ok {
		return nil, ErrVPCConnectionNotFound
	}

	if name != "" {
		v.Name = name
	}
	v.LastUpdatedTime = time.Now().UTC()
	v.Status = statusUpdateSuccessful
	v.AvailabilityState = "AVAILABLE"

	return v.toVPCConnection(), nil
}

func (b *InMemoryBackend) DeleteVPCConnection(accountID, vpcConnectionID string) (*VPCConnection, error) {
	b.mu.Lock("DeleteVPCConnection")
	defer b.mu.Unlock()

	key := appendixKey(accountID, vpcConnectionID)
	v, ok := b.vpcConnections[key]
	if !ok {
		return nil, ErrVPCConnectionNotFound
	}

	v.Status = statusDeleted
	deleted := v.toVPCConnection()
	delete(b.tags, v.Arn)
	delete(b.vpcConnections, key)

	return deleted, nil
}

func (b *InMemoryBackend) ListVPCConnections(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*VPCConnection, string, error) {
	b.mu.RLock("ListVPCConnections")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	all := make([]*VPCConnection, 0)
	for k, v := range b.vpcConnections {
		if strings.HasPrefix(k, prefix) {
			all = append(all, v.toVPCConnection())
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].VPCConnectionID < all[j].VPCConnectionID })

	result, next := paginateSlice(len(all), maxResults, nextToken, func(i int) *VPCConnection { return all[i] })

	return result, next, nil
}

// ---- Brands ----

func (b *InMemoryBackend) CreateBrand(accountID, brandID, name string) (*Brand, error) {
	if brandID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateBrand")
	defer b.mu.Unlock()

	key := appendixKey(accountID, brandID)
	if _, exists := b.brands[key]; exists {
		return nil, ErrBrandAlreadyExists
	}

	now := time.Now().UTC()
	br := &storedBrand{
		CreatedTime:     now,
		LastUpdatedTime: now,
		BrandID:         brandID,
		Arn:             b.buildARN("brand", brandID),
		Name:            name,
		Status:          statusCreationSuccessful,
	}
	b.brands[key] = br

	return br.toBrand(), nil
}

func (b *InMemoryBackend) DescribeBrand(accountID, brandID string) (*Brand, error) {
	b.mu.RLock("DescribeBrand")
	defer b.mu.RUnlock()

	br, ok := b.brands[appendixKey(accountID, brandID)]
	if !ok {
		return nil, ErrBrandNotFound
	}

	return br.toBrand(), nil
}

func (b *InMemoryBackend) UpdateBrand(accountID, brandID, name string) (*Brand, error) {
	b.mu.Lock("UpdateBrand")
	defer b.mu.Unlock()

	br, ok := b.brands[appendixKey(accountID, brandID)]
	if !ok {
		return nil, ErrBrandNotFound
	}

	if name != "" {
		br.Name = name
	}
	br.LastUpdatedTime = time.Now().UTC()
	br.Status = statusUpdateSuccessful

	return br.toBrand(), nil
}

func (b *InMemoryBackend) DeleteBrand(accountID, brandID string) error {
	b.mu.Lock("DeleteBrand")
	defer b.mu.Unlock()

	key := appendixKey(accountID, brandID)
	br, ok := b.brands[key]
	if !ok {
		return ErrBrandNotFound
	}

	delete(b.tags, br.Arn)
	delete(b.brands, key)

	return nil
}

func (b *InMemoryBackend) ListBrands(accountID string, maxResults int32, nextToken string) ([]*Brand, string, error) {
	b.mu.RLock("ListBrands")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	all := make([]*Brand, 0)
	for k, br := range b.brands {
		if strings.HasPrefix(k, prefix) {
			all = append(all, br.toBrand())
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].BrandID < all[j].BrandID })

	result, next := paginateSlice(len(all), maxResults, nextToken, func(i int) *Brand { return all[i] })

	return result, next, nil
}

// paginateSlice applies maxResults/nextToken pagination to an already-sorted slice.
// The getter func extracts element i from the caller's slice.
func paginateSlice[T any](length int, maxResults int32, nextToken string, getter func(int) T) ([]T, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		if off, err := decodePageToken(nextToken); err == nil {
			start = off
		}
	}

	end := start + int(maxResults)

	var next string
	if end < length {
		next = encodePageToken(end)
	} else {
		end = length
	}

	result := make([]T, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, getter(i))
	}

	return result, next
}
