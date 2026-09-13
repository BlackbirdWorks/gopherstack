package appstream

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	// StackAttribute enum values a real UpdateStackInput.AttributesToDelete
	// may carry (appstream@v1.64.5 types/enums.go) that this backend models.
	stackAttrStorageConnectors           = "STORAGE_CONNECTORS"
	stackAttrRedirectURL                 = "REDIRECT_URL"
	stackAttrFeedbackURL                 = "FEEDBACK_URL"
	stackAttrUserSettings                = "USER_SETTINGS"
	stackAttrEmbedHostDomains            = "EMBED_HOST_DOMAINS"
	stackAttrAccessEndpoints             = "ACCESS_ENDPOINTS"
	stackAttrStreamingExperienceSettings = "STREAMING_EXPERIENCE_SETTINGS"
	stackAttrContentRedirection          = "CONTENT_REDIRECTION"
)

type storedStack struct {
	ApplicationSettings         *ApplicationSettings         `json:"applicationSettings,omitempty"`
	ContentRedirection          *ContentRedirection          `json:"contentRedirection,omitempty"`
	StreamingExperienceSettings *StreamingExperienceSettings `json:"streamingExperienceSettings,omitempty"`
	CreatedTime                 time.Time                    `json:"createdTime"`
	Tags                        map[string]string            `json:"tags"`
	Name                        string                       `json:"name"`
	Arn                         string                       `json:"arn"`
	DisplayName                 string                       `json:"displayName"`
	Description                 string                       `json:"description"`
	RedirectURL                 string                       `json:"redirectUrl,omitempty"`
	FeedbackURL                 string                       `json:"feedbackUrl,omitempty"`
	EmbedHostDomains            []string                     `json:"embedHostDomains,omitempty"`
	UserSettings                []UserSetting                `json:"userSettings,omitempty"`
	StorageConnectors           []StorageConnector           `json:"storageConnectors,omitempty"`
	AccessEndpoints             []AccessEndpoint             `json:"accessEndpoints,omitempty"`
}

func (s *storedStack) toStack() *Stack {
	tags := make(map[string]string)
	maps.Copy(tags, s.Tags)

	stack := &Stack{
		CreatedTime:       s.CreatedTime,
		Tags:              tags,
		Name:              s.Name,
		Arn:               s.Arn,
		DisplayName:       s.DisplayName,
		Description:       s.Description,
		RedirectURL:       s.RedirectURL,
		FeedbackURL:       s.FeedbackURL,
		EmbedHostDomains:  append([]string(nil), s.EmbedHostDomains...),
		UserSettings:      append([]UserSetting(nil), s.UserSettings...),
		StorageConnectors: append([]StorageConnector(nil), s.StorageConnectors...),
		AccessEndpoints:   append([]AccessEndpoint(nil), s.AccessEndpoints...),
	}

	if s.ApplicationSettings != nil {
		as := *s.ApplicationSettings
		stack.ApplicationSettings = &as
	}

	if s.ContentRedirection != nil {
		cr := *s.ContentRedirection
		if cr.HostToClient != nil {
			h := *cr.HostToClient
			cr.HostToClient = &h
		}

		stack.ContentRedirection = &cr
	}

	if s.StreamingExperienceSettings != nil {
		ses := *s.StreamingExperienceSettings
		stack.StreamingExperienceSettings = &ses
	}

	return stack
}

func (b *InMemoryBackend) stackARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("stack/%s", name))
}

// CreateStack creates a new stack.
func (b *InMemoryBackend) CreateStack(name string, opts CreateStackOptions) (*Stack, error) {
	b.mu.Lock("CreateStack")
	defer b.mu.Unlock()

	if b.stacks.Has(name) {
		return nil, ErrAlreadyExists
	}

	stackArn := b.stackARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, opts.Tags)

	s := &storedStack{
		CreatedTime:                 time.Now().UTC(),
		Tags:                        storedTags,
		Name:                        name,
		Arn:                         stackArn,
		DisplayName:                 opts.DisplayName,
		Description:                 opts.Description,
		RedirectURL:                 opts.RedirectURL,
		FeedbackURL:                 opts.FeedbackURL,
		EmbedHostDomains:            append([]string(nil), opts.EmbedHostDomains...),
		UserSettings:                append([]UserSetting(nil), opts.UserSettings...),
		StorageConnectors:           append([]StorageConnector(nil), opts.StorageConnectors...),
		AccessEndpoints:             append([]AccessEndpoint(nil), opts.AccessEndpoints...),
		ApplicationSettings:         b.cloneApplicationSettings(opts.ApplicationSettings),
		ContentRedirection:          cloneContentRedirection(opts.ContentRedirection),
		StreamingExperienceSettings: cloneStreamingExperienceSettings(opts.StreamingExperienceSettings),
	}
	b.stacks.Put(s)
	b.tags[stackArn] = storedTags

	return s.toStack(), nil
}

// cloneApplicationSettings mirrors CreateUsageReportSubscription's
// bucket-naming convention: real AWS creates one S3 bucket per
// account+Region the first time persistent application settings are
// enabled for that account (doc comment on
// types.ApplicationSettingsResponse.S3BucketName).
func (b *InMemoryBackend) cloneApplicationSettings(as *ApplicationSettings) *ApplicationSettings {
	if as == nil {
		return nil
	}

	out := *as
	if out.Enabled && out.S3BucketName == "" {
		out.S3BucketName = fmt.Sprintf("appstream-app-settings-%s-%s", b.region, b.accountID)
	}

	return &out
}

func cloneContentRedirection(cr *ContentRedirection) *ContentRedirection {
	if cr == nil {
		return nil
	}

	out := *cr
	if out.HostToClient != nil {
		h := *out.HostToClient
		out.HostToClient = &h
	}

	return &out
}

func cloneStreamingExperienceSettings(s *StreamingExperienceSettings) *StreamingExperienceSettings {
	if s == nil {
		return nil
	}

	out := *s

	return &out
}

// DescribeStacks returns stacks, optionally filtered by names.
func (b *InMemoryBackend) DescribeStacks(names []string) ([]*Stack, error) {
	b.mu.RLock("DescribeStacks")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*Stack

		for _, name := range names {
			s, ok := b.stacks.Get(name)
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, s.toStack())
		}

		return result, nil
	}

	result := make([]*Stack, 0, b.stacks.Len())
	for _, s := range b.stacks.All() {
		result = append(result, s.toStack())
	}

	return result, nil
}

// applyStackAttributesToDelete clears the fields named by attrs, applied
// after every set field so a delete always wins over a same-request set.
func applyStackAttributesToDelete(s *storedStack, attrs []string) {
	for _, attr := range attrs {
		switch attr {
		case stackAttrStorageConnectors:
			s.StorageConnectors = nil
		case stackAttrRedirectURL:
			s.RedirectURL = ""
		case stackAttrFeedbackURL:
			s.FeedbackURL = ""
		case stackAttrUserSettings:
			s.UserSettings = nil
		case stackAttrEmbedHostDomains:
			s.EmbedHostDomains = nil
		case stackAttrAccessEndpoints:
			s.AccessEndpoints = nil
		case stackAttrStreamingExperienceSettings:
			s.StreamingExperienceSettings = nil
		case stackAttrContentRedirection:
			s.ContentRedirection = nil
		}
	}
}

// UpdateStack updates mutable fields of an existing stack.
func (b *InMemoryBackend) UpdateStack(name string, opts UpdateStackOptions) (*Stack, error) {
	b.mu.Lock("UpdateStack")
	defer b.mu.Unlock()

	s, ok := b.stacks.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	if opts.DisplayName != "" {
		s.DisplayName = opts.DisplayName
	}

	if opts.Description != "" {
		s.Description = opts.Description
	}

	if opts.RedirectURL != "" {
		s.RedirectURL = opts.RedirectURL
	}

	if opts.FeedbackURL != "" {
		s.FeedbackURL = opts.FeedbackURL
	}

	if len(opts.EmbedHostDomains) > 0 {
		s.EmbedHostDomains = slices.Clone(opts.EmbedHostDomains)
	}

	if len(opts.UserSettings) > 0 {
		s.UserSettings = slices.Clone(opts.UserSettings)
	}

	if opts.DeleteStorageConnectors != nil && *opts.DeleteStorageConnectors {
		s.StorageConnectors = nil
	} else if len(opts.StorageConnectors) > 0 {
		s.StorageConnectors = slices.Clone(opts.StorageConnectors)
	}

	if len(opts.AccessEndpoints) > 0 {
		s.AccessEndpoints = slices.Clone(opts.AccessEndpoints)
	}

	if opts.ApplicationSettings != nil {
		s.ApplicationSettings = b.cloneApplicationSettings(opts.ApplicationSettings)
	}

	if opts.ContentRedirection != nil {
		s.ContentRedirection = cloneContentRedirection(opts.ContentRedirection)
	}

	if opts.StreamingExperienceSettings != nil {
		s.StreamingExperienceSettings = cloneStreamingExperienceSettings(opts.StreamingExperienceSettings)
	}

	applyStackAttributesToDelete(s, opts.AttributesToDelete)

	return s.toStack(), nil
}

// DeleteStack removes a stack. Returns ErrResourceInUse if any fleet is associated with the stack.
func (b *InMemoryBackend) DeleteStack(name string) error {
	b.mu.Lock("DeleteStack")
	defer b.mu.Unlock()

	s, ok := b.stacks.Get(name)
	if !ok {
		return ErrNotFound
	}

	for _, stacks := range b.associations {
		if stacks[name] {
			return ErrResourceInUse
		}
	}

	delete(b.tags, s.Arn)
	b.stacks.Delete(name)

	return nil
}
