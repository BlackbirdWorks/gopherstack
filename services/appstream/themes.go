package appstream

import (
	"fmt"
	"time"
)

// themeStylingValues are the enum values of appstream@v1.64.5
// types.ThemeStyling (types/enums.go:1018-1026).
var themeStylingValues = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"LIGHT_BLUE": true,
	"BLUE":       true,
	"PINK":       true,
	"RED":        true,
}

// themeURL derives a pseudo-URL from an S3Location, matching this repo's
// existing convention for S3-object-backed response URLs (see
// services/amplify/artifacts.go, services/serverlessrepo/models.go).
func themeURL(loc S3Location) string {
	if loc.S3Bucket == "" {
		return ""
	}

	return fmt.Sprintf("https://s3.amazonaws.com/%s/%s", loc.S3Bucket, loc.S3Key)
}

type storedThemeFooterLink struct {
	DisplayName   string `json:"displayName"`
	FooterLinkURL string `json:"footerLinkURL"`
}

type storedTheme struct {
	CreatedTime                time.Time               `json:"createdTime"`
	StackName                  string                  `json:"stackName"`
	State                      string                  `json:"state"`
	ThemeStyling               string                  `json:"themeStyling"`
	ThemeTitleText             string                  `json:"themeTitleText"`
	FaviconS3Location          S3Location              `json:"faviconS3Location"`
	OrganizationLogoS3Location S3Location              `json:"organizationLogoS3Location"`
	ThemeFooterLinks           []storedThemeFooterLink `json:"themeFooterLinks"`
}

func (t *storedTheme) toTheme() *Theme {
	links := make([]ThemeFooterLink, 0, len(t.ThemeFooterLinks))
	for _, l := range t.ThemeFooterLinks {
		links = append(links, ThemeFooterLink(l))
	}

	return &Theme{
		CreatedTime:                t.CreatedTime,
		StackName:                  t.StackName,
		State:                      t.State,
		ThemeStyling:               t.ThemeStyling,
		ThemeTitleText:             t.ThemeTitleText,
		ThemeFaviconURL:            themeURL(t.FaviconS3Location),
		ThemeOrganizationLogoURL:   themeURL(t.OrganizationLogoS3Location),
		FaviconS3Location:          t.FaviconS3Location,
		OrganizationLogoS3Location: t.OrganizationLogoS3Location,
		ThemeFooterLinks:           links,
	}
}

// CreateThemeForStack creates a theme for a stack. FaviconS3Location,
// OrganizationLogoS3Location, ThemeStyling and TitleText are required
// members of CreateThemeForStackInput (api_op_CreateThemeForStack.go:29-59).
func (b *InMemoryBackend) CreateThemeForStack(
	stackName string,
	faviconS3Location, organizationLogoS3Location S3Location,
	themeStyling, titleText string,
	footerLinks []ThemeFooterLink,
) (*Theme, error) {
	if faviconS3Location.S3Bucket == "" {
		return nil, fmt.Errorf("%w: FaviconS3Location is required", ErrSerialization)
	}

	if organizationLogoS3Location.S3Bucket == "" {
		return nil, fmt.Errorf("%w: OrganizationLogoS3Location is required", ErrSerialization)
	}

	if !themeStylingValues[themeStyling] {
		return nil, fmt.Errorf("%w: ThemeStyling is required and must be one of %v",
			ErrSerialization, []string{"LIGHT_BLUE", "BLUE", "PINK", "RED"})
	}

	if titleText == "" {
		return nil, fmt.Errorf("%w: TitleText is required", ErrSerialization)
	}

	b.mu.Lock("CreateThemeForStack")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackName) {
		return nil, ErrNotFound
	}

	if b.themes.Has(stackName) {
		return nil, ErrAlreadyExists
	}

	links := make([]storedThemeFooterLink, 0, len(footerLinks))
	for _, l := range footerLinks {
		links = append(links, storedThemeFooterLink(l))
	}

	th := &storedTheme{
		CreatedTime:                time.Now().UTC(),
		StackName:                  stackName,
		State:                      "ENABLED",
		ThemeStyling:               themeStyling,
		ThemeTitleText:             titleText,
		FaviconS3Location:          faviconS3Location,
		OrganizationLogoS3Location: organizationLogoS3Location,
		ThemeFooterLinks:           links,
	}
	b.themes.Put(th)

	return th.toTheme(), nil
}

// DeleteThemeForStack removes a stack theme.
func (b *InMemoryBackend) DeleteThemeForStack(stackName string) error {
	b.mu.Lock("DeleteThemeForStack")
	defer b.mu.Unlock()

	if !b.themes.Has(stackName) {
		return ErrNotFound
	}

	b.themes.Delete(stackName)

	return nil
}

// DescribeThemeForStack returns the theme for a stack.
func (b *InMemoryBackend) DescribeThemeForStack(stackName string) (*Theme, error) {
	b.mu.RLock("DescribeThemeForStack")
	defer b.mu.RUnlock()

	th, ok := b.themes.Get(stackName)
	if !ok {
		return nil, ErrNotFound
	}

	return th.toTheme(), nil
}

// UpdateThemeForStack updates the theme for a stack. Every field on opts
// besides StackName is optional on the real wire (UpdateThemeForStackInput,
// api_op_UpdateThemeForStack.go:29-64) -- omitted means unchanged.
// AttributesToDelete is applied after every set field so a delete always
// wins over a same-request set.
func (b *InMemoryBackend) UpdateThemeForStack(stackName string, opts ThemeUpdateOptions) (*Theme, error) {
	b.mu.Lock("UpdateThemeForStack")
	defer b.mu.Unlock()

	th, ok := b.themes.Get(stackName)
	if !ok {
		return nil, ErrNotFound
	}

	applyThemeUpdate(th, opts)

	return th.toTheme(), nil
}

func applyThemeUpdate(th *storedTheme, opts ThemeUpdateOptions) {
	if opts.FaviconS3Location != nil {
		th.FaviconS3Location = *opts.FaviconS3Location
	}

	if opts.OrganizationLogoS3Location != nil {
		th.OrganizationLogoS3Location = *opts.OrganizationLogoS3Location
	}

	if opts.TitleText != nil {
		th.ThemeTitleText = *opts.TitleText
	}

	if opts.ThemeStyling != "" {
		th.ThemeStyling = opts.ThemeStyling
	}

	if opts.State != "" {
		th.State = opts.State
	}

	if opts.FooterLinks != nil {
		links := make([]storedThemeFooterLink, 0, len(opts.FooterLinks))
		for _, l := range opts.FooterLinks {
			links = append(links, storedThemeFooterLink(l))
		}

		th.ThemeFooterLinks = links
	}

	for _, attr := range opts.AttributesToDelete {
		if attr == "FOOTER_LINKS" {
			th.ThemeFooterLinks = nil
		}
	}
}
