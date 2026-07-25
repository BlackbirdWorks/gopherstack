package serverlessrepo

import (
	"fmt"
	"regexp"
	"time"
)

// validNameRe matches AWS SAR-valid application names: alphanumeric and hyphens only.
var validNameRe = regexp.MustCompile(`^[a-zA-Z0-9\-]+$`)

// validSemanticVersionRe matches a basic semver prefix (major.minor.patch).
var validSemanticVersionRe = regexp.MustCompile(`^\d+\.\d+\.\d+`)

// AWS SAR field length limits.
const (
	maxNameLength            = 140
	maxAuthorLength          = 127
	maxDescriptionLength     = 256
	maxLabelLength           = 127
	maxLabelCount            = 10
	maxSemanticVersionLength = 255
)

// ParameterDefinition represents a CloudFormation parameter definition for an application
// version. Matches aws-sdk-go-v2/service/serverlessapplicationrepository/types.ParameterDefinition
// field-for-field; gopherstack never derives non-empty parameter definitions from a template
// (that requires parsing the AWS SAM template body, which is out of scope), so this shape is
// exercised only via the always-empty slice CreateApplicationVersion/GetApplication return, but
// is kept fully field-accurate for callers seeding state directly.
type ParameterDefinition struct {
	DefaultValue          string   `json:"defaultValue,omitempty"`
	Description           string   `json:"description,omitempty"`
	Name                  string   `json:"name"`
	Type                  string   `json:"type,omitempty"`
	AllowedPattern        string   `json:"allowedPattern,omitempty"`
	ConstraintDescription string   `json:"constraintDescription,omitempty"`
	ReferencedByResources []string `json:"referencedByResources"`
	AllowedValues         []string `json:"allowedValues,omitempty"`
	MaxLength             int      `json:"maxLength,omitempty"`
	MaxValue              int      `json:"maxValue,omitempty"`
	MinLength             int      `json:"minLength,omitempty"`
	MinValue              int      `json:"minValue,omitempty"`
	NoEcho                bool     `json:"noEcho,omitempty"`
}

// Application represents an AWS Serverless Application Repository application.
type Application struct {
	CreationTime      time.Time `json:"creationTime"`
	LicenseURL        string    `json:"licenseUrl,omitempty"`
	Name              string    `json:"name"`
	Description       string    `json:"description,omitempty"`
	Author            string    `json:"author,omitempty"`
	HomePageURL       string    `json:"homePageUrl,omitempty"`
	ApplicationID     string    `json:"applicationId"`
	ReadmeURL         string    `json:"readmeUrl,omitempty"`
	SpdxLicenseID     string    `json:"spdxLicenseId,omitempty"`
	SourceCodeURL     string    `json:"sourceCodeUrl,omitempty"`
	SemanticVersion   string    `json:"semanticVersion,omitempty"`
	VerifiedAuthorURL string    `json:"verifiedAuthorUrl,omitempty"`
	Labels            []string  `json:"labels,omitempty"`
	IsVerifiedAuthor  bool      `json:"isVerifiedAuthor"`
}

// ApplicationVersion represents a version of a Serverless Application Repository application.
type ApplicationVersion struct {
	CreationTime         time.Time `json:"creationTime"`
	ApplicationID        string    `json:"applicationId"`
	SemanticVersion      string    `json:"semanticVersion"`
	SourceCodeURL        string    `json:"sourceCodeUrl,omitempty"`
	SourceCodeArchiveURL string    `json:"sourceCodeArchiveUrl,omitempty"`
	TemplateURL          string    `json:"templateUrl,omitempty"`
	// AppName identifies the owning application. It exists purely so the
	// flattened store.Table[ApplicationVersion] (see store_setup.go; this
	// collection was previously nested appName -> semanticVersion -> *version)
	// can derive its composite "appName#semanticVersion" key from the value
	// alone -- SemanticVersion is unique only within an application, not
	// globally. It is not part of the Serverless Application Repository wire
	// API, hence json:"-".
	AppName              string                `json:"-"`
	ParameterDefinitions []ParameterDefinition `json:"parameterDefinitions"`
	RequiredCapabilities []string              `json:"requiredCapabilities"`
	ResourcesSupported   bool                  `json:"resourcesSupported"`
}

// CloudFormationTemplate represents a CloudFormation template for an application.
type CloudFormationTemplate struct {
	CreationTime    time.Time `json:"creationTime"`
	ExpirationTime  time.Time `json:"expirationTime"`
	ApplicationID   string    `json:"applicationId"`
	TemplateID      string    `json:"templateId"`
	SemanticVersion string    `json:"semanticVersion,omitempty"`
	Status          string    `json:"status"`
	TemplateURL     string    `json:"templateUrl,omitempty"`
	// AppName identifies the owning application. TemplateID is already
	// globally unique (it is generated as "<appName>-<unixNano>"), so it
	// remains the store.Table[CloudFormationTemplate] primary key (see
	// store_setup.go); AppName exists purely to drive the additive "byApp"
	// secondary index used for DeleteApplication's cascade delete. It is not
	// part of the Serverless Application Repository wire API, hence json:"-".
	AppName string `json:"-"`
}

// CloudFormationChangeSet represents a CloudFormation change set for an application.
type CloudFormationChangeSet struct {
	ApplicationID   string `json:"applicationId"`
	ChangeSetID     string `json:"changeSetId"`
	SemanticVersion string `json:"semanticVersion,omitempty"`
	StackID         string `json:"stackId"`
	// AppName identifies the owning application. It exists purely so the
	// flattened store.Table[CloudFormationChangeSet] (see store_setup.go;
	// this collection was previously nested appName -> changeSetID -> *cs)
	// can derive its composite "appName#changeSetID" key from the value
	// alone -- ChangeSetID is derived from a caller-supplied changeSetName/
	// stackName and is unique only within an application, not globally. It is
	// not part of the Serverless Application Repository wire API, hence
	// json:"-".
	AppName      string   `json:"-"`
	Capabilities []string `json:"capabilities,omitempty"`
	Tags         []Tag    `json:"tags,omitempty"`
}

// ApplicationPolicyStatement represents a policy statement for an application.
type ApplicationPolicyStatement struct {
	StatementID     string   `json:"statementId,omitempty"`
	Actions         []string `json:"actions"`
	Principals      []string `json:"principals"`
	PrincipalOrgIDs []string `json:"principalOrgIDs,omitempty"`
}

// ApplicationDependency represents a nested application dependency.
type ApplicationDependency struct {
	ApplicationID   string `json:"applicationId"`
	SemanticVersion string `json:"semanticVersion"`
}

// CreateApplicationVersionOptions contains optional AWS SAR version inputs.
type CreateApplicationVersionOptions struct {
	SourceCodeURL        string
	SourceCodeArchiveURL string
	TemplateURL          string
}

// CreateCloudFormationChangeSetOptions contains optional deployment metadata.
type CreateCloudFormationChangeSetOptions struct {
	// TemplateID, if non-empty, is the UUID returned by a prior CreateCloudFormationTemplate
	// call for the same application. When set, it is cross-validated against that application's
	// recorded templates.
	TemplateID   string
	Capabilities []string
	Tags         []Tag
}

// cloneStringSlice returns a copy of a string slice, returning nil for nil input.
func cloneStringSlice(s []string) []string {
	if s == nil {
		return nil
	}

	result := make([]string, len(s))
	copy(result, s)

	return result
}

// nonNilStringSlice returns an empty slice for nil input.
func nonNilStringSlice(s []string) []string {
	if s != nil {
		return s
	}

	return []string{}
}

// cloneParameterDefinitions returns a deep copy of a ParameterDefinition slice,
// returning an empty (non-nil) slice for nil input.
func cloneParameterDefinitions(defs []ParameterDefinition) []ParameterDefinition {
	if defs == nil {
		return []ParameterDefinition{}
	}

	out := make([]ParameterDefinition, len(defs))
	for i, d := range defs {
		out[i] = d
		out[i].AllowedValues = cloneStringSlice(d.AllowedValues)
		out[i].ReferencedByResources = cloneStringSlice(d.ReferencedByResources)
	}

	return out
}

// synthesizeTemplateURL returns a deterministic S3-style template URL used whenever a
// version's packaged template is derived rather than caller-supplied verbatim -- e.g. only a
// sourceCodeUrl/sourceCodeArchiveUrl was given, or a raw templateBody was given instead of a
// templateUrl. Real AWS SAR uploads the processed/uploaded template to an S3 object and
// returns its URL in templateUrl; gopherstack emulates that with a deterministic path scoped
// by application name and semantic version.
func synthesizeTemplateURL(appName, semanticVersion string) string {
	return fmt.Sprintf("https://s3.amazonaws.com/serverlessrepo-templates/%s/%s.template", appName, semanticVersion)
}

// synthesizeLicenseURL returns a deterministic S3-style URL used when a caller supplies raw
// license content (licenseBody) instead of a licenseUrl, mirroring the emulation convention
// used by synthesizeTemplateURL.
func synthesizeLicenseURL(appName string) string {
	return fmt.Sprintf("https://s3.amazonaws.com/serverlessrepo-licenses/%s/LICENSE.txt", appName)
}

// synthesizeReadmeURL returns a deterministic S3-style URL used when a caller supplies raw
// readme content (readmeBody) instead of a readmeUrl, mirroring the emulation convention used
// by synthesizeTemplateURL.
func synthesizeReadmeURL(appName string) string {
	return fmt.Sprintf("https://s3.amazonaws.com/serverlessrepo-readmes/%s/README.md", appName)
}

// isValidSemanticVersion returns true if v looks like a semver string (major.minor.patch prefix)
// and does not exceed the AWS SAR maximum length.
func isValidSemanticVersion(v string) bool {
	return len(v) <= maxSemanticVersionLength && validSemanticVersionRe.MatchString(v)
}

// validateLabels checks that the label slice satisfies AWS SAR constraints.
func validateLabels(labels []string) error {
	if len(labels) > maxLabelCount {
		return fmt.Errorf("%w: at most %d labels are allowed", ErrValidation, maxLabelCount)
	}

	for i, l := range labels {
		if l == "" {
			return fmt.Errorf("%w: label %d must not be empty", ErrValidation, i)
		}

		if len(l) > maxLabelLength {
			return fmt.Errorf(
				"%w: label %d must be at most %d characters", ErrValidation, i, maxLabelLength,
			)
		}
	}

	return nil
}
