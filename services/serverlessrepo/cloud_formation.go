package serverlessrepo

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	// templateStatusActive is the status of an active CloudFormation template.
	templateStatusActive = "ACTIVE"
	// templateStatusPreparing is the status of a template still being built.
	templateStatusPreparing = "PREPARING"
	// templateExpirationHours is the number of hours before a template expires.
	templateExpirationHours = 1
)

// cloneTemplate returns a deep copy of t.
func cloneTemplate(t *CloudFormationTemplate) *CloudFormationTemplate {
	cp := *t

	return &cp
}

func cloneChangeSet(cs *CloudFormationChangeSet) *CloudFormationChangeSet {
	cp := *cs
	cp.Capabilities = cloneStringSlice(cs.Capabilities)
	cp.Tags = cloneTags(cs.Tags)

	return &cp
}

// CreateCloudFormationTemplate creates a new CloudFormation template for an application.
func (b *InMemoryBackend) CreateCloudFormationTemplate(
	appName, semanticVersion string,
) (*CloudFormationTemplate, error) {
	b.mu.Lock("CreateCloudFormationTemplate")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(appName)
	if !ok {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	resolvedVersion, versionErr := b.resolveVersionLocked(appName, semanticVersion, ErrTemplateNotFound)
	if versionErr != nil {
		return nil, versionErr
	}

	now := time.Now()
	// TemplateId is a bare UUID (confirmed pattern on CreateCloudFormationTemplateOutput,
	// aws-sdk-go-v2/service/serverlessapplicationrepository), not appName-prefixed.
	templateID := uuid.NewString()
	t := &CloudFormationTemplate{
		ApplicationID:   app.ApplicationID,
		TemplateID:      templateID,
		AppName:         appName,
		SemanticVersion: resolvedVersion,
		Status:          templateStatusActive,
		CreationTime:    now,
		ExpirationTime:  now.Add(templateExpirationHours * time.Hour),
		TemplateURL: fmt.Sprintf(
			"https://s3.amazonaws.com/serverlessrepo-templates/%s/%s.template",
			appName,
			templateID,
		),
	}
	b.cfTemplates.Put(t)

	return b.templateView(t), nil
}

// SetTemplatePrepareDelay keeps new CloudFormation templates in PREPARING for d
// (default 0: immediately ACTIVE).
func (b *InMemoryBackend) SetTemplatePrepareDelay(d time.Duration) {
	b.mu.Lock("SetTemplatePrepareDelay")
	defer b.mu.Unlock()

	b.templatePrepareDelay = d
}

func (b *InMemoryBackend) templateView(t *CloudFormationTemplate) *CloudFormationTemplate {
	cp := cloneTemplate(t)
	if cp.Status == templateStatusActive && b.templatePrepareDelay > 0 &&
		time.Since(cp.CreationTime) < b.templatePrepareDelay {
		cp.Status = templateStatusPreparing
	}

	return cp
}

// resolveVersionLocked checks that a requested semantic version exists for the application.
func (b *InMemoryBackend) resolveVersionLocked(
	appName, semanticVersion string,
	notFound error,
) (string, error) {
	if semanticVersion == "" {
		return "", nil
	}

	if !isValidSemanticVersion(semanticVersion) {
		return "", fmt.Errorf("%w: semanticVersion must be a valid semantic version (e.g. 1.0.0)", ErrValidation)
	}

	if !b.appVersions.Has(versionKey(appName, semanticVersion)) {
		return "", fmt.Errorf(
			"%w: could not find version %q for application %q", notFound, semanticVersion, appName)
	}

	return semanticVersion, nil
}

// GetCloudFormationTemplate returns a CloudFormation template by application name and template ID.
func (b *InMemoryBackend) GetCloudFormationTemplate(appName, templateID string) (*CloudFormationTemplate, error) {
	b.mu.RLock("GetCloudFormationTemplate")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	t, ok := b.cfTemplates.Get(templateID)
	if !ok || t.AppName != appName {
		return nil, fmt.Errorf(
			"%w: could not find template %q for application %q",
			ErrTemplateNotFound,
			templateID,
			appName,
		)
	}

	return b.templateView(t), nil
}

// CreateCloudFormationChangeSet creates a new CloudFormation change set for an application.
func (b *InMemoryBackend) CreateCloudFormationChangeSet(
	appName string,
	stackName string,
	changeSetName string,
	semanticVersion string,
) (*CloudFormationChangeSet, error) {
	return b.CreateCloudFormationChangeSetWithOptions(
		appName,
		stackName,
		changeSetName,
		semanticVersion,
		CreateCloudFormationChangeSetOptions{},
	)
}

// CreateCloudFormationChangeSetWithOptions creates a deployment change set with request metadata.
func (b *InMemoryBackend) CreateCloudFormationChangeSetWithOptions(
	appName string,
	stackName string,
	changeSetName string,
	semanticVersion string,
	opts CreateCloudFormationChangeSetOptions,
) (*CloudFormationChangeSet, error) {
	b.mu.Lock("CreateCloudFormationChangeSet")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(appName)
	if !ok {
		// CreateCloudFormationChangeSet's modelled error set has no NotFoundException
		// (deserializers.go awsRestjson1_deserializeOpErrorCreateCloudFormationChangeSet):
		// an unknown ApplicationId is a BadRequestException, not a 404.
		return nil, fmt.Errorf("%w: could not find application %q", ErrValidation, appName)
	}

	if nameErr := validateStackNames(stackName, changeSetName); nameErr != nil {
		return nil, nameErr
	}

	resolvedVersion, versionErr := b.resolveVersionLocked(appName, semanticVersion, ErrValidation)
	if versionErr != nil {
		return nil, versionErr
	}

	for _, capability := range opts.Capabilities {
		if !isValidCapability(capability) {
			return nil, fmt.Errorf("%w: unsupported capability %q", ErrValidation, capability)
		}
	}

	// TemplateId, when supplied, must reference a template previously created for this
	// application via CreateCloudFormationTemplate, rather than being silently ignored.
	// CreateCloudFormationChangeSet models no NotFoundException, so an unknown or
	// mismatched-application TemplateId is a BadRequestException, not a 404.
	if opts.TemplateID != "" {
		t, foundTemplate := b.cfTemplates.Get(opts.TemplateID)
		if !foundTemplate || t.AppName != appName {
			return nil, fmt.Errorf(
				"%w: could not find template %q for application %q",
				ErrValidation,
				opts.TemplateID,
				appName,
			)
		}
	}

	// suffix mirrors real CloudFormation's UUID stack-ID suffix
	// (arn:...:stack/{name}/{uuid}).
	suffix := uuid.NewString()

	csName := changeSetName
	if csName == "" {
		csName = stackName + "-" + suffix
	}

	changeSetID := arn.Build(
		"cloudformation",
		b.region,
		b.accountID,
		"changeSet/"+csName,
	)
	stackID := arn.Build(
		"cloudformation",
		b.region,
		b.accountID,
		"stack/"+stackName+"/"+suffix,
	)

	cs := &CloudFormationChangeSet{
		ApplicationID:   app.ApplicationID,
		ChangeSetID:     changeSetID,
		AppName:         appName,
		SemanticVersion: resolvedVersion,
		StackID:         stackID,
		Capabilities:    cloneStringSlice(opts.Capabilities),
		Tags:            cloneTags(opts.Tags),
	}
	b.cfChangeSets.Put(cloneChangeSet(cs))

	return cloneChangeSet(cs), nil
}

var stackNameRe = regexp.MustCompile(`^[a-zA-Z][-a-zA-Z0-9]*$`)

const stackNameMaxLen = 128

func validateStackNames(stackName, changeSetName string) error {
	validStack := stackNameRe.MatchString(stackName) || strings.HasPrefix(stackName, "arn:")
	if len(stackName) > stackNameMaxLen || !validStack {
		return fmt.Errorf("%w: stackName must match [a-zA-Z][-a-zA-Z0-9]* and be at most %d characters",
			ErrValidation, stackNameMaxLen)
	}

	if changeSetName != "" && (len(changeSetName) > stackNameMaxLen || !stackNameRe.MatchString(changeSetName)) {
		return fmt.Errorf("%w: changeSetName must match [a-zA-Z][-a-zA-Z0-9]* and be at most %d characters",
			ErrValidation, stackNameMaxLen)
	}

	return nil
}

func isValidCapability(capability string) bool {
	switch capability {
	case "CAPABILITY_IAM",
		"CAPABILITY_NAMED_IAM",
		"CAPABILITY_AUTO_EXPAND",
		"CAPABILITY_RESOURCE_POLICY":
		return true
	default:
		return false
	}
}
