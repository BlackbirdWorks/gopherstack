package elasticbeanstalk

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// configTemplateKey returns the map key for a configuration template.
func configTemplateKey(appName, templateName string) string {
	return appName + "\x00" + templateName
}

// --- ConfigurationTemplate store.Table/Index helpers. Callers must hold b.mu. ---

func (b *InMemoryBackend) configTemplateGet(region, appName, templateName string) (*ConfigurationTemplate, bool) {
	return b.configTemplates.Get(regionKey(region, configTemplateKey(appName, templateName)))
}

func (b *InMemoryBackend) configTemplatePut(v *ConfigurationTemplate) { b.configTemplates.Put(v) }

func (b *InMemoryBackend) configTemplateDelete(region, appName, templateName string) {
	b.configTemplates.Delete(regionKey(region, configTemplateKey(appName, templateName)))
}

func (b *InMemoryBackend) configTemplatesInRegion(region string) []*ConfigurationTemplate {
	return b.configTemplatesByRegion.Get(region)
}

// configTemplateByARN finds a configuration template whose derived ARN
// matches resourceARN. Unlike Application/Environment/ApplicationVersion,
// ConfigurationTemplate carries no ARN field or ARN index (real AWS doesn't
// return one in Create/DescribeConfigurationTemplate responses either -- see
// https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/AWSHowTo.iam.policies.arn.html),
// so the ARN is reconstructed on demand from the documented
// "configurationtemplate/{application-name}/{template-name}" resource path.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) configTemplateByARN(region, resourceARN string) (*ConfigurationTemplate, bool) {
	for _, tmpl := range b.configTemplatesInRegion(region) {
		candidate := arn.Build("elasticbeanstalk", region, b.accountID,
			"configurationtemplate/"+tmpl.ApplicationName+"/"+tmpl.TemplateName)
		if candidate == resourceARN {
			return tmpl, true
		}
	}

	return nil, false
}

// --- ConfigurationTemplate operations ---

// CreateConfigurationTemplate creates a new configuration template for an application.
func (b *InMemoryBackend) CreateConfigurationTemplate(
	ctx context.Context,
	appName, templateName, description, solutionStack string,
	tags map[string]string,
) (*ConfigurationTemplate, error) {
	b.mu.Lock("CreateConfigurationTemplate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.configTemplateGet(region, appName, templateName); ok {
		return nil, fmt.Errorf(
			"%w: configuration template %s already exists",
			ErrAlreadyExists,
			templateName,
		)
	}

	tmpl := &ConfigurationTemplate{
		ApplicationName:   appName,
		TemplateName:      templateName,
		Description:       description,
		DateCreated:       nowISO8601(),
		DateUpdated:       nowISO8601(),
		SolutionStackName: solutionStack,
		Tags:              copyTags(tags),
		region:            region,
	}
	b.configTemplatePut(tmpl)

	return cloneConfigurationTemplate(tmpl), nil
}

// DescribeConfigurationTemplates returns all configuration templates for an application (improvement #17).
func (b *InMemoryBackend) DescribeConfigurationTemplates(ctx context.Context, appName string) []*ConfigurationTemplate {
	b.mu.RLock("DescribeConfigurationTemplates")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	tmpls := b.configTemplatesInRegion(region)
	list := make([]*ConfigurationTemplate, 0, len(tmpls))

	for _, tmpl := range tmpls {
		if appName == "" || tmpl.ApplicationName == appName {
			list = append(list, cloneConfigurationTemplate(tmpl))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].TemplateName < list[j].TemplateName
	})

	return list
}

// DeleteConfigurationTemplate removes a configuration template.
func (b *InMemoryBackend) DeleteConfigurationTemplate(ctx context.Context, appName, templateName string) error {
	b.mu.Lock("DeleteConfigurationTemplate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.configTemplateGet(region, appName, templateName); !ok {
		return fmt.Errorf("%w: configuration template %s not found", ErrNotFound, templateName)
	}

	b.configTemplateDelete(region, appName, templateName)

	return nil
}

// DeleteEnvironmentConfiguration deletes the draft configuration associated with an environment.
// This is a no-op in the in-memory backend.
func (b *InMemoryBackend) DeleteEnvironmentConfiguration(_ context.Context, _, _ string) error {
	return nil
}

// UpdateConfigurationTemplate updates a configuration template's description.
func (b *InMemoryBackend) UpdateConfigurationTemplate(
	ctx context.Context,
	appName, templateName, description string,
) (*ConfigurationTemplate, error) {
	b.mu.Lock("UpdateConfigurationTemplate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	tmpl, ok := b.configTemplateGet(region, appName, templateName)
	if !ok {
		return nil, fmt.Errorf("%w: configuration template %s not found", ErrNotFound, templateName)
	}

	tmpl.Description = description
	tmpl.DateUpdated = nowISO8601()

	return cloneConfigurationTemplate(tmpl), nil
}

// addConfigTemplateInternal seeds a configuration template directly into the backend.
// Caller must hold the write lock.
func (b *InMemoryBackend) addConfigTemplateInternal(region string, tmpl *ConfigurationTemplate) {
	cp := cloneConfigurationTemplate(tmpl)
	cp.region = region
	b.configTemplatePut(cp)
}
