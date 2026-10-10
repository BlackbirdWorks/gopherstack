package elasticbeanstalk

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// --- Environment store.Table/Index helpers. Callers must hold b.mu. ---

func (b *InMemoryBackend) environmentGet(region, appName, envName string) (*Environment, bool) {
	return b.environments.Get(regionKey(region, envKey(appName, envName)))
}

func (b *InMemoryBackend) environmentPut(v *Environment) { b.environments.Put(v) }

func (b *InMemoryBackend) environmentDeleteKey(region, appName, envName string) {
	b.environments.Delete(regionKey(region, envKey(appName, envName)))
}

func (b *InMemoryBackend) environmentsInRegion(region string) []*Environment {
	return b.environmentsByRegion.Get(region)
}

func (b *InMemoryBackend) environmentByARN(region, resourceARN string) (*Environment, bool) {
	list := b.environmentsByARN.Get(regionKey(region, resourceARN))
	if len(list) == 0 {
		return nil, false
	}

	return list[0], true
}

func (b *InMemoryBackend) environmentByName(region, envName string) (*Environment, bool) {
	list := b.environmentsByName.Get(regionKey(region, envName))
	if len(list) == 0 {
		return nil, false
	}

	return list[0], true
}

func (b *InMemoryBackend) environmentCNAMETaken(region, cname string) bool {
	return len(b.environmentsByCNAME.Get(regionKey(region, cname))) > 0
}

// environmentByID finds an environment by EnvironmentId. No dedicated index
// exists for this lookup (only by name/ARN/CNAME), so it scans the region's
// environments -- same precedent as configTemplateByARN's linear scan.
func (b *InMemoryBackend) environmentByID(region, envID string) (*Environment, bool) {
	for _, env := range b.environmentsInRegion(region) {
		if env.EnvironmentID == envID {
			return env, true
		}
	}

	return nil, false
}

func (b *InMemoryBackend) nextEnvID(region string) string {
	b.envCounters[region]++

	return fmt.Sprintf("e-%08d", b.envCounters[region])
}

// envKey returns the map key for an environment.
func envKey(appName, envName string) string {
	return appName + "\x00" + envName
}

// --- Environment operations ---

// CreateEnvironmentParams holds optional parameters for CreateEnvironment (improvements #1, #5, #14, #15, #16).
type CreateEnvironmentParams struct {
	TierType         string
	TierName         string
	TierVersion      string
	CNAMEPrefix      string
	PlatformARN      string
	TemplateName     string
	VersionLabel     string
	OperationsRole   string
	LoadBalancerType string
	VPCID            string
	Subnets          string
	InstanceProfile  string
	CustomAMI        string
	OptionSettings   []OptionSetting
	OptionsToRemove  []OptionSetting

	EnvironmentLinks []EnvironmentLink
}

// UpdateEnvironmentParams holds state changes accepted by UpdateEnvironment.
type UpdateEnvironmentParams struct {
	SolutionStackName string
	PlatformARN       string
	TemplateName      string
	VersionLabel      string
	Description       string
	TierType          string
	TierName          string
	TierVersion       string
	OptionSettings    []OptionSetting
	OptionsToRemove   []OptionSetting
	EnvironmentLinks  []EnvironmentLink
}

// ValidateInstanceProfileARN validates that an instance profile ARN has the correct format (improvement #16).
func ValidateInstanceProfileARN(instanceProfile string) error {
	if instanceProfile == "" {
		return nil
	}

	if !strings.HasPrefix(instanceProfile, arnPrefixIAM) {
		return fmt.Errorf(
			"%w: InstanceProfile must be a valid IAM ARN starting with %s",
			ErrInvalidParameter,
			arnPrefixIAM,
		)
	}

	return nil
}

// CreateEnvironment creates a new Elastic Beanstalk environment.
func (b *InMemoryBackend) CreateEnvironment(
	ctx context.Context,
	appName, envName, solutionStack, description string,
	tags map[string]string,
	params CreateEnvironmentParams,
) (*Environment, error) {
	b.mu.Lock("CreateEnvironment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if err := validateEnvironmentNames(envName, params.CNAMEPrefix); err != nil {
		return nil, err
	}

	if _, ok := b.applicationGet(region, appName); !ok {
		return nil, applicationNotFound(appName)
	}

	b.reapLocked(region)

	if _, ok := b.environmentGet(region, appName, envName); ok {
		return nil, wrapf(ErrAlreadyExists, "Environment %s already exists.", envName)
	}

	optionSettings := slices.Clone(params.OptionSettings)

	if params.TemplateName != "" {
		tmpl, ok := b.configTemplateGet(region, appName, params.TemplateName)
		if !ok {
			return nil, wrapf(ErrNotFound, "No Configuration Template named '%s' found.", params.TemplateName)
		}

		if solutionStack == "" && params.PlatformARN == "" {
			solutionStack = tmpl.SolutionStackName
			params.PlatformARN = tmpl.PlatformArn
		}

		optionSettings = updateOptionSettings(tmpl.OptionSettings, params.OptionSettings, params.OptionsToRemove)
	}

	cnamePrefix := cmp.Or(params.CNAMEPrefix, envName)
	cname := cnamePrefix + "." + region + ".elasticbeanstalk.com"

	if b.environmentCNAMETaken(region, cname) {
		return nil, wrapf(ErrInvalidParameter, "DNS name (%s) is not available.", cname)
	}

	envID := b.nextEnvID(region)
	envARN := arn.Build("elasticbeanstalk", region, b.accountID, "environment/"+appName+"/"+envName)
	tierName := cmp.Or(params.TierName, defaultEnvironmentTierName)
	tierType := cmp.Or(params.TierType, defaultEnvironmentTierType)

	env := &Environment{
		OptionSettings:    optionSettings,
		ApplicationName:   appName,
		EnvironmentName:   envName,
		EnvironmentID:     envID,
		EnvironmentARN:    envARN,
		SolutionStackName: solutionStack,
		PlatformARN:       params.PlatformARN,
		TemplateName:      params.TemplateName,
		VersionLabel:      params.VersionLabel,
		Description:       description,
		OperationsRole:    params.OperationsRole,
		Status:            envStatusReady,
		Health:            envHealthGreen,
		Tier:              tierName,
		TierType:          tierType,
		TierName:          tierName,
		TierVersion:       params.TierVersion,
		CNAME:             cname,
		CNAMEPrefix:       cnamePrefix,
		LoadBalancerType:  params.LoadBalancerType,
		VPCID:             params.VPCID,
		Subnets:           params.Subnets,
		InstanceProfile:   params.InstanceProfile,
		CustomAMI:         params.CustomAMI,
		DateCreated:       nowISO8601(),
		DateUpdated:       nowISO8601(),
		Region:            region,
		Tags:              copyTags(tags),
		EnvironmentLinks:  slices.Clone(params.EnvironmentLinks),
	}
	b.beginTransition(env, envStatusLaunching)
	b.environmentPut(env)

	b.appendEvent(ctx, region, env, "createEnvironment is starting.", eventSeverityInfo)
	b.appendEventAt(
		ctx,
		region,
		env,
		"Successfully launched environment: "+envName+".",
		eventSeverityInfo,
		b.visibleAt(),
	)

	return b.observe(env), nil
}

// DescribeEnvironments returns environments, optionally filtered by app/environment names or IDs.
// Results are sorted by EnvironmentName for deterministic output.
func (b *InMemoryBackend) DescribeEnvironments(
	ctx context.Context,
	appName string,
	envNames []string,
	envIDs []string,
) []*Environment {
	b.mu.RLock("DescribeEnvironments")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	envs := b.environmentsInRegion(region)

	list := make([]*Environment, 0, len(envs))

	for _, env := range envs {
		if envMatches(env, appName, envNames, envIDs) && !b.terminated(env) {
			list = append(list, b.observe(env))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].EnvironmentName < list[j].EnvironmentName
	})

	return list
}

func envMatches(env *Environment, appName string, envNames, envIDs []string) bool {
	if appName != "" && env.ApplicationName != appName {
		return false
	}

	if len(envNames) > 0 && !slices.Contains(envNames, env.EnvironmentName) {
		return false
	}

	return len(envIDs) == 0 || slices.Contains(envIDs, env.EnvironmentID)
}

// DescribeDeletedEnvironments returns terminated environments deleted after since (zero means all).
func (b *InMemoryBackend) DescribeDeletedEnvironments(
	ctx context.Context,
	appName string,
	envNames, envIDs []string,
	since time.Time,
) []*Environment {
	b.mu.RLock("DescribeDeletedEnvironments")
	defer b.mu.RUnlock()

	var list []*Environment

	region := getRegion(ctx, b.region)
	candidates := slices.Clone(b.deletedEnvironments[region])

	for _, env := range b.environmentsInRegion(region) {
		if b.terminated(env) {
			candidates = append(candidates, b.observe(env))
		}
	}

	for _, env := range candidates {
		if !envMatches(env, appName, envNames, envIDs) {
			continue
		}

		if t, err := time.Parse(time.RFC3339, env.DateUpdated); err == nil && !t.After(since) {
			continue
		}

		list = append(list, cloneEnvironment(env))
	}

	return list
}

// UpdateEnvironment updates an environment's description or solution stack.
func (b *InMemoryBackend) UpdateEnvironment(
	ctx context.Context,
	appName, envName, description, solutionStack string,
) (*Environment, error) {
	return b.UpdateEnvironmentWithParams(ctx, appName, envName, UpdateEnvironmentParams{
		Description:       description,
		SolutionStackName: solutionStack,
	})
}

// UpdateEnvironmentWithParams applies all mutable environment properties.
func (b *InMemoryBackend) UpdateEnvironmentWithParams(
	ctx context.Context,
	appName, envName string,
	params UpdateEnvironmentParams,
) (*Environment, error) {
	b.mu.Lock("UpdateEnvironment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	b.reapLocked(region)

	env, ok := b.environmentGet(region, appName, envName)
	if !ok {
		return nil, environmentNotFound(envName)
	}

	if b.transitioning(env) {
		return nil, wrapf(
			ErrInvalidParameter,
			"Environment named %s is in an invalid state for this operation. Must be Ready.",
			envName,
		)
	}

	if params.Description != "" {
		env.Description = params.Description
	}

	if params.SolutionStackName != "" {
		env.SolutionStackName = params.SolutionStackName
		env.PlatformARN = ""
		env.TemplateName = ""
	}

	if params.PlatformARN != "" {
		env.PlatformARN = params.PlatformARN
		env.SolutionStackName = ""
		env.TemplateName = ""
	}

	if params.TemplateName != "" {
		env.TemplateName = params.TemplateName
		env.SolutionStackName = ""
		env.PlatformARN = ""
	}

	if params.VersionLabel != "" {
		env.VersionLabel = params.VersionLabel
	}

	if params.TierName != "" {
		env.Tier = params.TierName
		env.TierName = params.TierName
	}

	if params.TierType != "" {
		env.TierType = params.TierType
	}

	if params.TierVersion != "" {
		env.TierVersion = params.TierVersion
	}

	env.OptionSettings = updateOptionSettings(
		env.OptionSettings,
		params.OptionSettings,
		params.OptionsToRemove,
	)

	if params.EnvironmentLinks != nil {
		env.EnvironmentLinks = slices.Clone(params.EnvironmentLinks)
	}

	env.DateUpdated = nowISO8601()

	b.beginTransition(env, envStatusUpdating)
	b.appendEvent(ctx, region, env, "Environment update is starting.", eventSeverityInfo)
	b.appendEventAt(ctx, region, env, "Environment update completed successfully.", eventSeverityInfo, b.visibleAt())

	return b.observe(env), nil
}

// updateOptionSettings applies updates and removals while preserving deterministic output ordering.
func updateOptionSettings(current, updates, removals []OptionSetting) []OptionSetting {
	byKey := make(map[string]OptionSetting, len(current)+len(updates))
	for _, setting := range current {
		byKey[optionSettingKey(setting)] = setting
	}
	for _, setting := range updates {
		byKey[optionSettingKey(setting)] = setting
	}
	for _, setting := range removals {
		delete(byKey, optionSettingKey(setting))
	}

	result := make([]OptionSetting, 0, len(byKey))
	for _, setting := range byKey {
		result = append(result, setting)
	}
	sort.Slice(result, func(i, j int) bool {
		return optionSettingKey(result[i]) < optionSettingKey(result[j])
	})

	return result
}

func optionSettingKey(setting OptionSetting) string {
	return setting.Namespace + "\x00" + setting.OptionName + "\x00" + setting.ResourceName
}

// TerminateEnvironment marks an environment as Terminated and removes it from storage.
func (b *InMemoryBackend) TerminateEnvironment(ctx context.Context, appName, envName string) (*Environment, error) {
	b.mu.Lock("TerminateEnvironment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	b.reapLocked(region)

	env, ok := b.environmentGet(region, appName, envName)
	if !ok {
		return nil, environmentNotFound(envName)
	}

	return b.terminateEnvironmentLocked(ctx, region, env), nil
}

// terminateEnvironmentLocked marks env as Terminated and removes it from
// storage. Caller must hold b.mu.
func (b *InMemoryBackend) terminateEnvironmentLocked(
	ctx context.Context,
	region string,
	env *Environment,
) *Environment {
	env.DateUpdated = nowISO8601()
	b.appendEvent(ctx, region, env, "terminateEnvironment is starting.", eventSeverityInfo)
	b.appendEventAt(ctx, region, env, "terminateEnvironment completed successfully.", eventSeverityInfo, b.visibleAt())

	b.beginTransition(env, envStatusTerminating)

	if b.lifecycleDelay > 0 {
		return b.observe(env)
	}

	env.transitionStatus = envStatusTerminating
	env.transitionUntil = b.now()
	out := b.observe(env)
	b.reapLocked(region)

	return out
}

// AbortEnvironmentUpdate aborts an in-progress environment configuration update.
// This is a no-op in the in-memory backend since updates complete instantly.
func (b *InMemoryBackend) AbortEnvironmentUpdate(_ context.Context, _ string) error {
	return nil
}

// AssociateEnvironmentOperationsRole associates an operations IAM role with an environment.
func (b *InMemoryBackend) AssociateEnvironmentOperationsRole(
	ctx context.Context,
	envName, role string,
) error {
	b.mu.Lock("AssociateEnvironmentOperationsRole")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	env, ok := b.environmentByName(region, envName)
	if !ok {
		return environmentNotFound(envName)
	}

	env.OperationsRole = role

	return nil
}

// CheckDNSAvailability checks whether the specified CNAME prefix is available.
// Returns available=true when no existing environment in the request region uses that prefix.
func (b *InMemoryBackend) CheckDNSAvailability(ctx context.Context, cnamePrefix string) (bool, string) {
	b.mu.RLock("CheckDNSAvailability")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	fqcname := cnamePrefix + "." + region + ".elasticbeanstalk.com"

	if b.environmentCNAMETaken(region, fqcname) {
		return false, fqcname
	}

	if _, ok := b.environmentByName(region, cnamePrefix); ok {
		return false, fqcname
	}

	return true, fqcname
}

// DescribeEnvironmentHealth returns the health and status of an environment by name.
func (b *InMemoryBackend) DescribeEnvironmentHealth(ctx context.Context, envName string) (string, string, error) {
	b.mu.RLock("DescribeEnvironmentHealth")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	env, ok := b.environmentByName(region, envName)
	if !ok {
		return "", "", environmentNotFound(envName)
	}

	cp := b.observe(env)

	return cp.Health, cp.Status, nil
}

// DisassociateEnvironmentOperationsRole removes the operations role from an environment.
func (b *InMemoryBackend) DisassociateEnvironmentOperationsRole(ctx context.Context, envName string) error {
	b.mu.Lock("DisassociateEnvironmentOperationsRole")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	env, ok := b.environmentByName(region, envName)
	if !ok {
		return environmentNotFound(envName)
	}

	env.OperationsRole = ""

	return nil
}

// SwapEnvironmentCNAMEs swaps the CNAME values between two environments (improvement #10).
func (b *InMemoryBackend) SwapEnvironmentCNAMEs(ctx context.Context, sourceEnvName, destEnvName string) error {
	b.mu.Lock("SwapEnvironmentCNAMEs")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	var srcEnv, dstEnv *Environment

	for _, env := range b.environmentsInRegion(region) {
		switch env.EnvironmentName {
		case sourceEnvName:
			srcEnv = env
		case destEnvName:
			dstEnv = env
		}
	}

	if srcEnv == nil {
		return environmentNotFound(sourceEnvName)
	}

	if dstEnv == nil {
		return environmentNotFound(destEnvName)
	}

	// CNAME is an indexed field (environmentsByCNAME); mutating it in place
	// would leave a stale index entry (see pkgs/store gotcha), so both
	// entries are deleted, mutated, and re-Put to rebuild every index.
	b.environmentDeleteKey(region, srcEnv.ApplicationName, srcEnv.EnvironmentName)
	b.environmentDeleteKey(region, dstEnv.ApplicationName, dstEnv.EnvironmentName)

	srcEnv.CNAME, dstEnv.CNAME = dstEnv.CNAME, srcEnv.CNAME

	b.environmentPut(srcEnv)
	b.environmentPut(dstEnv)

	return nil
}

// addEnvironmentInternal seeds an environment directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addEnvironmentInternal(region string, env *Environment) {
	cp := cloneEnvironment(env)
	cp.Region = region
	b.environmentPut(cp)
}

func environmentNotFound(name string) error {
	return wrapf(ErrNotFound, "No Environment found for EnvironmentName = '%s'.", name)
}

func applicationNotFoundParam(name string) error {
	return wrapf(ErrInvalidParameter, "No Application named '%s' found.", name)
}

func applicationNotFound(name string) error {
	return wrapf(ErrNotFound, "No Application named '%s' found.", name)
}

func validateEnvironmentNames(envName, cnamePrefix string) error {
	if !validEnvLabel(envName, minEnvironmentNameLen, maxEnvironmentNameLen) {
		return wrapf(
			ErrInvalidParameter,
			"Environment name must be between %d and %d characters, contain only letters, numbers and hyphens, "+
				"and not begin or end with a hyphen.",
			minEnvironmentNameLen,
			maxEnvironmentNameLen,
		)
	}

	if cnamePrefix != "" && !validEnvLabel(cnamePrefix, minCNAMEPrefixLen, maxCNAMEPrefixLen) {
		return wrapf(
			ErrInvalidParameter,
			"CNAME prefix must be between %d and %d characters, contain only letters, numbers and hyphens, "+
				"and not begin or end with a hyphen.",
			minCNAMEPrefixLen,
			maxCNAMEPrefixLen,
		)
	}

	return nil
}
