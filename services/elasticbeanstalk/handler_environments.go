package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
)

// --- Environment operations ---

type environmentTierType struct {
	Name    string `xml:"Name"`
	Type    string `xml:"Type"`
	Version string `xml:"Version"`
}

type environmentDescType struct {
	ApplicationName   string              `xml:"ApplicationName"`
	EnvironmentName   string              `xml:"EnvironmentName"`
	EnvironmentID     string              `xml:"EnvironmentId"`
	EnvironmentArn    string              `xml:"EnvironmentArn"`
	Description       string              `xml:"Description,omitempty"`
	SolutionStackName string              `xml:"SolutionStackName"`
	PlatformArn       string              `xml:"PlatformArn,omitempty"`
	VersionLabel      string              `xml:"VersionLabel,omitempty"`
	OperationsRole    string              `xml:"OperationsRole,omitempty"`
	DateCreated       string              `xml:"DateCreated,omitempty"`
	DateUpdated       string              `xml:"DateUpdated,omitempty"`
	Status            string              `xml:"Status"`
	Health            string              `xml:"Health"`
	Tier              environmentTierType `xml:"Tier"`
	CNAME             string              `xml:"CNAME"`
	EndpointURL       string              `xml:"EndpointURL"`
}

func toEnvironmentDesc(env *Environment) environmentDescType {
	cname := env.CNAME
	if cname == "" {
		cname = env.EnvironmentName + "." + env.Region + ".elasticbeanstalk.com"
	}

	tierName := env.TierName
	if tierName == "" {
		tierName = env.Tier
	}

	if tierName == "" {
		tierName = "WebServer"
	}

	tierType := env.TierType
	if tierType == "" {
		tierType = "Standard"
	}

	tierVersion := env.TierVersion
	if tierVersion == "" {
		tierVersion = "1.0"
	}

	return environmentDescType{
		ApplicationName:   env.ApplicationName,
		EnvironmentName:   env.EnvironmentName,
		EnvironmentID:     env.EnvironmentID,
		EnvironmentArn:    env.EnvironmentARN,
		Description:       env.Description,
		SolutionStackName: env.SolutionStackName,
		PlatformArn:       env.PlatformARN,
		VersionLabel:      env.VersionLabel,
		OperationsRole:    env.OperationsRole,
		DateCreated:       env.DateCreated,
		DateUpdated:       env.DateUpdated,
		Status:            env.Status,
		Health:            env.Health,
		Tier: environmentTierType{
			Name:    tierName,
			Type:    tierType,
			Version: tierVersion,
		},
		CNAME:       cname,
		EndpointURL: cname,
	}
}

type createEnvironmentResponse struct {
	XMLName                 xml.Name            `xml:"CreateEnvironmentResponse"`
	Xmlns                   string              `xml:"xmlns,attr"`
	CreateEnvironmentResult environmentDescType `xml:"CreateEnvironmentResult"`
	ResponseMetadata        responseMetadata    `xml:"ResponseMetadata"`
}

func (h *Handler) handleCreateEnvironment(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	envName := vals.Get("EnvironmentName")

	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	solutionStack := vals.Get("SolutionStackName")
	description := vals.Get("Description")
	tags := parseTagList(vals, "Tags.member")
	optionSettings := parseOptionSettings(vals, "OptionSettings.member")

	// Parse tier (improvement #1)
	tierName := vals.Get("Tier.Name")
	tierType := vals.Get("Tier.Type")
	tierVersion := vals.Get("Tier.Version")

	// Parse load balancer type from OptionSettings (improvement #14)
	lbType := parseOptionSetting(vals, nsEBEnvironment, "LoadBalancerType")

	// Parse VPC config from OptionSettings (improvement #15)
	vpcID := parseOptionSetting(vals, nsEC2VPC, "VPCId")
	subnets := parseOptionSetting(vals, nsEC2VPC, "Subnets")

	// Parse instance profile from OptionSettings (improvement #16)
	instanceProfile := parseOptionSetting(vals, nsAutoScalingLaunchConfig, "IamInstanceProfile")
	if err := ValidateInstanceProfileARN(instanceProfile); err != nil {
		return nil, err
	}

	// Parse custom AMI from OptionSettings (improvement #5)
	customAMI := parseOptionSetting(vals, nsAutoScalingLaunchConfig, "ImageId")

	params := CreateEnvironmentParams{
		TierType:         tierType,
		TierName:         tierName,
		TierVersion:      tierVersion,
		CNAMEPrefix:      vals.Get("CNAMEPrefix"),
		PlatformARN:      vals.Get("PlatformArn"),
		TemplateName:     vals.Get("TemplateName"),
		VersionLabel:     vals.Get("VersionLabel"),
		OperationsRole:   vals.Get("OperationsRole"),
		LoadBalancerType: lbType,
		VPCID:            vpcID,
		Subnets:          subnets,
		InstanceProfile:  instanceProfile,
		CustomAMI:        customAMI,
		OptionSettings:   optionSettings,
	}

	env, err := h.Backend.CreateEnvironment(ctx, appName, envName, solutionStack, description, tags, params)
	if err != nil {
		return nil, err
	}

	return &createEnvironmentResponse{
		Xmlns:                   ebXMLNS,
		CreateEnvironmentResult: toEnvironmentDesc(env),
		ResponseMetadata:        responseMetadata{RequestID: "eb-create-env"},
	}, nil
}

type describeEnvironmentsResult struct {
	Environments []environmentDescType `xml:"Environments>member"`
}

type describeEnvironmentsResponse struct {
	XMLName                    xml.Name                   `xml:"DescribeEnvironmentsResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	DescribeEnvironmentsResult describeEnvironmentsResult `xml:"DescribeEnvironmentsResult"`
}

func (h *Handler) handleDescribeEnvironments(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	envNames := parseMembers(vals, "EnvironmentNames.member")
	envIDs := parseMembers(vals, "EnvironmentIds.member")
	envs := h.Backend.DescribeEnvironments(ctx, appName, envNames, envIDs)

	members := make([]environmentDescType, 0, len(envs))

	for _, env := range envs {
		members = append(members, toEnvironmentDesc(env))
	}

	return &describeEnvironmentsResponse{
		Xmlns:                      ebXMLNS,
		DescribeEnvironmentsResult: describeEnvironmentsResult{Environments: members},
		ResponseMetadata:           responseMetadata{RequestID: "eb-describe-envs"},
	}, nil
}

type updateEnvironmentResponse struct {
	XMLName                 xml.Name            `xml:"UpdateEnvironmentResponse"`
	Xmlns                   string              `xml:"xmlns,attr"`
	UpdateEnvironmentResult environmentDescType `xml:"UpdateEnvironmentResult"`
	ResponseMetadata        responseMetadata    `xml:"ResponseMetadata"`
}

func (h *Handler) handleUpdateEnvironment(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	appName := vals.Get("ApplicationName")
	description := vals.Get("Description")

	// If no app name provided, search across all environments for this name.
	if appName == "" {
		envs := h.Backend.DescribeEnvironments(ctx, "", []string{envName}, nil)

		if len(envs) == 1 {
			appName = envs[0].ApplicationName
		} else if len(envs) > 1 {
			return nil, fmt.Errorf(
				"%w: multiple environments named %s; please specify ApplicationName",
				ErrInvalidParameter,
				envName,
			)
		}
		// len(envs) == 0: let the backend return a not-found error below.
	}

	env, err := h.Backend.UpdateEnvironmentWithParams(ctx, appName, envName, UpdateEnvironmentParams{
		Description:       description,
		SolutionStackName: vals.Get("SolutionStackName"),
		PlatformARN:       vals.Get("PlatformArn"),
		TemplateName:      vals.Get("TemplateName"),
		VersionLabel:      vals.Get("VersionLabel"),
		TierName:          vals.Get("Tier.Name"),
		TierType:          vals.Get("Tier.Type"),
		TierVersion:       vals.Get("Tier.Version"),
		OptionSettings:    parseOptionSettings(vals, "OptionSettings.member"),
		OptionsToRemove:   parseOptionSettings(vals, "OptionsToRemove.member"),
	})
	if err != nil {
		return nil, err
	}

	return &updateEnvironmentResponse{
		Xmlns:                   ebXMLNS,
		UpdateEnvironmentResult: toEnvironmentDesc(env),
		ResponseMetadata:        responseMetadata{RequestID: "eb-update-env"},
	}, nil
}

type terminateEnvironmentResponse struct {
	XMLName                    xml.Name            `xml:"TerminateEnvironmentResponse"`
	Xmlns                      string              `xml:"xmlns,attr"`
	TerminateEnvironmentResult environmentDescType `xml:"TerminateEnvironmentResult"`
	ResponseMetadata           responseMetadata    `xml:"ResponseMetadata"`
}

func (h *Handler) handleTerminateEnvironment(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	appName := vals.Get("ApplicationName")

	// If no app name provided, search across all environments for this name.
	if appName == "" {
		envs := h.Backend.DescribeEnvironments(ctx, "", []string{envName}, nil)
		switch len(envs) {
		case 0:
			// No matching environments; let the backend handle the not-found case.
		case 1:
			appName = envs[0].ApplicationName
		default:
			return nil, fmt.Errorf(
				"%w: multiple environments named %s; please specify ApplicationName",
				ErrInvalidParameter,
				envName,
			)
		}
	}

	env, err := h.Backend.TerminateEnvironment(ctx, appName, envName)
	if err != nil {
		return nil, err
	}

	return &terminateEnvironmentResponse{
		Xmlns:                      ebXMLNS,
		TerminateEnvironmentResult: toEnvironmentDesc(env),
		ResponseMetadata:           responseMetadata{RequestID: "eb-terminate-env"},
	}, nil
}

// --- Environment Resources ---

// asgMemberType through triggerMemberType mirror elasticbeanstalk@v1.37.4
// types.AutoScalingGroup/Instance/LaunchConfiguration/LaunchTemplate/
// LoadBalancer/Queue/Trigger (types.go:162,709,796,805,826,1185,1428):
// each is a one- or two-field member of an EnvironmentResourceDescription
// list, not a bare string.

type asgMemberType struct {
	Name string `xml:"Name,omitempty"`
}

type instanceMemberType struct {
	ID string `xml:"Id,omitempty"`
}

type launchConfigMemberType struct {
	Name string `xml:"Name,omitempty"`
}

type launchTemplateMemberType struct {
	ID string `xml:"Id,omitempty"`
}

type loadBalancerMemberType struct {
	Name string `xml:"Name,omitempty"`
}

type queueMemberType struct {
	Name string `xml:"Name,omitempty"`
	URL  string `xml:"URL,omitempty"`
}

type triggerMemberType struct {
	Name string `xml:"Name,omitempty"`
}

// environmentResourceDescType mirrors types.EnvironmentResourceDescription
// (elasticbeanstalk@v1.37.4 types.go:605). Each list uses a two-segment path
// with a struct element so encoding/xml repeats <member> per item; a
// three-segment path onto a []string (e.g. "AutoScalingGroups>member>Name")
// nests every element under ONE shared <member> instead of one per item,
// collapsing multi-item lists to their last value (gopherstack-5pim).
type environmentResourceDescType struct {
	EnvironmentName      string                     `xml:"EnvironmentName"`
	AutoScalingGroups    []asgMemberType            `xml:"AutoScalingGroups>member"`
	Instances            []instanceMemberType       `xml:"Instances>member"`
	LaunchConfigurations []launchConfigMemberType   `xml:"LaunchConfigurations>member"`
	LaunchTemplates      []launchTemplateMemberType `xml:"LaunchTemplates>member"`
	LoadBalancers        []loadBalancerMemberType   `xml:"LoadBalancers>member"`
	Queues               []queueMemberType          `xml:"Queues>member"`
	Triggers             []triggerMemberType        `xml:"Triggers>member"`
}

type describeEnvironmentResourcesResult struct {
	EnvironmentResources environmentResourceDescType `xml:"EnvironmentResources"`
}

type describeEnvironmentResourcesResponse struct {
	XMLName                            xml.Name                           `xml:"DescribeEnvironmentResourcesResponse"`
	ResponseMetadata                   responseMetadata                   `xml:"ResponseMetadata"`
	Xmlns                              string                             `xml:"xmlns,attr"`
	DescribeEnvironmentResourcesResult describeEnvironmentResourcesResult `xml:"DescribeEnvironmentResourcesResult"`
}

func (h *Handler) handleDescribeEnvironmentResources(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	envID := vals.Get("EnvironmentId")
	if envName == "" && envID == "" {
		return nil, fmt.Errorf("%w: EnvironmentName or EnvironmentId is required", ErrInvalidParameter)
	}

	envs := h.Backend.DescribeEnvironments(ctx, "", []string{envName}, []string{envID})
	if envName == "" {
		envs = h.Backend.DescribeEnvironments(ctx, "", nil, []string{envID})
	} else if envID == "" {
		envs = h.Backend.DescribeEnvironments(ctx, "", []string{envName}, nil)
	}
	if len(envs) == 0 {
		return nil, fmt.Errorf("%w: environment not found", ErrNotFound)
	}
	env := envs[0]
	resources := environmentResourceDescType{
		EnvironmentName:      env.EnvironmentName,
		AutoScalingGroups:    []asgMemberType{{Name: env.EnvironmentName + "-asg"}},
		Instances:            []instanceMemberType{{ID: "i-" + strings.TrimPrefix(env.EnvironmentID, "e-")}},
		LaunchConfigurations: []launchConfigMemberType{{Name: env.EnvironmentName + "-lc"}},
	}
	if env.TierName == "Worker" {
		queueURL := "https://sqs." + env.Region + ".amazonaws.com/" + env.EnvironmentName
		resources.Queues = []queueMemberType{{URL: queueURL}}
	} else {
		resources.LoadBalancers = []loadBalancerMemberType{{Name: env.EnvironmentName + "-lb"}}
	}

	return &describeEnvironmentResourcesResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentResourcesResult: describeEnvironmentResourcesResult{
			EnvironmentResources: resources,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-env-resources"},
	}, nil
}

// --- Restart / Rebuild stubs ---

// restartAppServerResponse is the XML response for RestartAppServer.
type restartAppServerResponse struct {
	XMLName          xml.Name         `xml:"RestartAppServerResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleRestartAppServer signals a restart of the application servers for an environment.
// Real AWS triggers an in-place rolling restart; the stub is a no-op that returns 200.
func (h *Handler) handleRestartAppServer(_ context.Context, _ url.Values) (any, error) {
	return &restartAppServerResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-restart-app-server"},
	}, nil
}

// rebuildEnvironmentResponse is the XML response for RebuildEnvironment.
type rebuildEnvironmentResponse struct {
	XMLName          xml.Name         `xml:"RebuildEnvironmentResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleRebuildEnvironment triggers a full environment rebuild.
// Real AWS terminates and relaunches the environment; the stub is a no-op that returns 200.
func (h *Handler) handleRebuildEnvironment(_ context.Context, _ url.Values) (any, error) {
	return &rebuildEnvironmentResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-rebuild-environment"},
	}, nil
}

// abortEnvironmentUpdateResponse is the XML response for AbortEnvironmentUpdate.
type abortEnvironmentUpdateResponse struct {
	XMLName          xml.Name         `xml:"AbortEnvironmentUpdateResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleAbortEnvironmentUpdate aborts an in-progress environment configuration update.
func (h *Handler) handleAbortEnvironmentUpdate(_ context.Context, _ url.Values) (any, error) {
	return &abortEnvironmentUpdateResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-abort-env-update"},
	}, nil
}

// associateEnvironmentOperationsRoleResponse is the XML response for AssociateEnvironmentOperationsRole.
type associateEnvironmentOperationsRoleResponse struct {
	XMLName          xml.Name         `xml:"AssociateEnvironmentOperationsRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

// handleAssociateEnvironmentOperationsRole associates an operations role with an environment.
func (h *Handler) handleAssociateEnvironmentOperationsRole(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	operationsRole := vals.Get("OperationsRole")
	if operationsRole == "" {
		return nil, fmt.Errorf("%w: OperationsRole is required", ErrInvalidParameter)
	}

	if err := h.Backend.AssociateEnvironmentOperationsRole(ctx, envName, operationsRole); err != nil {
		return nil, err
	}

	return &associateEnvironmentOperationsRoleResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-assoc-ops-role"},
	}, nil
}

// checkDNSAvailabilityResult is the result body for CheckDNSAvailability.
type checkDNSAvailabilityResult struct {
	FullyQualifiedCNAME string `xml:"FullyQualifiedCNAME"`
	Available           bool   `xml:"Available"`
}

// checkDNSAvailabilityResponse is the XML response for CheckDNSAvailability.
type checkDNSAvailabilityResponse struct {
	XMLName                    xml.Name                   `xml:"CheckDNSAvailabilityResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	CheckDNSAvailabilityResult checkDNSAvailabilityResult `xml:"CheckDNSAvailabilityResult"`
}

// handleCheckDNSAvailability checks whether a CNAME prefix is available.
func (h *Handler) handleCheckDNSAvailability(ctx context.Context, vals url.Values) (any, error) {
	cnamePrefix := vals.Get("CNAMEPrefix")
	if cnamePrefix == "" {
		return nil, fmt.Errorf("%w: CNAMEPrefix is required", ErrInvalidParameter)
	}

	available, fqcname := h.Backend.CheckDNSAvailability(ctx, cnamePrefix)

	return &checkDNSAvailabilityResponse{
		Xmlns: ebXMLNS,
		CheckDNSAvailabilityResult: checkDNSAvailabilityResult{
			Available:           available,
			FullyQualifiedCNAME: fqcname,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-check-dns"},
	}, nil
}

// composeEnvironmentsResult is the result body for ComposeEnvironments.
type composeEnvironmentsResult struct {
	Environments []environmentDescType `xml:"Environments>member"`
}

// composeEnvironmentsResponse is the XML response for ComposeEnvironments.
type composeEnvironmentsResponse struct {
	XMLName                   xml.Name                  `xml:"ComposeEnvironmentsResponse"`
	Xmlns                     string                    `xml:"xmlns,attr"`
	ResponseMetadata          responseMetadata          `xml:"ResponseMetadata"`
	ComposeEnvironmentsResult composeEnvironmentsResult `xml:"ComposeEnvironmentsResult"`
}

// handleComposeEnvironments composes a group of environments for an application.
func (h *Handler) handleComposeEnvironments(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	envs := h.Backend.ComposeEnvironments(ctx, appName)

	members := make([]environmentDescType, 0, len(envs))

	for _, env := range envs {
		members = append(members, toEnvironmentDesc(env))
	}

	return &composeEnvironmentsResponse{
		Xmlns:                     ebXMLNS,
		ComposeEnvironmentsResult: composeEnvironmentsResult{Environments: members},
		ResponseMetadata:          responseMetadata{RequestID: "eb-compose-envs"},
	}, nil
}

// describeEnvironmentHealthResponse is the XML response for DescribeEnvironmentHealth.
type describeEnvironmentHealthResult struct {
	EnvironmentName string `xml:"EnvironmentName"`
	HealthStatus    string `xml:"HealthStatus"`
	Status          string `xml:"Status"`
	Color           string `xml:"Color"`
	RefreshedAt     string `xml:"RefreshedAt"`
}

type describeEnvironmentHealthResponse struct {
	XMLName                         xml.Name                        `xml:"DescribeEnvironmentHealthResponse"`
	Xmlns                           string                          `xml:"xmlns,attr"`
	DescribeEnvironmentHealthResult describeEnvironmentHealthResult `xml:"DescribeEnvironmentHealthResult"`
	ResponseMetadata                responseMetadata                `xml:"ResponseMetadata"`
}

func (h *Handler) handleDescribeEnvironmentHealth(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	health, status, err := h.Backend.DescribeEnvironmentHealth(ctx, envName)
	if err != nil {
		return nil, err
	}

	return &describeEnvironmentHealthResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentHealthResult: describeEnvironmentHealthResult{
			EnvironmentName: envName,
			HealthStatus:    health,
			Status:          status,
			Color:           healthColorGreen,
			RefreshedAt:     healthRefreshedAt,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-env-health"},
	}, nil
}

// disassociateEnvironmentOperationsRoleResponse is the XML response for DisassociateEnvironmentOperationsRole.
type disassociateEnvironmentOperationsRoleResponse struct {
	XMLName          xml.Name         `xml:"DisassociateEnvironmentOperationsRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDisassociateEnvironmentOperationsRole(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")
	if envName == "" {
		return nil, fmt.Errorf("%w: EnvironmentName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateEnvironmentOperationsRole(ctx, envName); err != nil {
		return nil, err
	}

	return &disassociateEnvironmentOperationsRoleResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-disassoc-ops-role"},
	}, nil
}

// listAvailableSolutionStacksResponse is the XML response for ListAvailableSolutionStacks.
type listAvailableSolutionStacksResult struct {
	SolutionStacks []string `xml:"SolutionStacks>member"`
}

type listAvailableSolutionStacksResponse struct {
	XMLName                           xml.Name                          `xml:"ListAvailableSolutionStacksResponse"`
	Xmlns                             string                            `xml:"xmlns,attr"`
	ResponseMetadata                  responseMetadata                  `xml:"ResponseMetadata"`
	ListAvailableSolutionStacksResult listAvailableSolutionStacksResult `xml:"ListAvailableSolutionStacksResult"`
}

var availableSolutionStacks = []string{ //nolint:gochecknoglobals // package-level constant slice
	"64bit Amazon Linux 2023 v4.3.0 running Python 3.11",
	"64bit Amazon Linux 2023 v4.3.0 running Node.js 20",
	"64bit Amazon Linux 2023 v4.3.0 running Go 1",
	"64bit Amazon Linux 2023 v6.3.0 running PHP 8.3",
	"64bit Amazon Linux 2023 v4.3.0 running Corretto 21",
	"64bit Amazon Linux 2023 v4.3.0 running Corretto 17",
	"64bit Amazon Linux 2023 v4.3.0 running Ruby 3.3",
	"64bit Amazon Linux 2023 v4.3.0 running Docker",
}

func (h *Handler) handleListAvailableSolutionStacks(_ context.Context, _ url.Values) (any, error) {
	return &listAvailableSolutionStacksResponse{
		Xmlns: ebXMLNS,
		ListAvailableSolutionStacksResult: listAvailableSolutionStacksResult{
			SolutionStacks: availableSolutionStacks,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-solution-stacks"},
	}, nil
}

// requestEnvironmentInfoResponse is the XML response for RequestEnvironmentInfo.
type requestEnvironmentInfoResponse struct {
	XMLName          xml.Name         `xml:"RequestEnvironmentInfoResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleRequestEnvironmentInfo(_ context.Context, _ url.Values) (any, error) {
	return &requestEnvironmentInfoResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-request-env-info"},
	}, nil
}

// retrieveEnvironmentInfoResponse is the XML response for RetrieveEnvironmentInfo.
type environmentInfoDescription struct {
	InfoType        string `xml:"InfoType"`
	Ec2InstanceID   string `xml:"Ec2InstanceId"`
	SampleTimestamp string `xml:"SampleTimestamp"`
	Message         string `xml:"Message"`
}

type retrieveEnvironmentInfoResult struct {
	EnvironmentInfo []environmentInfoDescription `xml:"EnvironmentInfo>member"`
}

type retrieveEnvironmentInfoResponse struct {
	XMLName                       xml.Name                      `xml:"RetrieveEnvironmentInfoResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	ResponseMetadata              responseMetadata              `xml:"ResponseMetadata"`
	RetrieveEnvironmentInfoResult retrieveEnvironmentInfoResult `xml:"RetrieveEnvironmentInfoResult"`
}

func (h *Handler) handleRetrieveEnvironmentInfo(_ context.Context, _ url.Values) (any, error) {
	return &retrieveEnvironmentInfoResponse{
		Xmlns: ebXMLNS,
		RetrieveEnvironmentInfoResult: retrieveEnvironmentInfoResult{
			EnvironmentInfo: []environmentInfoDescription{},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-retrieve-env-info"},
	}, nil
}

// swapEnvironmentCNAMEsResponse is the XML response for SwapEnvironmentCNAMEs.
type swapEnvironmentCNAMEsResponse struct {
	XMLName          xml.Name         `xml:"SwapEnvironmentCNAMEsResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleSwapEnvironmentCNAMEs(ctx context.Context, vals url.Values) (any, error) {
	sourceEnv := vals.Get("SourceEnvironmentName")
	destEnv := vals.Get("DestinationEnvironmentName")

	if sourceEnv == "" && vals.Get("SourceEnvironmentId") == "" {
		return nil, fmt.Errorf(
			"%w: SourceEnvironmentName or SourceEnvironmentId is required",
			ErrInvalidParameter,
		)
	}

	if destEnv == "" && vals.Get("DestinationEnvironmentId") == "" {
		return nil, fmt.Errorf(
			"%w: DestinationEnvironmentName or DestinationEnvironmentId is required",
			ErrInvalidParameter,
		)
	}

	// Resolve env names from IDs if names not provided
	if sourceEnv == "" {
		srcID := vals.Get("SourceEnvironmentId")
		envs := h.Backend.DescribeEnvironments(ctx, "", nil, []string{srcID})

		if len(envs) > 0 {
			sourceEnv = envs[0].EnvironmentName
		}
	}

	if destEnv == "" {
		dstID := vals.Get("DestinationEnvironmentId")
		envs := h.Backend.DescribeEnvironments(ctx, "", nil, []string{dstID})

		if len(envs) > 0 {
			destEnv = envs[0].EnvironmentName
		}
	}

	// Actually swap CNAMEs (improvement #10)
	if err := h.Backend.SwapEnvironmentCNAMEs(ctx, sourceEnv, destEnv); err != nil {
		return nil, err
	}

	return &swapEnvironmentCNAMEsResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-swap-cnames"},
	}, nil
}
