package ec2

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	tagspkg "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ---- action handlers ----

// maxUserDataBytes is the AWS limit on decoded EC2 user data (16 KiB).
const maxUserDataBytes = 16384

// validateUserData enforces the AWS EC2 contract for the UserData parameter:
// it must be valid standard base64 and, once decoded, must not exceed the
// 16 KiB limit. An empty value is accepted (user data is optional). Malformed
// base64 yields InvalidUserData.Malformed; an over-limit payload yields
// InvalidParameterValue, matching AWS error codes.
func validateUserData(userData string) error {
	if userData == "" {
		return nil
	}

	decoded, err := base64.StdEncoding.DecodeString(userData)
	if err != nil {
		return fmt.Errorf(
			"%w: user data must be a valid base64-encoded string",
			ErrInvalidUserData,
		)
	}

	if len(decoded) > maxUserDataBytes {
		return fmt.Errorf(
			"%w: User data is limited to %d bytes",
			ErrInvalidParameter,
			maxUserDataBytes,
		)
	}

	return nil
}

// applyInstanceLaunchSettings persists the base64 user data and applies the key
// name and security groups to each freshly launched instance.
func (h *Handler) applyInstanceLaunchSettings(
	instances []*Instance,
	userData, keyName string,
	sgIDs []string,
) error {
	for _, inst := range instances {
		if userData != "" {
			// Store as-is; DescribeInstanceAttribute returns the raw (base64) form.
			if err := h.Backend.SetInstanceAttribute(inst.ID, attrUserData, userData); err != nil {
				return err
			}
		}

		if keyName != "" {
			inst.KeyName = keyName
		}

		if len(sgIDs) > 0 {
			if err := h.Backend.SetInstanceSecurityGroups(inst.ID, sgIDs); err != nil {
				return err
			}

			inst.SecurityGroups = sgIDs
		}
	}

	return nil
}

// applyInstanceLaunchAttributes wires the RunInstances top-level
// DisableApiTermination / InstanceInitiatedShutdownBehavior / EbsOptimized
// parameters (distinct from the post-launch ModifyInstanceAttribute path)
// onto newly-created instances.
func (h *Handler) applyInstanceLaunchAttributes(
	instances []*Instance,
	disableAPITermination, shutdownBehavior, ebsOptimized string,
) error {
	type launchAttr struct {
		name, value string
	}

	// maxLaunchAttrs is the number of RunInstances attribute params handled
	// below (DisableApiTermination, InstanceInitiatedShutdownBehavior, EbsOptimized).
	const maxLaunchAttrs = 3

	attrs := make([]launchAttr, 0, maxLaunchAttrs)
	if disableAPITermination != "" {
		attrs = append(attrs, launchAttr{attrDisableAPITermination, disableAPITermination})
	}

	if shutdownBehavior != "" {
		attrs = append(attrs, launchAttr{attrInstanceInitiatedShutdownBehavior, shutdownBehavior})
	}

	if ebsOptimized != "" {
		attrs = append(attrs, launchAttr{attrEBSOptimized, ebsOptimized})
	}

	if len(attrs) == 0 {
		return nil
	}

	for _, inst := range instances {
		for _, a := range attrs {
			if err := h.Backend.SetInstanceAttribute(inst.ID, a.name, a.value); err != nil {
				return err
			}
		}
	}

	return nil
}

// applyRunInstancesCreditSpecification wires RunInstances'
// CreditSpecification.CpuCredits (ec2@v1.329.0 serializers.go
// awsEc2query_serializeOpDocumentRunInstancesInput: flat key
// "CreditSpecification.CpuCredits") onto the same instanceCreditSpecs store
// ModifyInstanceCreditSpecification already uses -- observable via
// DescribeInstanceCreditSpecifications.
func (h *Handler) applyRunInstancesCreditSpecification(instances []*Instance, cpuCredits string) {
	if cpuCredits == "" {
		return
	}

	specs := make([]InstanceCreditSpec, 0, len(instances))
	for _, inst := range instances {
		specs = append(specs, InstanceCreditSpec{InstanceID: inst.ID, CPUCredits: cpuCredits})
	}

	h.Backend.ModifyInstanceCreditSpecification(specs)
}

// applyRunInstancesPrivateDNSNameOptions wires RunInstances'
// PrivateDnsNameOptions (ec2@v1.329.0 serializers.go
// awsEc2query_serializeOpDocumentRunInstancesInput: nested under
// "PrivateDnsNameOptions.HostnameType"/".EnableResourceNameDnsARecord"/
// ".EnableResourceNameDnsAAAARecord") onto the same per-instance store
// ModifyPrivateDnsNameOptions already uses.
func (h *Handler) applyRunInstancesPrivateDNSNameOptions(instances []*Instance, vals url.Values) error {
	hostnameType := vals.Get("PrivateDnsNameOptions.HostnameType")

	_, hasARecord := vals["PrivateDnsNameOptions.EnableResourceNameDnsARecord"]
	_, hasAAAARecord := vals["PrivateDnsNameOptions.EnableResourceNameDnsAAAARecord"]

	if hostnameType == "" && !hasARecord && !hasAAAARecord {
		return nil
	}

	var enableARecord, enableAAAARecord *bool

	if hasARecord {
		v := vals.Get("PrivateDnsNameOptions.EnableResourceNameDnsARecord") == ec2BooleanTrue
		enableARecord = &v
	}

	if hasAAAARecord {
		v := vals.Get("PrivateDnsNameOptions.EnableResourceNameDnsAAAARecord") == ec2BooleanTrue
		enableAAAARecord = &v
	}

	for _, inst := range instances {
		if _, err := h.Backend.ModifyPrivateDNSNameOptions(
			inst.ID, hostnameType, enableARecord, enableAAAARecord,
		); err != nil {
			return err
		}
	}

	return nil
}

// applyRunInstancesPostCreateOptions applies the RunInstances request fields
// that mutate freshly-launched instances after creation but aren't part of
// the core InstanceType/ImageId/SubnetId/count path: CreditSpecification,
// PrivateDnsNameOptions, and the IAM instance profile association.
func (h *Handler) applyRunInstancesPostCreateOptions(instances []*Instance, vals url.Values) error {
	h.applyRunInstancesCreditSpecification(instances, vals.Get("CreditSpecification.CpuCredits"))

	if err := h.applyRunInstancesPrivateDNSNameOptions(instances, vals); err != nil {
		return err
	}

	if vals.Get("HibernationOptions.Configured") == ec2BooleanTrue {
		if err := h.Backend.SetInstancesHibernation(instanceIDsOf(instances), true); err != nil {
			return err
		}
	}

	profileARN := iamInstanceProfileArg(vals)
	if profileARN == "" {
		return nil
	}

	for _, inst := range instances {
		if _, err := h.Backend.AssociateIamInstanceProfile(inst.ID, profileARN); err != nil {
			return err
		}
	}

	return nil
}

// iamInstanceProfileArg reads the real RunInstances/AssociateIamInstanceProfile
// IamInstanceProfile.Arn/IamInstanceProfile.Name wire keys (serializers.go:91938,
// awsEc2query_serializeDocumentIamInstanceProfileSpecification), preferring Arn.
func iamInstanceProfileArg(vals url.Values) string {
	if arn := vals.Get("IamInstanceProfile.Arn"); arn != "" {
		return arn
	}

	return vals.Get("IamInstanceProfile.Name")
}

// iamProfilesByInstance returns the wire-shaped IAM instance profile for
// every instance that has a current "associated" association, mirroring the
// real DescribeInstances/RunInstances rendering of types.Instance.
// IamInstanceProfile (deserializers.go:110585). DescribeIamInstanceProfileAssociations
// already scans every association internally regardless of the instanceID
// filter passed to it, so one unfiltered call replaces what used to be one
// full scan per instance.
func (h *Handler) iamProfilesByInstance() map[string]*iamProfileSpec {
	out := make(map[string]*iamProfileSpec)

	for _, assoc := range h.Backend.DescribeIamInstanceProfileAssociations(nil, "") {
		if assoc.State != stateAssociated {
			continue
		}

		if _, exists := out[assoc.InstanceID]; exists {
			continue
		}

		out[assoc.InstanceID] = &iamProfileSpec{
			ARN: assoc.IamInstanceProfile,
			ID:  iamProfileName(assoc.IamInstanceProfile),
		}
	}

	return out
}

// securityGroupNamesFor returns the ID→Name map for every security group
// referenced by instances, fetched with a single DescribeSecurityGroups call
// instead of one per instance.
func instanceIDsOf(instances []*Instance) []string {
	ids := make([]string, len(instances))
	for i, inst := range instances {
		ids[i] = inst.ID
	}

	return ids
}

func securityGroupNamesFor(b Backend, instances []*Instance) map[string]string {
	var ids []string

	seen := make(map[string]struct{})

	for _, inst := range instances {
		for _, sgID := range inst.SecurityGroups {
			if _, ok := seen[sgID]; ok {
				continue
			}

			seen[sgID] = struct{}{}
			ids = append(ids, sgID)
		}
	}

	names := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return names
	}

	for _, sg := range b.DescribeSecurityGroups(ids) {
		names[sg.ID] = sg.Name
	}

	return names
}

// runInstancesWithMetadata validates MetadataOptions before launching so a bad value leaves no instances behind.
func (h *Handler) runInstancesWithMetadata(
	imageID, instanceType, subnetID string, count int, vals url.Values,
) ([]*Instance, error) {
	mdOpts, err := metadataOptionsFromValues(vals)
	if err != nil {
		return nil, err
	}

	pinnedID := tagspkg.PinnedIDFromTagSpecs(vals, "instance")

	instances, err := h.Backend.RunInstancesWithID(pinnedID, imageID, instanceType, subnetID, count)
	if err != nil {
		return nil, err
	}

	if err = h.Backend.ApplyLaunchMetadataOptions(instanceIDsOf(instances), mdOpts); err != nil {
		return nil, err
	}

	return instances, nil
}

func (h *Handler) handleRunInstances(vals url.Values, reqID string) (any, error) {
	if err := h.overlayLaunchTemplate(vals); err != nil {
		return nil, err
	}

	imageID := vals.Get("ImageId")
	instanceType := vals.Get("InstanceType")
	subnetID := vals.Get("SubnetId")
	userData := vals.Get("UserData")
	keyName := vals.Get("KeyName")
	disableAPITermination := vals.Get("DisableApiTermination")
	shutdownBehavior := vals.Get("InstanceInitiatedShutdownBehavior")
	ebsOptimized := vals.Get("EbsOptimized")

	if err := h.validateRunInstancesInput(imageID, instanceType, keyName, userData); err != nil {
		return nil, err
	}

	if err := h.requireLaunchableImageFor(vals, imageID); err != nil {
		return nil, err
	}

	minCount, maxCount, err := parseRunInstancesCounts(vals)
	if err != nil {
		return nil, err
	}

	_ = maxCount // AWS uses MaxCount for capacity planning; mock always launches minCount

	sgIDs, err := h.validateSecurityGroupIDs(vals)
	if err != nil {
		return nil, err
	}

	instances, err := h.runInstancesWithMetadata(imageID, instanceType, subnetID, minCount, vals)
	if err != nil {
		return nil, err
	}

	if err = h.applyInstanceLaunchSettings(instances, userData, keyName, sgIDs); err != nil {
		return nil, err
	}

	if err = h.applyInstanceLaunchAttributes(
		instances, disableAPITermination, shutdownBehavior, ebsOptimized,
	); err != nil {
		return nil, err
	}

	if err = h.applyRunInstancesStorage(instances, vals); err != nil {
		return nil, err
	}

	if cb, c := h.computeBackend(); c != nil {
		h.launchOnCompute(h.svcCtx, cb, c, instances, keyName, userData)
	}

	if err = h.tagLaunchedInstances(instances, vals); err != nil {
		return nil, err
	}

	ids := make([]string, len(instances))
	for i, inst := range instances {
		ids[i] = inst.ID
	}

	// Render from locked copies: the lifecycle reconciler mutates stored instances concurrently.
	if fresh := h.Backend.DescribeInstances(ids, ""); len(fresh) == len(instances) {
		instances = fresh
	}

	tagsByID := h.Backend.TagsForResources(ids)
	iamProfiles := h.iamProfilesByInstance()
	sgNames := securityGroupNamesFor(h.Backend, instances)
	sdcByID := h.Backend.PrimaryNetworkInterfaceSourceDestChecks(instanceIDsOf(instances))

	devicesByID := h.Backend.InstanceBlockDevices(instanceIDsOf(instances))

	items := make([]instanceItem, 0, len(instances))
	for _, inst := range instances {
		item := toInstanceItem(inst, tagsByID[inst.ID], iamProfiles[inst.ID], sgNames, sdcByID[inst.ID])
		item.BlockDeviceMapping = blockDeviceSet(devicesByID[inst.ID])
		items = append(items, item)
	}

	return &runInstancesResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		ReservationID: instanceReservationID(instances[0]),
		OwnerID:       h.AccountID,
		InstancesSet:  instanceItemSet{Items: items},
	}, nil
}

// describeInstancesMaxResults is the maximum MaxResults for DescribeInstances.
const describeInstancesMaxResults = 1000

// describeInstancesMinResults is the minimum MaxResults for DescribeInstances.
const describeInstancesMinResults = 5

func (h *Handler) handleDescribeInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if err := h.requireInstancesExist(ids); err != nil {
		return nil, err
	}

	// Parse named EC2 filters: Filter.N.Name / Filter.N.Value.M
	filters := parseEC2Filters(vals)

	instances, tagsByID := h.describeInstancesFiltered(ids, filters)

	// Pagination: MaxResults / NextToken.
	maxResults := 0
	if v := vals.Get("MaxResults"); v != "" {
		if _, scanErr := fmt.Sscan(v, &maxResults); scanErr != nil || maxResults < 1 {
			return nil, fmt.Errorf("%w: MaxResults must be a positive integer", ErrInvalidParameter)
		}
		if maxResults < describeInstancesMinResults || maxResults > describeInstancesMaxResults {
			return nil, fmt.Errorf(
				"%w: MaxResults must be between %d and %d",
				ErrInvalidParameter,
				describeInstancesMinResults,
				describeInstancesMaxResults,
			)
		}
	}

	offset := 0
	if tok := vals.Get("NextToken"); tok != "" {
		n := page.DecodeHMACToken(tok, ec2PaginationSalt)
		if n == 0 {
			return nil, fmt.Errorf("%w: the pagination token is not valid", ErrInvalidPaginationToken)
		}
		offset = n
	}

	var nextToken string

	if maxResults > 0 {
		if offset > len(instances) {
			offset = len(instances)
		}

		instances = instances[offset:]

		if len(instances) > maxResults {
			nextToken = page.EncodeHMACToken(offset+maxResults, ec2PaginationSalt)
			instances = instances[:maxResults]
		}
	}

	iamProfiles := h.iamProfilesByInstance()
	sgNames := securityGroupNamesFor(h.Backend, instances)
	sdcByID := h.Backend.PrimaryNetworkInterfaceSourceDestChecks(instanceIDsOf(instances))

	devicesByID := h.Backend.InstanceBlockDevices(instanceIDsOf(instances))

	items := make([]instanceItem, 0, len(instances))
	for _, inst := range instances {
		item := toInstanceItem(inst, tagsByID[inst.ID], iamProfiles[inst.ID], sgNames, sdcByID[inst.ID])
		item.BlockDeviceMapping = blockDeviceSet(devicesByID[inst.ID])
		items = append(items, item)
	}

	reservations := groupReservations(instances, items, h.AccountID)

	return &describeInstancesResponse{
		Xmlns:          ec2XMLNS,
		RequestID:      reqID,
		ReservationSet: reservationSet{Items: reservations},
		NextToken:      nextToken,
	}, nil
}

// instanceMatcher is the optional backend fast path that filters before copying.
type instanceMatcher interface {
	DescribeInstancesMatching(
		ids []string, match func(inst *Instance, tags map[string]string) bool,
	) ([]*Instance, map[string]map[string]string)
}

func (h *Handler) describeInstancesFiltered(
	ids []string, filters map[string][]string,
) ([]*Instance, map[string]map[string]string) {
	if m, ok := h.Backend.(instanceMatcher); ok {
		return m.DescribeInstancesMatching(ids, compileInstanceFilters(filters))
	}

	instances := h.Backend.DescribeInstances(ids, "")

	allIDs := make([]string, len(instances))
	for i, inst := range instances {
		allIDs[i] = inst.ID
	}

	tagsByID := h.Backend.TagsForResources(allIDs)

	return applyInstanceFilters(instances, filters, tagsByID), tagsByID
}

func (h *Handler) handleTerminateInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	cb, c := h.computeBackend()

	var (
		providerIDs map[string]string
		dnsNames    map[string]string
	)

	if c != nil {
		providerIDs = snapshotProviderIDs(cb, ids)

		if lookup, ok := h.Backend.(instanceLookup); ok {
			dnsNames = snapshotPublicDNSNames(lookup, ids)
		}
	}

	changes, err := h.Backend.TerminateInstances(ids)
	if err != nil {
		return nil, err
	}

	if c != nil {
		h.terminateOnCompute(h.svcCtx, cb, c, providerIDs, dnsNames)
		h.reapComputeVolumes(h.svcCtx)
	}

	for _, ch := range changes {
		h.imds.forget(ch.InstanceID)
	}

	items := make([]instanceStateChangeItem, 0, len(changes))
	for _, ch := range changes {
		items = append(items, instanceStateChangeItem{
			InstanceID:    ch.InstanceID,
			CurrentState:  stateItem{Code: ch.CurrentState.Code, Name: ch.CurrentState.Name},
			PreviousState: stateItem{Code: ch.PreviousState.Code, Name: ch.PreviousState.Name},
		})
	}

	return &terminateInstancesResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		InstancesSet: instanceStateChangeSet{Items: items},
	}, nil
}

// boolToEC2Attr renders a Go bool as the "true"/"false" string EC2 query-protocol
// attribute values use.
func boolToEC2Attr(v bool) string {
	if v {
		return ec2BooleanTrue
	}

	return ec2BooleanFalse
}

// handleDescribeInstanceAttribute returns the current value for the requested instance attribute.
// Terraform calls this after RunInstances to read instanceInitiatedShutdownBehavior.
func (h *Handler) handleDescribeInstanceAttribute(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	attr := vals.Get("Attribute")

	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	instances := h.Backend.DescribeInstances([]string{instanceID}, "")
	if len(instances) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if attr == attrBlockDeviceMapping {
		return &describeInstanceBlockDevicesResponse{
			Xmlns:      ec2XMLNS,
			RequestID:  reqID,
			InstanceID: instanceID,
			Devices:    blockDeviceItems(h.Backend.InstanceBlockDevices([]string{instanceID})[instanceID]),
		}, nil
	}

	inst := instances[0]
	attrValue := h.instanceAttributeValue(inst, instanceID, attr)

	return &describeInstanceAttributeResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		InstanceID: instanceID,
		Attribute:  namedStringAttr{XMLName: xml.Name{Local: attr}, Value: attrValue},
	}, nil
}

// instanceAttributeValue builds the DescribeInstanceAttribute string value
// from stored instance state when possible, falling back to AWS defaults for
// unmodelled attributes. Split out of handleDescribeInstanceAttribute to keep
// cyclomatic complexity down.
func (h *Handler) instanceAttributeValue(inst *Instance, instanceID, attr string) string {
	switch attr {
	case attrUserData:
		return inst.UserData
	case attrInstanceType:
		return inst.InstanceType
	case attrEnaSupport:
		return boolToEC2Attr(inst.EnaSupport)
	case attrSriovNetSupport:
		if inst.SriovNetSupport != "" {
			return inst.SriovNetSupport
		}

		return "simple"
	case attrDisableAPIStop:
		return boolToEC2Attr(inst.DisableAPIStop)
	case attrDisableAPITermination:
		return boolToEC2Attr(inst.DisableAPITermination)
	case attrEBSOptimized:
		return boolToEC2Attr(inst.EBSOptimized)
	case attrSourceDest:
		// sourceDestCheck lives on the primary ENI attachment; AWS defaults
		// it to true for VPC instances.
		return boolToEC2Attr(h.Backend.PrimaryNetworkInterfaceSourceDestCheck(instanceID))
	case attrInstanceInitiatedShutdownBehavior:
		if inst.InstanceInitiatedShutdownBehavior != "" {
			return inst.InstanceInitiatedShutdownBehavior
		}

		return "stop"
	case attrKernel, attrRamdisk:
		// Modern (HVM) instances have no kernel/ramdisk image; AWS returns an
		// empty value rather than "stop" (that default only applies to
		// instanceInitiatedShutdownBehavior).
		return ""
	default:
		return ""
	}
}

func toInstanceItem(
	inst *Instance, instanceTags map[string]string, iamProfile *iamProfileSpec, sgNames map[string]string,
	sourceDestCheck bool,
) instanceItem {
	tagItems := make([]instanceTagItem, 0, len(instanceTags))
	for k, v := range instanceTags {
		tagItems = append(tagItems, instanceTagItem{Key: k, Value: v})
	}

	slices.SortFunc(tagItems, func(a, b instanceTagItem) int { return strings.Compare(a.Key, b.Key) })

	// GroupIdentifier carries both groupId and groupName (ec2@v1.329.0
	// deserializers.go:107843 awsEc2query_deserializeDocumentGroupIdentifier);
	// a security group deleted after attachment yields an empty groupName,
	// matching a lookup miss below.
	groupItems := make([]instanceGroupItem, 0, len(inst.SecurityGroups))
	for _, sgID := range inst.SecurityGroups {
		groupItems = append(groupItems, instanceGroupItem{GroupID: sgID, GroupName: sgNames[sgID]})
	}

	item := instanceItem{
		InstanceID:            inst.ID,
		RootDeviceName:        inst.RootDeviceName,
		RootDeviceType:        rootDeviceTypeFor(inst),
		ImageID:               inst.ImageID,
		InstanceType:          inst.InstanceType,
		StateItem:             stateItem{Code: inst.State.Code, Name: inst.State.Name},
		StateTransitionReason: inst.StateTransitionReason,
		VPCID:                 inst.VPCID,
		SubnetID:              inst.SubnetID,
		LaunchTime:            inst.LaunchTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		OutpostArn:            inst.OutpostArn,
		PrivateIPAddress:      inst.PrivateIP,
		PublicIPAddress:       inst.PublicIPAddress,
		PublicDNSName:         inst.PublicDNSName,
		KeyName:               inst.KeyName,
		SriovNetSupport:       inst.SriovNetSupport,
		EBSOptimized:          inst.EBSOptimized,
		EnaSupport:            inst.EnaSupport,
		SourceDestCheck:       &sourceDestCheck,
		GroupSet:              instanceGroupSet{Items: groupItems},
		TagSet:                instanceTagItemSet{Items: tagItems},
		IamInstanceProfile:    iamProfile,
		Placement: instancePlacementItem{
			Tenancy:              inst.Placement.Tenancy,
			AvailabilityZone:     inst.Placement.AvailabilityZone,
			GroupName:            inst.Placement.GroupName,
			GroupID:              inst.Placement.GroupID,
			Affinity:             inst.Placement.Affinity,
			HostID:               inst.Placement.HostID,
			HostResourceGroupArn: inst.Placement.HostResourceGroupArn,
			PartitionNumber:      inst.Placement.PartitionNumber,
		},
	}

	md := metadataOptionsOf(inst)
	item.MetadataOptions = &imdsOptionsItem{
		State: md.State, HTTPTokens: md.HTTPTokens, HTTPEndpoint: md.HTTPEndpoint,
		InstanceMetadataTags: md.InstanceMetadataTags, HTTPPutResponseHopLimit: md.HTTPPutResponseHopLimit,
	}

	if inst.HibernationConfigured {
		item.HibernationOptions = &instanceHibernationOptionsItem{Configured: true}
	}

	if inst.StateReasonCode != "" {
		item.StateReasonItem = &stateReasonItem{
			Code:    inst.StateReasonCode,
			Message: inst.StateReasonMessage,
		}
	}

	if inst.CPUOptions.CoreCount > 0 || inst.CPUOptions.ThreadsPerCore > 0 {
		item.CPUOptions = &instanceCPUOptionsItem{
			CoreCount:      inst.CPUOptions.CoreCount,
			ThreadsPerCore: inst.CPUOptions.ThreadsPerCore,
		}
	}
	if inst.MaintenanceOptions.AutoRecovery != "" {
		item.MaintenanceOptions = &instanceMaintenanceOptionsItem{
			AutoRecovery: inst.MaintenanceOptions.AutoRecovery,
		}
	}
	if inst.NetworkPerformanceOptions.BandwidthWeighting != "" {
		item.NetworkPerformanceOptions = &instanceNetworkPerformanceOptionsItem{
			BandwidthWeighting: inst.NetworkPerformanceOptions.BandwidthWeighting,
		}
	}

	if inst.PrivateDNSNameOptions.HostnameType != "" {
		item.PrivateDNSNameOptions = &instancePrivateDNSNameOptionsItem{
			HostnameType:                    inst.PrivateDNSNameOptions.HostnameType,
			EnableResourceNameDNSARecord:    inst.PrivateDNSNameOptions.EnableResourceNameDNSARecord,
			EnableResourceNameDNSAAAARecord: inst.PrivateDNSNameOptions.EnableResourceNameDNSAAAARecord,
		}
	}

	return item
}

type stateItem struct {
	Name string `xml:"name"`
	Code int    `xml:"code"`
}

type instanceGroupItem struct {
	GroupID   string `xml:"groupId"`
	GroupName string `xml:"groupName"`
}

type instanceGroupSet struct {
	Items []instanceGroupItem `xml:"item"`
}

type instancePlacementItem struct {
	Tenancy              string `xml:"tenancy,omitempty"`
	AvailabilityZone     string `xml:"availabilityZone,omitempty"`
	GroupName            string `xml:"groupName,omitempty"`
	GroupID              string `xml:"groupId,omitempty"`
	Affinity             string `xml:"affinity,omitempty"`
	HostID               string `xml:"hostId,omitempty"`
	HostResourceGroupArn string `xml:"hostResourceGroupArn,omitempty"`
	PartitionNumber      int32  `xml:"partitionNumber,omitempty"`
}

// stateReasonItem is the <stateReason> element carrying the structured
// code/message for an instance's most recent state transition.
type stateReasonItem struct {
	Code    string `xml:"code,omitempty"`
	Message string `xml:"message,omitempty"`
}

type instanceCPUOptionsItem struct {
	CoreCount      int32 `xml:"coreCount"`
	ThreadsPerCore int32 `xml:"threadsPerCore"`
}

type instanceMaintenanceOptionsItem struct {
	AutoRecovery string `xml:"autoRecovery,omitempty"`
}

type instanceNetworkPerformanceOptionsItem struct {
	BandwidthWeighting string `xml:"bandwidthWeighting,omitempty"`
}

// instancePrivateDNSNameOptionsItem mirrors PrivateDnsNameOptionsResponse
// (ec2@v1.329.0 types/types.go:18643), rendered under <privateDnsNameOptions>
// (deserializers.go:114945).
type instancePrivateDNSNameOptionsItem struct {
	HostnameType                    string `xml:"hostnameType,omitempty"`
	EnableResourceNameDNSARecord    bool   `xml:"enableResourceNameDnsARecord"`
	EnableResourceNameDNSAAAARecord bool   `xml:"enableResourceNameDnsAAAARecord"`
}

type instanceHibernationOptionsItem struct {
	Configured bool `xml:"configured"`
}

// instanceBlockDeviceItem mirrors types.InstanceBlockDeviceMapping
// (ec2@v1.329.0 deserializers.go awsEc2query_deserializeDocumentInstanceBlockDeviceMapping).
type instanceBlockDeviceItem struct {
	DeviceName string                `xml:"deviceName"`
	Ebs        instanceEbsDeviceItem `xml:"ebs"`
}

type instanceEbsDeviceItem struct {
	AttachTime          string `xml:"attachTime,omitempty"`
	Status              string `xml:"status,omitempty"`
	VolumeID            string `xml:"volumeId"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
}

type instanceBlockDeviceSet struct {
	Items []instanceBlockDeviceItem `xml:"item"`
}

func blockDeviceSet(atts []VolumeAttachment) *instanceBlockDeviceSet {
	if len(atts) == 0 {
		return nil
	}

	return &instanceBlockDeviceSet{Items: blockDeviceItems(atts)}
}

func blockDeviceItems(atts []VolumeAttachment) []instanceBlockDeviceItem {
	if len(atts) == 0 {
		return nil
	}

	items := make([]instanceBlockDeviceItem, 0, len(atts))

	for _, att := range atts {
		items = append(items, instanceBlockDeviceItem{
			DeviceName: att.Device,
			Ebs: instanceEbsDeviceItem{
				AttachTime:          att.AttachTime.UTC().Format("2006-01-02T15:04:05.000Z"),
				Status:              att.State,
				VolumeID:            att.VolumeID,
				DeleteOnTermination: att.DeleteOnTermination,
			},
		})
	}

	slices.SortFunc(
		items,
		func(a, b instanceBlockDeviceItem) int { return strings.Compare(a.DeviceName, b.DeviceName) },
	)

	return items
}

type instanceItem struct {
	RootDeviceName            string                                 `xml:"rootDeviceName,omitempty"`
	RootDeviceType            string                                 `xml:"rootDeviceType,omitempty"`
	BlockDeviceMapping        *instanceBlockDeviceSet                `xml:"blockDeviceMapping,omitempty"`
	HibernationOptions        *instanceHibernationOptionsItem        `xml:"hibernationOptions,omitempty"`
	NetworkPerformanceOptions *instanceNetworkPerformanceOptionsItem `xml:"networkPerformanceOptions,omitempty"`
	MaintenanceOptions        *instanceMaintenanceOptionsItem        `xml:"maintenanceOptions,omitempty"`
	MetadataOptions           *imdsOptionsItem                       `xml:"metadataOptions,omitempty"`
	CPUOptions                *instanceCPUOptionsItem                `xml:"cpuOptions,omitempty"`
	StateReasonItem           *stateReasonItem                       `xml:"stateReason,omitempty"`
	IamInstanceProfile        *iamProfileSpec                        `xml:"iamInstanceProfile,omitempty"`
	PrivateDNSNameOptions     *instancePrivateDNSNameOptionsItem     `xml:"privateDnsNameOptions,omitempty"`
	SourceDestCheck           *bool                                  `xml:"sourceDestCheck,omitempty"`
	Placement                 instancePlacementItem                  `xml:"placement"`
	// OutpostArn is a top-level field, sibling to Placement -- see
	// store.go's Instance.OutpostArn doc comment for the SDK confirmation.
	OutpostArn       string    `xml:"outpostArn,omitempty"`
	PublicDNSName    string    `xml:"dnsName,omitempty"`
	SubnetID         string    `xml:"subnetId,omitempty"`
	PrivateIPAddress string    `xml:"privateIpAddress,omitempty"`
	PublicIPAddress  string    `xml:"ipAddress,omitempty"`
	LaunchTime       string    `xml:"launchTime"`
	KeyName          string    `xml:"keyName,omitempty"`
	VPCID            string    `xml:"vpcId,omitempty"`
	InstanceType     string    `xml:"instanceType"`
	ImageID          string    `xml:"imageId"`
	InstanceID       string    `xml:"instanceId"`
	StateItem        stateItem `xml:"instanceState"`
	// StateTransitionReason is AWS's legacy free-text reason string, distinct
	// from the structured StateReasonItem above.
	StateTransitionReason string             `xml:"reason,omitempty"`
	SriovNetSupport       string             `xml:"sriovNetSupport,omitempty"`
	GroupSet              instanceGroupSet   `xml:"groupSet"`
	TagSet                instanceTagItemSet `xml:"tagSet"`
	EBSOptimized          bool               `xml:"ebsOptimized"`
	EnaSupport            bool               `xml:"enaSupport"`
}

// instanceTagItem is the embedded per-instance tag entry in DescribeInstances
// XML (no resourceId/resourceType fields, only key/value).
type instanceTagItem struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type instanceTagItemSet struct {
	Items []instanceTagItem `xml:"item"`
}

type instanceItemSet struct {
	Items []instanceItem `xml:"item"`
}

type runInstancesResponse struct {
	XMLName       xml.Name        `xml:"RunInstancesResponse"`
	Xmlns         string          `xml:"xmlns,attr"`
	RequestID     string          `xml:"requestId"`
	ReservationID string          `xml:"reservationId"`
	OwnerID       string          `xml:"ownerId"`
	InstancesSet  instanceItemSet `xml:"instancesSet"`
}

type reservationItem struct {
	ReservationID string          `xml:"reservationId"`
	OwnerID       string          `xml:"ownerId"`
	InstancesSet  instanceItemSet `xml:"instancesSet"`
}

type reservationSet struct {
	Items []reservationItem `xml:"item"`
}

type describeInstancesResponse struct {
	XMLName        xml.Name       `xml:"DescribeInstancesResponse"`
	Xmlns          string         `xml:"xmlns,attr"`
	RequestID      string         `xml:"requestId"`
	NextToken      string         `xml:"nextToken,omitempty"`
	ReservationSet reservationSet `xml:"reservationSet"`
}

type instanceStateChangeItem struct {
	InstanceID    string    `xml:"instanceId"`
	CurrentState  stateItem `xml:"currentState"`
	PreviousState stateItem `xml:"previousState"`
}

type instanceStateChangeSet struct {
	Items []instanceStateChangeItem `xml:"item"`
}

type terminateInstancesResponse struct {
	XMLName      xml.Name               `xml:"TerminateInstancesResponse"`
	Xmlns        string                 `xml:"xmlns,attr"`
	RequestID    string                 `xml:"requestId"`
	InstancesSet instanceStateChangeSet `xml:"instancesSet"`
}

type namedStringAttr struct {
	XMLName xml.Name `json:"xmlName"`
	Value   string   `json:"value,omitempty" xml:"value"`
}

type describeInstanceBlockDevicesResponse struct {
	XMLName    xml.Name                  `xml:"DescribeInstanceAttributeResponse"`
	Xmlns      string                    `xml:"xmlns,attr"`
	RequestID  string                    `xml:"requestId"`
	InstanceID string                    `xml:"instanceId"`
	Devices    []instanceBlockDeviceItem `xml:"blockDeviceMapping>item"`
}

type describeInstanceAttributeResponse struct {
	XMLName    xml.Name `xml:"DescribeInstanceAttributeResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	InstanceID string   `xml:"instanceId"`
	Attribute  namedStringAttr
}

// instanceReservationID returns the reservation an instance was launched in. Instances restored
// from snapshots that predate the field get a stable id derived from their own.
func instanceReservationID(inst *Instance) string {
	if inst.ReservationID != "" {
		return inst.ReservationID
	}

	return "r-" + strings.TrimPrefix(inst.ID, "i-")
}

// groupReservations groups items (parallel to instances) by launch reservation, in first-seen order.
func groupReservations(instances []*Instance, items []instanceItem, ownerID string) []reservationItem {
	reservations := []reservationItem{}
	index := make(map[string]int, len(instances))

	for i, inst := range instances {
		rid := instanceReservationID(inst)

		pos, ok := index[rid]
		if !ok {
			pos = len(reservations)
			index[rid] = pos
			reservations = append(reservations, reservationItem{ReservationID: rid, OwnerID: ownerID})
		}

		reservations[pos].InstancesSet.Items = append(reservations[pos].InstancesSet.Items, items[i])
	}

	return reservations
}

func rootDeviceTypeFor(inst *Instance) string {
	if inst.RootDeviceName == "" {
		return ""
	}

	return "ebs"
}

// overlayLaunchTemplate fills RunInstances parameters the request leaves unset from the
// referenced launch template version.
func (h *Handler) overlayLaunchTemplate(vals url.Values) error {
	ref := vals.Get("LaunchTemplate.LaunchTemplateId")
	if ref == "" {
		ref = vals.Get("LaunchTemplate.LaunchTemplateName")
	}

	if ref == "" {
		return nil
	}

	data, err := h.Backend.ResolveLaunchTemplateData(ref, vals.Get("LaunchTemplate.Version"))
	if err != nil {
		return err
	}

	set := make(map[string]struct{}, len(vals))
	for k := range vals {
		set[ltDataGroup(k)] = struct{}{}
	}

	for k, v := range data {
		if _, ok := set[ltDataGroup(k)]; !ok {
			vals.Set(k, v)
		}
	}

	if vals.Get("SubnetId") == "" {
		vals.Set("SubnetId", vals.Get("NetworkInterface.1.SubnetId"))
	}

	if len(parseMemberList(vals, "SecurityGroupId")) == 0 {
		niGroups := collectIndexed(vals, "NetworkInterface.1.SecurityGroupId")
		for i, sg := range niGroups {
			vals.Set(fmt.Sprintf("SecurityGroupId.%d", i+1), sg)
		}
	}

	return nil
}

func collectIndexed(vals url.Values, prefix string) []string {
	var out []string

	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			return out
		}

		out = append(out, v)
	}
}

// attachLaunchVolumes creates the root and BlockDeviceMapping EBS volumes of each new instance.
func (h *Handler) attachLaunchVolumes(instances []*Instance, vals url.Values) error {
	var specs []LaunchVolumeSpec

	for i := 1; ; i++ {
		prefix := fmt.Sprintf("BlockDeviceMapping.%d.", i)

		dev := vals.Get(prefix + "DeviceName")
		if dev == "" {
			break
		}

		if vals.Get(prefix+"NoDevice") != "" || vals.Get(prefix+"VirtualName") != "" {
			continue
		}

		spec := LaunchVolumeSpec{
			DeviceName: dev,
			VolumeType: vals.Get(prefix + "Ebs.VolumeType"),
			SnapshotID: vals.Get(prefix + "Ebs.SnapshotId"),
			KmsKeyID:   vals.Get(prefix + "Ebs.KmsKeyId"),
			Encrypted:  vals.Get(prefix+"Ebs.Encrypted") == ec2BooleanTrue,
		}
		spec.Size, _ = strconv.Atoi(vals.Get(prefix + "Ebs.VolumeSize"))
		spec.Iops, _ = strconv.Atoi(vals.Get(prefix + "Ebs.Iops"))
		spec.Throughput, _ = strconv.Atoi(vals.Get(prefix + "Ebs.Throughput"))

		if dot := vals.Get(prefix + "Ebs.DeleteOnTermination"); dot != "" {
			spec.DeleteOnTermination = dot == ec2BooleanTrue
		} else {
			spec.DeleteOnTerminationUnset = true
		}

		specs = append(specs, spec)
	}

	volTags := parseTagSpecification(vals, "volume")

	for _, inst := range instances {
		own := make([]LaunchVolumeSpec, len(specs))
		copy(own, specs)

		if err := h.Backend.AttachLaunchVolumes(inst.ID, own, volTags); err != nil {
			return err
		}
	}

	return nil
}

func (h *Handler) applyRunInstancesStorage(instances []*Instance, vals url.Values) error {
	if err := h.applyRunInstancesPostCreateOptions(instances, vals); err != nil {
		return err
	}

	return h.attachLaunchVolumes(instances, vals)
}

func (h *Handler) tagLaunchedInstances(instances []*Instance, vals url.Values) error {
	tags := parseTagSpecification(vals, "instance")
	if len(tags) == 0 {
		return nil
	}

	ids := make([]string, 0, len(instances))
	for _, inst := range instances {
		ids = append(ids, inst.ID)
	}

	return h.Backend.CreateTags(ids, tags)
}
