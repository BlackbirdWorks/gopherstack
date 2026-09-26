package cloudformation

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	resTypeEC2TransitGateway      = "AWS::EC2::TransitGateway"
	resTypeEC2TGWAttachment       = "AWS::EC2::TransitGatewayAttachment"
	resTypeEC2TGWRouteTable       = "AWS::EC2::TransitGatewayRouteTable"
	resTypeEC2TGWRoute            = "AWS::EC2::TransitGatewayRoute"
	resTypeIAMAccessKey           = "AWS::IAM::AccessKey"
	resTypeIAMServiceLinkedRole   = "AWS::IAM::ServiceLinkedRole"
	resTypeIAMUserToGroupAddition = "AWS::IAM::UserToGroupAddition"
	resTypeLogsDestination        = "AWS::Logs::Destination"
	resTypeRoute53RecordSetGroup  = "AWS::Route53::RecordSetGroup"
	resTypeSQSQueueInlinePolicy   = "AWS::SQS::QueueInlinePolicy"
	resTypeSNSTopicInlinePolicy   = "AWS::SNS::TopicInlinePolicy"
)

// int64Prop reads an integer-valued property as int64, accepting JSON
// numbers or stringified numbers.
func int64Prop(props map[string]any, key string, params, physicalIDs map[string]string) int64 {
	if v, ok := props[key].(float64); ok {
		return int64(v)
	}

	if s := strProp(props, key, params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	}

	return 0
}

// boolProp reads a boolean-valued property, returning false when absent.
func boolProp(props map[string]any, key string) bool {
	v, _ := props[key].(bool)

	return v
}

// iamNameFromRefValue extracts a bare IAM resource name from a value that may
// be either a literal name or an ARN. This codebase's AWS::IAM::User/::Group
// Ref returns the resource ARN (resources_iam.go createIAMUser/createIAMGroup),
// so a template's {"Ref": "MyUser"} feeding UserName/GroupName/Users here
// arrives as an ARN -- IAM's own CreateAccessKey/AddUserToGroup take bare
// names, not ARNs.
func iamNameFromRefValue(s string) string {
	if !strings.HasPrefix(s, "arn:") {
		return s
	}

	if idx := strings.LastIndex(s, "/"); idx >= 0 {
		return s[idx+1:]
	}

	return s
}

// ---- EC2 TransitGateway ----

func (rc *ResourceCreator) createEC2TransitGateway(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	p := ec2backend.CreateTransitGatewayParams{
		Description:                     strProp(props, "Description", params, physicalIDs),
		AutoAcceptSharedAttachments:     strProp(props, "AutoAcceptSharedAttachments", params, physicalIDs),
		DefaultRouteTableAssociation:    strProp(props, "DefaultRouteTableAssociation", params, physicalIDs),
		DefaultRouteTablePropagation:    strProp(props, "DefaultRouteTablePropagation", params, physicalIDs),
		DNSSupport:                      strProp(props, "DnsSupport", params, physicalIDs),
		MulticastSupport:                strProp(props, "MulticastSupport", params, physicalIDs),
		SecurityGroupReferencingSupport: strProp(props, "SecurityGroupReferencingSupport", params, physicalIDs),
		VpnEcmpSupport:                  strProp(props, "VpnEcmpSupport", params, physicalIDs),
		TransitGatewayCidrBlocks:        strSliceProp(props["TransitGatewayCidrBlocks"], params, physicalIDs),
		AmazonSideAsn:                   int64Prop(props, "AmazonSideAsn", params, physicalIDs),
		Tags:                            tagListProp(props, params, physicalIDs),
	}

	tgw, err := rc.backends.EC2.Backend.CreateTransitGateway(p)
	if err != nil {
		return "", fmt.Errorf("create EC2 TransitGateway %s: %w", logicalID, err)
	}

	return tgw.ID, nil
}

func (rc *ResourceCreator) deleteEC2TransitGateway(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	if len(rc.backends.EC2.Backend.DescribeTransitGateways([]string{id})) == 0 {
		return nil
	}

	_, err := rc.backends.EC2.Backend.DeleteTransitGateway(id)

	return err
}

// ---- EC2 TransitGatewayAttachment (VPC attachment) ----

func (rc *ResourceCreator) createEC2TGWAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	tgwID := strProp(props, "TransitGatewayId", params, physicalIDs)
	vpcID := strProp(props, "VpcId", params, physicalIDs)
	subnetIDs := strSliceProp(props["SubnetIds"], params, physicalIDs)
	tags := tagListProp(props, params, physicalIDs)

	att, err := rc.backends.EC2.Backend.CreateTransitGatewayVpcAttachment(tgwID, vpcID, subnetIDs, tags)
	if err != nil {
		return "", fmt.Errorf("create EC2 TransitGatewayAttachment %s: %w", logicalID, err)
	}

	return att.TransitGatewayAttachmentID, nil
}

func (rc *ResourceCreator) deleteEC2TGWAttachment(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	err := rc.backends.EC2.Backend.DeleteTransitGatewayVpcAttachment(id)
	if errors.Is(err, ec2backend.ErrTGWAttachmentNotFound) {
		return nil
	}

	return err
}

// ---- EC2 TransitGatewayRouteTable ----

func (rc *ResourceCreator) createEC2TGWRouteTable(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	tgwID := strProp(props, "TransitGatewayId", params, physicalIDs)
	tags := tagListProp(props, params, physicalIDs)

	rt, err := rc.backends.EC2.Backend.CreateTransitGatewayRouteTable(tgwID, tags)
	if err != nil {
		return "", fmt.Errorf("create EC2 TransitGatewayRouteTable %s: %w", logicalID, err)
	}

	return rt.RouteTableID, nil
}

func (rc *ResourceCreator) deleteEC2TGWRouteTable(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	err := rc.backends.EC2.Backend.DeleteTransitGatewayRouteTable(id)
	if errors.Is(err, ec2backend.ErrTGWRouteTableNotFound) {
		return nil
	}

	return err
}

// ---- EC2 TransitGatewayRoute ----

// tgwRoutePhysID and its parsing counterpart encode the (routeTableID,
// destinationCIDR) compound key that identifies a TGW static route --
// real AWS's CreateTransitGatewayRoute response has no standalone route ID.
func tgwRoutePhysID(routeTableID, destinationCIDR string) string {
	return routeTableID + ":" + destinationCIDR
}

func (rc *ResourceCreator) createEC2TGWRoute(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	routeTableID := strProp(props, "TransitGatewayRouteTableId", params, physicalIDs)
	destCIDR := strProp(props, "DestinationCidrBlock", params, physicalIDs)
	attachmentID := strProp(props, "TransitGatewayAttachmentId", params, physicalIDs)
	blackhole := boolProp(props, "Blackhole")

	_, err := rc.backends.EC2.Backend.CreateTransitGatewayRoute(routeTableID, destCIDR, attachmentID, blackhole)
	if err != nil {
		return "", fmt.Errorf("create EC2 TransitGatewayRoute %s: %w", logicalID, err)
	}

	return tgwRoutePhysID(routeTableID, destCIDR), nil
}

func (rc *ResourceCreator) deleteEC2TGWRoute(physID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	routeTableID, destCIDR, ok := strings.Cut(physID, ":")
	if !ok {
		return nil
	}

	err := rc.backends.EC2.Backend.DeleteTransitGatewayRoute(routeTableID, destCIDR)
	if errors.Is(err, ec2backend.ErrRouteNotFound) {
		return nil
	}

	return err
}

// ---- IAM AccessKey ----

func (rc *ResourceCreator) createIAMAccessKey(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	userName := iamNameFromRefValue(strProp(props, "UserName", params, physicalIDs))

	ak, err := rc.backends.IAM.Backend.CreateAccessKey(userName)
	if err != nil {
		return "", fmt.Errorf("create IAM AccessKey for user %s: %w", userName, err)
	}

	if status := strProp(props, "Status", params, physicalIDs); status != "" && status != ak.Status {
		_ = rc.backends.IAM.Backend.UpdateAccessKey(userName, ak.AccessKeyID, status)
	}

	// Ref/GetAtt Id both return AccessKeyId (physID); GetAtt SecretAccessKey is
	// only ever available at creation time, so it is stashed under a synthetic
	// physicalIDs key (same convention as custom-resource Data outputs --
	// see applyCustomResourceResponse / getCustomResourceAttrFromPhysicalIDs).
	physicalIDs[logicalID+"/SecretAccessKey"] = ak.SecretAccessKey

	return ak.AccessKeyID, nil
}

func (rc *ResourceCreator) deleteIAMAccessKey(
	props map[string]any, stackPhysicalIDs map[string]string, physID string,
) error {
	if rc.backends.IAM == nil {
		return nil
	}

	userName := iamNameFromRefValue(strProp(props, "UserName", nil, stackPhysicalIDs))

	err := rc.backends.IAM.Backend.DeleteAccessKey(userName, physID)
	if errors.Is(err, iambackend.ErrAccessKeyNotFound) {
		return nil
	}

	return err
}

// ---- IAM ServiceLinkedRole ----

func (rc *ResourceCreator) createIAMServiceLinkedRole(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	awsServiceName := strProp(props, "AWSServiceName", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	customSuffix := strProp(props, "CustomSuffix", params, physicalIDs)

	role, err := rc.backends.IAM.Backend.CreateServiceLinkedRole(awsServiceName, description, customSuffix)
	if err != nil {
		return "", fmt.Errorf("create IAM ServiceLinkedRole for %s: %w", awsServiceName, err)
	}

	return role.RoleName, nil
}

func (rc *ResourceCreator) deleteIAMServiceLinkedRole(roleName string) error {
	if rc.backends.IAM == nil {
		return nil
	}

	err := rc.backends.IAM.Backend.DeleteServiceLinkedRole(roleName)
	if errors.Is(err, iambackend.ErrRoleNotFound) {
		return nil
	}

	return err
}

// ---- IAM UserToGroupAddition ----

func (rc *ResourceCreator) createIAMUserToGroupAddition(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	groupName := iamNameFromRefValue(strProp(props, "GroupName", params, physicalIDs))
	users := strSliceProp(props["Users"], params, physicalIDs)

	for i, u := range users {
		users[i] = iamNameFromRefValue(u)
	}

	for _, userName := range users {
		if err := rc.backends.IAM.Backend.AddUserToGroup(groupName, userName); err != nil {
			return "", fmt.Errorf("add user %s to IAM group %s: %w", userName, groupName, err)
		}
	}

	return logicalID + "-" + groupName, nil
}

func (rc *ResourceCreator) deleteIAMUserToGroupAddition(
	props map[string]any, stackPhysicalIDs map[string]string,
) error {
	if rc.backends.IAM == nil {
		return nil
	}

	groupName := iamNameFromRefValue(strProp(props, "GroupName", nil, stackPhysicalIDs))
	users := strSliceProp(props["Users"], nil, stackPhysicalIDs)

	for i, u := range users {
		users[i] = iamNameFromRefValue(u)
	}

	for _, userName := range users {
		err := rc.backends.IAM.Backend.RemoveUserFromGroup(groupName, userName)
		if err != nil && !errors.Is(err, iambackend.ErrGroupNotFound) && !errors.Is(err, iambackend.ErrUserNotFound) {
			return err
		}
	}

	return nil
}

// ---- Logs::Destination ----

func (rc *ResourceCreator) createLogsDestination(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DestinationName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	targetArn := strProp(props, "TargetArn", params, physicalIDs)
	roleArn := strProp(props, "RoleArn", params, physicalIDs)

	mem, ok := rc.backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	if !ok {
		return name, nil
	}

	dest, err := mem.PutDestination(name, targetArn, roleArn)
	if err != nil {
		return "", fmt.Errorf("create Logs Destination %s: %w", name, err)
	}

	if policy := strProp(props, "DestinationPolicy", params, physicalIDs); policy != "" {
		if polErr := mem.PutDestinationPolicy(dest.DestinationName, policy); polErr != nil {
			return "", fmt.Errorf("set Logs Destination policy %s: %w", name, polErr)
		}
	}

	return dest.DestinationName, nil
}

func (rc *ResourceCreator) deleteLogsDestination(name string) error {
	if rc.backends.CloudWatchLogs == nil {
		return nil
	}

	mem, ok := rc.backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	err := mem.DeleteDestination(name)
	if errors.Is(err, cwlogsbackend.ErrDestinationNotFound) {
		return nil
	}

	return err
}

// logsDestinationArn reproduces the ARN formula PutDestination computes
// (destinations.go), letting Fn::GetAtt derive Arn without a backend lookup.
func logsDestinationArn(name, accountID, region string) string {
	return arn.Build("logs", region, accountID, "destination:"+name)
}

// ---- Route53 RecordSetGroup ----

func (rc *ResourceCreator) createRoute53RecordSetGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Route53 == nil {
		return logicalID + "-stub", nil
	}

	hostedZoneID := strProp(props, "HostedZoneId", params, physicalIDs)

	rawSets, _ := props["RecordSets"].([]any)

	changes := make([]route53backend.Change, 0, len(rawSets))

	for _, rs := range rawSets {
		m, ok := rs.(map[string]any)
		if !ok {
			continue
		}

		changes = append(changes, route53RecordSetGroupChange(m, params, physicalIDs))
	}

	if len(changes) > 0 {
		if _, err := rc.backends.Route53.Backend.ChangeResourceRecordSets(hostedZoneID, changes); err != nil {
			return "", fmt.Errorf("create Route53 RecordSetGroup %s: %w", logicalID, err)
		}
	}

	return logicalID, nil
}

// route53RecordSetGroupChange converts one RecordSetGroup.RecordSets entry
// into a real Route53 Change (split out of createRoute53RecordSetGroup to
// keep its cognitive complexity down).
func route53RecordSetGroupChange(m map[string]any, params, physicalIDs map[string]string) route53backend.Change {
	var ttl int64
	if v, ttlOK := m["TTL"].(float64); ttlOK {
		ttl = int64(v)
	} else if s := strProp(m, "TTL", params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			ttl = n
		}
	}

	var records []route53backend.ResourceRecord
	if rawRecords, recOK := m["ResourceRecords"].([]any); recOK {
		for _, r := range rawRecords {
			if val := resolve(r, params, physicalIDs); val != "" {
				records = append(records, route53backend.ResourceRecord{Value: val})
			}
		}
	}

	return route53backend.Change{
		Action: route53backend.ChangeActionCreate,
		ResourceRecordSet: route53backend.ResourceRecordSet{
			Name:    strProp(m, "Name", params, physicalIDs),
			Type:    strProp(m, "Type", params, physicalIDs),
			TTL:     ttl,
			Records: records,
		},
	}
}

// ---- SQS QueueInlinePolicy / SNS TopicInlinePolicy ----

func (rc *ResourceCreator) createSQSQueueInlinePolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SQS == nil {
		return logicalID + "-stub", nil
	}

	queueURL := strProp(props, "Queue", params, physicalIDs)
	policyDocument := strProp(props, "PolicyDocument", params, physicalIDs)

	err := rc.backends.SQS.Backend.SetQueueAttributes(&sqsbackend.SetQueueAttributesInput{
		QueueURL:   queueURL,
		Attributes: map[string]string{"Policy": policyDocument},
	})
	if err != nil {
		return "", fmt.Errorf("set SQS QueueInlinePolicy on %s: %w", queueURL, err)
	}

	return queueURL, nil
}

func (rc *ResourceCreator) createSNSTopicInlinePolicy(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SNS == nil {
		return logicalID + "-stub", nil
	}

	topicArn := strProp(props, "TopicArn", params, physicalIDs)
	policyDocument := strProp(props, "PolicyDocument", params, physicalIDs)

	if err := rc.backends.SNS.Backend.SetTopicAttributes(topicArn, "Policy", policyDocument); err != nil {
		return "", fmt.Errorf("set SNS TopicInlinePolicy on %s: %w", topicArn, err)
	}

	return topicArn, nil
}
