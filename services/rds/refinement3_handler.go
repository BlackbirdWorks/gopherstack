package rds

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultTargetGroupName = proxyDefaultTargetGroupName

// dispatchExtended12 routes DB Proxy and Activity Stream operations added in refinement 3.
// Split from dispatchExtended11 to keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended12(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBProxy":
		return h.handleCreateDBProxy(vals)
	case "DeleteDBProxy":
		return h.handleDeleteDBProxy(vals)
	case "DescribeDBProxies":
		return h.handleDescribeDBProxies(vals)
	case "ModifyDBProxy":
		return h.handleModifyDBProxy(vals)
	case "RegisterDBProxyTargets":
		return h.handleRegisterDBProxyTargets(vals)
	case "DeregisterDBProxyTargets":
		return h.handleDeregisterDBProxyTargets(vals)
	case "DescribeDBProxyTargets":
		return h.handleDescribeDBProxyTargets(vals)
	case "DescribeDBProxyTargetGroups":
		return h.handleDescribeDBProxyTargetGroups(vals)
	case "ModifyDBProxyTargetGroup":
		return h.handleModifyDBProxyTargetGroup(vals)
	default:
		return h.dispatchExtended13(action, vals)
	}
}

// dispatchExtended13 routes proxy endpoint and activity stream operations added in refinement 3.
// Split from dispatchExtended12 to keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended13(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBProxyEndpoint":
		return h.handleCreateDBProxyEndpoint(vals)
	case "DeleteDBProxyEndpoint":
		return h.handleDeleteDBProxyEndpoint(vals)
	case "DescribeDBProxyEndpoints":
		return h.handleDescribeDBProxyEndpoints(vals)
	case "ModifyDBProxyEndpoint":
		return h.handleModifyDBProxyEndpoint(vals)
	case "StartActivityStream":
		return h.handleStartActivityStream(vals)
	case "StopActivityStream":
		return h.handleStopActivityStream(vals)
	case "ModifyActivityStream":
		return h.handleModifyActivityStream(vals)
	default:
		return h.dispatchExtended14(action, vals)
	}
}

// ---- DB Proxy handlers ----

func (h *Handler) handleCreateDBProxy(vals url.Values) (any, error) {
	name := vals.Get("DBProxyName")
	engineFamily := vals.Get("EngineFamily")
	roleARN := vals.Get("RoleArn")

	auth := parseUserAuthConfigs(vals)

	proxy, err := h.Backend.CreateDBProxy(name, engineFamily, roleARN, auth)
	if err != nil {
		return nil, err
	}

	return createDBProxyResponse{
		Xmlns:   rdsXMLNS,
		DBProxy: toXMLProxy(proxy),
	}, nil
}

func (h *Handler) handleDeleteDBProxy(vals url.Values) (any, error) {
	name := vals.Get("DBProxyName")
	proxy, err := h.Backend.DeleteDBProxy(name)
	if err != nil {
		return nil, err
	}

	return deleteDBProxyResponse{
		Xmlns:   rdsXMLNS,
		DBProxy: toXMLProxy(proxy),
	}, nil
}

func (h *Handler) handleDescribeDBProxies(vals url.Values) (any, error) {
	name := vals.Get("DBProxyName")
	proxies, err := h.Backend.DescribeDBProxies(name)
	if err != nil {
		return nil, err
	}

	xmlProxies := make([]xmlDBProxy, 0, len(proxies))
	for i := range proxies {
		xmlProxies = append(xmlProxies, toXMLProxy(&proxies[i]))
	}

	return describeDBProxiesResponse{
		Xmlns:     rdsXMLNS,
		DBProxies: xmlDBProxyList{Members: xmlProxies},
	}, nil
}

func (h *Handler) handleModifyDBProxy(vals url.Values) (any, error) {
	name := vals.Get("DBProxyName")

	var requireTLS *bool
	if v := vals.Get("RequireTLS"); v != "" {
		b := v == "true"
		requireTLS = &b
	}

	var idleClientTimeout *int
	if v := vals.Get("IdleClientTimeout"); v != "" {
		i, _ := strconv.Atoi(v)
		idleClientTimeout = &i
	}

	auth := parseUserAuthConfigs(vals)

	proxy, err := h.Backend.ModifyDBProxy(name, requireTLS, idleClientTimeout, auth)
	if err != nil {
		return nil, err
	}

	return modifyDBProxyResponse{
		Xmlns:   rdsXMLNS,
		DBProxy: toXMLProxy(proxy),
	}, nil
}

func (h *Handler) handleRegisterDBProxyTargets(vals url.Values) (any, error) {
	proxyName := vals.Get("DBProxyName")
	targetGroupName := vals.Get("TargetGroupName")
	if targetGroupName == "" {
		targetGroupName = defaultTargetGroupName
	}

	instanceIDs := extractIndexedList(vals, "DBInstanceIdentifiers.member.")
	clusterIDs := extractIndexedList(vals, "DBClusterIdentifiers.member.")

	targets, err := h.Backend.RegisterDBProxyTargets(proxyName, targetGroupName, instanceIDs, clusterIDs)
	if err != nil {
		return nil, err
	}

	xmlTargets := make([]xmlDBProxyTarget, 0, len(targets))
	for _, t := range targets {
		xmlTargets = append(xmlTargets, toXMLProxyTarget(t))
	}

	return registerDBProxyTargetsResponse{
		Xmlns:   rdsXMLNS,
		Targets: xmlDBProxyTargetList{Members: xmlTargets},
	}, nil
}

func (h *Handler) handleDeregisterDBProxyTargets(vals url.Values) (any, error) {
	proxyName := vals.Get("DBProxyName")
	targetGroupName := vals.Get("TargetGroupName")
	if targetGroupName == "" {
		targetGroupName = defaultTargetGroupName
	}

	instanceIDs := extractIndexedList(vals, "DBInstanceIdentifiers.member.")
	clusterIDs := extractIndexedList(vals, "DBClusterIdentifiers.member.")

	if err := h.Backend.DeregisterDBProxyTargets(proxyName, targetGroupName, instanceIDs, clusterIDs); err != nil {
		return nil, err
	}

	return deregisterDBProxyTargetsResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleDescribeDBProxyTargets(vals url.Values) (any, error) {
	proxyName := vals.Get("DBProxyName")
	targetGroupName := vals.Get("TargetGroupName")
	if targetGroupName == "" {
		targetGroupName = defaultTargetGroupName
	}

	targets, err := h.Backend.DescribeDBProxyTargets(proxyName, targetGroupName)
	if err != nil {
		return nil, err
	}

	xmlTargets := make([]xmlDBProxyTarget, 0, len(targets))
	for _, t := range targets {
		xmlTargets = append(xmlTargets, toXMLProxyTarget(t))
	}

	return describeDBProxyTargetsResponse{
		Xmlns:   rdsXMLNS,
		Targets: xmlDBProxyTargetList{Members: xmlTargets},
	}, nil
}

func (h *Handler) handleDescribeDBProxyTargetGroups(vals url.Values) (any, error) {
	proxyName := vals.Get("DBProxyName")
	targetGroupName := vals.Get("TargetGroupName")

	groups, err := h.Backend.DescribeDBProxyTargetGroups(proxyName, targetGroupName)
	if err != nil {
		return nil, err
	}

	xmlGroups := make([]xmlDBProxyTargetGroup, 0, len(groups))
	for i := range groups {
		xmlGroups = append(xmlGroups, toXMLProxyTargetGroup(&groups[i]))
	}

	return describeDBProxyTargetGroupsResponse{
		Xmlns:        rdsXMLNS,
		TargetGroups: xmlDBProxyTargetGroupList{Members: xmlGroups},
	}, nil
}

func (h *Handler) handleModifyDBProxyTargetGroup(vals url.Values) (any, error) {
	proxyName := vals.Get("DBProxyName")
	targetGroupName := vals.Get("TargetGroupName")
	if targetGroupName == "" {
		targetGroupName = defaultTargetGroupName
	}

	maxPct, _ := strconv.Atoi(vals.Get("ConnectionPoolConfig.MaxConnectionsPercent"))
	maxIdlePct, _ := strconv.Atoi(vals.Get("ConnectionPoolConfig.MaxIdleConnectionsPercent"))
	borrowTimeout, _ := strconv.Atoi(vals.Get("ConnectionPoolConfig.ConnectionBorrowTimeout"))
	initQuery := vals.Get("ConnectionPoolConfig.InitQuery")

	cfg := ConnectionPoolConfig{
		MaxConnectionsPercent:     maxPct,
		MaxIdleConnectionsPercent: maxIdlePct,
		ConnectionBorrowTimeout:   borrowTimeout,
		InitQuery:                 initQuery,
	}

	tg, err := h.Backend.ModifyDBProxyTargetGroup(proxyName, targetGroupName, cfg)
	if err != nil {
		return nil, err
	}

	return modifyDBProxyTargetGroupResponse{
		Xmlns:       rdsXMLNS,
		TargetGroup: toXMLProxyTargetGroup(tg),
	}, nil
}

// ---- DB Proxy Endpoint handlers ----

func (h *Handler) handleCreateDBProxyEndpoint(vals url.Values) (any, error) {
	proxyName := vals.Get("DBProxyName")
	endpointName := vals.Get("DBProxyEndpointName")
	targetRole := vals.Get("TargetRole")

	subnetIDs := extractIndexedList(vals, "VpcSubnetIds.member.")
	sgIDs := extractIndexedList(vals, "VpcSecurityGroupIds.member.")

	ep, err := h.Backend.CreateDBProxyEndpoint(proxyName, endpointName, targetRole, subnetIDs, sgIDs)
	if err != nil {
		return nil, err
	}

	return createDBProxyEndpointResponse{
		Xmlns:           rdsXMLNS,
		DBProxyEndpoint: toXMLProxyEndpoint(ep),
	}, nil
}

func (h *Handler) handleDeleteDBProxyEndpoint(vals url.Values) (any, error) {
	endpointName := vals.Get("DBProxyEndpointName")
	ep, err := h.Backend.DeleteDBProxyEndpoint(endpointName)
	if err != nil {
		return nil, err
	}

	return deleteDBProxyEndpointResponse{
		Xmlns:           rdsXMLNS,
		DBProxyEndpoint: toXMLProxyEndpoint(ep),
	}, nil
}

func (h *Handler) handleDescribeDBProxyEndpoints(vals url.Values) (any, error) {
	proxyName := vals.Get("DBProxyName")
	endpointName := vals.Get("DBProxyEndpointName")

	endpoints, err := h.Backend.DescribeDBProxyEndpoints(proxyName, endpointName)
	if err != nil {
		return nil, err
	}

	xmlEps := make([]xmlDBProxyEndpoint, 0, len(endpoints))
	for i := range endpoints {
		xmlEps = append(xmlEps, toXMLProxyEndpoint(&endpoints[i]))
	}

	return describeDBProxyEndpointsResponse{
		Xmlns:            rdsXMLNS,
		DBProxyEndpoints: xmlDBProxyEndpointList{Members: xmlEps},
	}, nil
}

func (h *Handler) handleModifyDBProxyEndpoint(vals url.Values) (any, error) {
	endpointName := vals.Get("DBProxyEndpointName")
	sgIDs := extractIndexedList(vals, "VpcSecurityGroupIds.member.")

	ep, err := h.Backend.ModifyDBProxyEndpoint(endpointName, sgIDs)
	if err != nil {
		return nil, err
	}

	return modifyDBProxyEndpointResponse{
		Xmlns:           rdsXMLNS,
		DBProxyEndpoint: toXMLProxyEndpoint(ep),
	}, nil
}

// ---- Activity Stream handlers ----

func (h *Handler) handleStartActivityStream(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	kmsKeyID := vals.Get("KmsKeyId")
	mode := vals.Get("Mode")

	// The resource ARN encodes the cluster identifier at the end
	clusterID := arnToClusterID(resourceARN)

	cluster, err := h.Backend.StartActivityStream(clusterID, kmsKeyID, mode)
	if err != nil {
		return nil, err
	}

	return startActivityStreamResponse{
		Xmlns:             rdsXMLNS,
		KinesisStreamName: cluster.ActivityStreamKinesisStreamName,
		KMSKeyID:          cluster.ActivityStreamKMSKeyID,
		Status:            cluster.ActivityStreamStatus,
		Mode:              cluster.ActivityStreamMode,
		ApplyImmediately:  true,
	}, nil
}

func (h *Handler) handleStopActivityStream(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	clusterID := arnToClusterID(resourceARN)

	cluster, err := h.Backend.StopActivityStream(clusterID)
	if err != nil {
		return nil, err
	}

	return stopActivityStreamResponse{
		Xmlns:             rdsXMLNS,
		KinesisStreamName: cluster.ActivityStreamKinesisStreamName,
		KMSKeyID:          cluster.ActivityStreamKMSKeyID,
		Status:            cluster.ActivityStreamStatus,
	}, nil
}

func (h *Handler) handleModifyActivityStream(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	auditPolicy := vals.Get("AuditPolicyState")
	clusterID := arnToClusterID(resourceARN)

	cluster, err := h.Backend.ModifyActivityStream(clusterID, auditPolicy)
	if err != nil {
		return nil, err
	}

	return modifyActivityStreamResponse{
		Xmlns:       rdsXMLNS,
		KMSKeyID:    cluster.ActivityStreamKMSKeyID,
		Status:      cluster.ActivityStreamStatus,
		AuditPolicy: cluster.ActivityStreamAuditPolicy,
	}, nil
}

// arnToClusterID extracts the cluster identifier from an ARN like
// "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster".
func arnToClusterID(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return arn
}

// extractIndexedList reads a numbered list of values from form values like
// "prefix1", "prefix2", etc.
func extractIndexedList(vals url.Values, prefix string) []string {
	result := make([]string, 0)
	for i := 1; ; i++ {
		v := vals.Get(prefix + strconv.Itoa(i))
		if v == "" {
			break
		}
		result = append(result, v)
	}

	return result
}

// parseUserAuthConfigs parses UserAuth list from form values.
func parseUserAuthConfigs(vals url.Values) []UserAuthConfig {
	auth := make([]UserAuthConfig, 0)
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("Auth.member.%d.", i)
		scheme := vals.Get(prefix + "AuthScheme")
		if scheme == "" {
			break
		}
		auth = append(auth, UserAuthConfig{
			AuthScheme:  scheme,
			Description: vals.Get(prefix + "Description"),
			IAMAuth:     vals.Get(prefix + "IAMAuth"),
			SecretARN:   vals.Get(prefix + "SecretArn"),
			UserName:    vals.Get(prefix + "UserName"),
		})
	}

	return auth
}

// ---- XML conversion helpers ----

func toXMLProxy(p *DBProxy) xmlDBProxy {
	xmlAuth := make([]xmlUserAuthConfig, 0, len(p.Auth))
	for _, a := range p.Auth {
		xmlAuth = append(xmlAuth, xmlUserAuthConfig(a))
	}

	return xmlDBProxy{
		DBProxyName:       p.DBProxyName,
		DBProxyARN:        p.DBProxyARN,
		Status:            p.Status,
		Endpoint:          p.Endpoint,
		EngineFamily:      p.EngineFamily,
		RoleARN:           p.RoleARN,
		RequireTLS:        p.RequireTLS,
		IdleClientTimeout: p.IdleClientTimeout,
		DebugLogging:      p.DebugLogging,
		CreatedDate:       p.CreatedDate.Format(time.RFC3339),
		UpdatedDate:       p.UpdatedDate.Format(time.RFC3339),
		Auth:              xmlUserAuthConfigList{Members: xmlAuth},
	}
}

func toXMLProxyTarget(t DBProxyTarget) xmlDBProxyTarget {
	return xmlDBProxyTarget(t)
}

func toXMLProxyTargetGroup(tg *DBProxyTargetGroup) xmlDBProxyTargetGroup {
	return xmlDBProxyTargetGroup{
		DBProxyName:     tg.DBProxyName,
		TargetGroupName: tg.TargetGroupName,
		TargetGroupARN:  tg.TargetGroupARN,
		IsDefault:       tg.IsDefault,
		Status:          tg.Status,
		CreatedDate:     tg.CreatedDate.Format(time.RFC3339),
		UpdatedDate:     tg.UpdatedDate.Format(time.RFC3339),
		ConnectionPoolConfig: xmlConnectionPoolConfig{
			MaxConnectionsPercent:     tg.ConnectionPoolConfig.MaxConnectionsPercent,
			MaxIdleConnectionsPercent: tg.ConnectionPoolConfig.MaxIdleConnectionsPercent,
			ConnectionBorrowTimeout:   tg.ConnectionPoolConfig.ConnectionBorrowTimeout,
			InitQuery:                 tg.ConnectionPoolConfig.InitQuery,
		},
	}
}

func toXMLProxyEndpoint(ep *DBProxyEndpoint) xmlDBProxyEndpoint {
	return xmlDBProxyEndpoint{
		DBProxyEndpointName: ep.DBProxyEndpointName,
		DBProxyEndpointARN:  ep.DBProxyEndpointARN,
		DBProxyName:         ep.DBProxyName,
		Status:              ep.Status,
		Endpoint:            ep.Endpoint,
		TargetRole:          ep.TargetRole,
		IsDefault:           ep.IsDefault,
		CreatedDate:         ep.CreatedDate.Format(time.RFC3339),
	}
}

// ---- XML types ----

type xmlUserAuthConfig struct {
	AuthScheme  string `xml:"AuthScheme,omitempty"`
	Description string `xml:"Description,omitempty"`
	IAMAuth     string `xml:"IAMAuth,omitempty"`
	SecretARN   string `xml:"SecretArn,omitempty"`
	UserName    string `xml:"UserName,omitempty"`
}

type xmlUserAuthConfigList struct {
	Members []xmlUserAuthConfig `xml:"UserAuthConfig"`
}

type xmlConnectionPoolConfig struct {
	InitQuery                 string `xml:"InitQuery,omitempty"`
	ConnectionBorrowTimeout   int    `xml:"ConnectionBorrowTimeout,omitempty"`
	MaxConnectionsPercent     int    `xml:"MaxConnectionsPercent,omitempty"`
	MaxIdleConnectionsPercent int    `xml:"MaxIdleConnectionsPercent,omitempty"`
}

type xmlDBProxy struct {
	DBProxyName       string                `xml:"DBProxyName"`
	DBProxyARN        string                `xml:"DBProxyArn"`
	Status            string                `xml:"Status"`
	Endpoint          string                `xml:"Endpoint,omitempty"`
	EngineFamily      string                `xml:"EngineFamily,omitempty"`
	RoleARN           string                `xml:"RoleArn,omitempty"`
	CreatedDate       string                `xml:"CreatedDate,omitempty"`
	UpdatedDate       string                `xml:"UpdatedDate,omitempty"`
	Auth              xmlUserAuthConfigList `xml:"Auth"`
	IdleClientTimeout int                   `xml:"IdleClientTimeout,omitempty"`
	RequireTLS        bool                  `xml:"RequireTLS,omitempty"`
	DebugLogging      bool                  `xml:"DebugLogging,omitempty"`
}

type xmlDBProxyList struct {
	Members []xmlDBProxy `xml:"member"`
}

type createDBProxyResponse struct {
	XMLName xml.Name   `xml:"CreateDBProxyResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	DBProxy xmlDBProxy `xml:"CreateDBProxyResult>DBProxy"`
}

type deleteDBProxyResponse struct {
	XMLName xml.Name   `xml:"DeleteDBProxyResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	DBProxy xmlDBProxy `xml:"DeleteDBProxyResult>DBProxy"`
}

type describeDBProxiesResponse struct {
	XMLName   xml.Name       `xml:"DescribeDBProxiesResponse"`
	Xmlns     string         `xml:"xmlns,attr"`
	DBProxies xmlDBProxyList `xml:"DescribeDBProxiesResult>DBProxies"`
}

type modifyDBProxyResponse struct {
	XMLName xml.Name   `xml:"ModifyDBProxyResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	DBProxy xmlDBProxy `xml:"ModifyDBProxyResult>DBProxy"`
}

type xmlDBProxyTarget struct {
	TargetARN        string `xml:"TargetArn,omitempty"`
	Endpoint         string `xml:"Endpoint,omitempty"`
	TrackedClusterID string `xml:"TrackedClusterId,omitempty"`
	RdsResourceID    string `xml:"RdsResourceId,omitempty"`
	Type             string `xml:"Type,omitempty"`
	Role             string `xml:"Role,omitempty"`
	TargetHealth     string `xml:"TargetHealth,omitempty"`
	Port             int    `xml:"Port,omitempty"`
}

type xmlDBProxyTargetList struct {
	Members []xmlDBProxyTarget `xml:"member"`
}

type registerDBProxyTargetsResponse struct {
	XMLName xml.Name             `xml:"RegisterDBProxyTargetsResponse"`
	Xmlns   string               `xml:"xmlns,attr"`
	Targets xmlDBProxyTargetList `xml:"RegisterDBProxyTargetsResult>DBProxyTargets"`
}

type deregisterDBProxyTargetsResult struct{}

type deregisterDBProxyTargetsResponse struct {
	Result  deregisterDBProxyTargetsResult `xml:"DeregisterDBProxyTargetsResult"`
	XMLName xml.Name                       `xml:"DeregisterDBProxyTargetsResponse"`
	Xmlns   string                         `xml:"xmlns,attr"`
}

type describeDBProxyTargetsResponse struct {
	XMLName xml.Name             `xml:"DescribeDBProxyTargetsResponse"`
	Xmlns   string               `xml:"xmlns,attr"`
	Targets xmlDBProxyTargetList `xml:"DescribeDBProxyTargetsResult>Targets"`
}

type xmlDBProxyTargetGroup struct {
	DBProxyName          string                  `xml:"DBProxyName"`
	TargetGroupName      string                  `xml:"TargetGroupName"`
	TargetGroupARN       string                  `xml:"TargetGroupArn,omitempty"`
	Status               string                  `xml:"Status,omitempty"`
	CreatedDate          string                  `xml:"CreatedDate,omitempty"`
	UpdatedDate          string                  `xml:"UpdatedDate,omitempty"`
	ConnectionPoolConfig xmlConnectionPoolConfig `xml:"ConnectionPoolConfig"`
	IsDefault            bool                    `xml:"IsDefault,omitempty"`
}

type xmlDBProxyTargetGroupList struct {
	Members []xmlDBProxyTargetGroup `xml:"member"`
}

type describeDBProxyTargetGroupsResponse struct {
	XMLName      xml.Name                  `xml:"DescribeDBProxyTargetGroupsResponse"`
	Xmlns        string                    `xml:"xmlns,attr"`
	TargetGroups xmlDBProxyTargetGroupList `xml:"DescribeDBProxyTargetGroupsResult>TargetGroups"`
}

type modifyDBProxyTargetGroupResponse struct {
	XMLName     xml.Name              `xml:"ModifyDBProxyTargetGroupResponse"`
	Xmlns       string                `xml:"xmlns,attr"`
	TargetGroup xmlDBProxyTargetGroup `xml:"ModifyDBProxyTargetGroupResult>DBProxyTargetGroup"`
}

type xmlDBProxyEndpoint struct {
	DBProxyEndpointName string `xml:"DBProxyEndpointName"`
	DBProxyEndpointARN  string `xml:"DBProxyEndpointArn,omitempty"`
	DBProxyName         string `xml:"DBProxyName"`
	Status              string `xml:"Status,omitempty"`
	Endpoint            string `xml:"Endpoint,omitempty"`
	TargetRole          string `xml:"TargetRole,omitempty"`
	CreatedDate         string `xml:"CreatedDate,omitempty"`
	IsDefault           bool   `xml:"IsDefault,omitempty"`
}

type xmlDBProxyEndpointList struct {
	Members []xmlDBProxyEndpoint `xml:"member"`
}

type createDBProxyEndpointResponse struct {
	XMLName         xml.Name           `xml:"CreateDBProxyEndpointResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	DBProxyEndpoint xmlDBProxyEndpoint `xml:"CreateDBProxyEndpointResult>DBProxyEndpoint"`
}

type deleteDBProxyEndpointResponse struct {
	XMLName         xml.Name           `xml:"DeleteDBProxyEndpointResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	DBProxyEndpoint xmlDBProxyEndpoint `xml:"DeleteDBProxyEndpointResult>DBProxyEndpoint"`
}

type describeDBProxyEndpointsResponse struct {
	XMLName          xml.Name               `xml:"DescribeDBProxyEndpointsResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	DBProxyEndpoints xmlDBProxyEndpointList `xml:"DescribeDBProxyEndpointsResult>DBProxyEndpoints"`
}

type modifyDBProxyEndpointResponse struct {
	XMLName         xml.Name           `xml:"ModifyDBProxyEndpointResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	DBProxyEndpoint xmlDBProxyEndpoint `xml:"ModifyDBProxyEndpointResult>DBProxyEndpoint"`
}

type startActivityStreamResponse struct {
	XMLName           xml.Name `xml:"StartActivityStreamResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	KinesisStreamName string   `xml:"StartActivityStreamResult>KinesisStreamName,omitempty"`
	KMSKeyID          string   `xml:"StartActivityStreamResult>KmsKeyId,omitempty"`
	Status            string   `xml:"StartActivityStreamResult>Status,omitempty"`
	Mode              string   `xml:"StartActivityStreamResult>Mode,omitempty"`
	ApplyImmediately  bool     `xml:"StartActivityStreamResult>ApplyImmediately,omitempty"`
}

type stopActivityStreamResponse struct {
	XMLName           xml.Name `xml:"StopActivityStreamResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	KinesisStreamName string   `xml:"StopActivityStreamResult>KinesisStreamName,omitempty"`
	KMSKeyID          string   `xml:"StopActivityStreamResult>KmsKeyId,omitempty"`
	Status            string   `xml:"StopActivityStreamResult>Status,omitempty"`
}

type modifyActivityStreamResponse struct {
	XMLName     xml.Name `xml:"ModifyActivityStreamResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	KMSKeyID    string   `xml:"ModifyActivityStreamResult>KmsKeyId,omitempty"`
	Status      string   `xml:"ModifyActivityStreamResult>Status,omitempty"`
	AuditPolicy string   `xml:"ModifyActivityStreamResult>AuditPolicy,omitempty"`
}
