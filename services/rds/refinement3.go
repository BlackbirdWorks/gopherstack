package rds

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const proxyRandSuffixBytes = 4

// proxyRandSuffix generates an 8-character random hex suffix for proxy endpoints.
func proxyRandSuffix() string {
	buf := make([]byte, proxyRandSuffixBytes)
	if _, err := rand.Read(buf); err != nil {
		return "00000000"
	}

	return hex.EncodeToString(buf)
}

// ---- DB Proxy types ----

// DBProxy represents an RDS DB proxy.
type DBProxy struct {
	CreatedDate          time.Time            `json:"createdDate"`
	UpdatedDate          time.Time            `json:"updatedDate"`
	RoleARN              string               `json:"roleArn"`
	Status               string               `json:"status"`
	Endpoint             string               `json:"endpoint"`
	EngineFamily         string               `json:"engineFamily"`
	DBProxyARN           string               `json:"dbProxyArn"`
	DBProxyName          string               `json:"dbProxyName"`
	VpcSecurityGroupIDs  []string             `json:"vpcSecurityGroupIds"`
	Auth                 []UserAuthConfig     `json:"auth"`
	VpcSubnetIDs         []string             `json:"vpcSubnetIds"`
	ConnectionPoolConfig ConnectionPoolConfig `json:"connectionPoolConfig"`
	IdleClientTimeout    int                  `json:"idleClientTimeout"`
	DebugLogging         bool                 `json:"debugLogging"`
	RequireTLS           bool                 `json:"requireTls"`
}

// UserAuthConfig holds authentication details for a DB proxy.
type UserAuthConfig struct {
	AuthScheme  string `json:"authScheme"`
	Description string `json:"description"`
	IAMAuth     string `json:"iamAuth"`
	SecretARN   string `json:"secretArn"`
	UserName    string `json:"userName"`
}

// ConnectionPoolConfig holds connection pooling parameters for a proxy target group.
type ConnectionPoolConfig struct {
	InitQuery                 string   `json:"initQuery"`
	SessionPinningFilters     []string `json:"sessionPinningFilters"`
	ConnectionBorrowTimeout   int      `json:"connectionBorrowTimeout"`
	MaxConnectionsPercent     int      `json:"maxConnectionsPercent"`
	MaxIdleConnectionsPercent int      `json:"maxIdleConnectionsPercent"`
}

// DBProxyTarget represents a single target within a DB proxy target group.
type DBProxyTarget struct {
	TargetARN        string `json:"targetArn"`
	Endpoint         string `json:"endpoint"`
	TrackedClusterID string `json:"trackedClusterId"`
	RdsResourceID    string `json:"rdsResourceId"`
	Type             string `json:"type"`
	Role             string `json:"role"`
	TargetHealth     string `json:"targetHealth"`
	Port             int    `json:"port"`
}

// DBProxyTargetGroup represents a group of targets for a DB proxy.
type DBProxyTargetGroup struct {
	CreatedDate          time.Time            `json:"createdDate"`
	UpdatedDate          time.Time            `json:"updatedDate"`
	DBProxyName          string               `json:"dbProxyName"`
	TargetGroupName      string               `json:"targetGroupName"`
	TargetGroupARN       string               `json:"targetGroupArn"`
	Status               string               `json:"status"`
	ConnectionPoolConfig ConnectionPoolConfig `json:"connectionPoolConfig"`
	IsDefault            bool                 `json:"isDefault"`
}

// DBProxyEndpoint represents a custom endpoint for a DB proxy.
type DBProxyEndpoint struct {
	CreatedDate         time.Time `json:"createdDate"`
	DBProxyEndpointName string    `json:"dbProxyEndpointName"`
	DBProxyEndpointARN  string    `json:"dbProxyEndpointArn"`
	DBProxyName         string    `json:"dbProxyName"`
	Status              string    `json:"status"`
	VpcID               string    `json:"vpcId"`
	Endpoint            string    `json:"endpoint"`
	TargetRole          string    `json:"targetRole"`
	VpcSecurityGroupIDs []string  `json:"vpcSecurityGroupIds"`
	VpcSubnetIDs        []string  `json:"vpcSubnetIds"`
	IsDefault           bool      `json:"isDefault"`
}

// ---- DB Proxy backend operations ----

const (
	proxyDefaultTargetGroupName       = "default"
	proxyDefaultIdleClientTimeout     = 1800
	proxyDefaultMaxConnectionsPct     = 100
	proxyDefaultMaxIdleConnectionsPct = 50
	proxyDefaultBorrowTimeout         = 120
	proxyDefaultPort                  = 3306
	activityStreamStatusStarted       = "started"
)

var (
	// ErrDBProxyAlreadyExists is returned when a DB proxy with the same name already exists.
	ErrDBProxyAlreadyExists = errors.New("DBProxyAlreadyExists")
	// ErrDBProxyEndpointAlreadyExists is returned when a DB proxy endpoint with the same name already exists.
	ErrDBProxyEndpointAlreadyExists = errors.New("DBProxyEndpointAlreadyExists")
	// ErrCannotDeleteDefaultProxyEndpoint is returned when attempting to delete a default proxy endpoint.
	ErrCannotDeleteDefaultProxyEndpoint = errors.New("InvalidDBProxyEndpointStateFault")
	// ErrActivityStreamAlreadyStarted is returned when the activity stream is already started.
	ErrActivityStreamAlreadyStarted = errors.New("InvalidDBClusterStateFault")
	// ErrActivityStreamNotStarted is returned when the activity stream is not started.
	ErrActivityStreamNotStarted = errors.New("InvalidDBClusterStateFault")
)

// CreateDBProxy creates a new RDS DB proxy.
func (b *InMemoryBackend) CreateDBProxy(name, engineFamily, roleARN string, auth []UserAuthConfig) (*DBProxy, error) {
	b.mu.Lock("CreateDBProxy")
	defer b.mu.Unlock()

	if _, exists := b.proxies[name]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDBProxyAlreadyExists, name)
	}

	proxy := &DBProxy{
		DBProxyName:       name,
		DBProxyARN:        arn.Build("rds", b.region, b.accountID, fmt.Sprintf("db-proxy:prx-%s", name)),
		Status:            instanceStatusAvailable,
		Endpoint:          fmt.Sprintf("%s.proxy-%s.%s.rds.amazonaws.com", name, proxyRandSuffix(), b.region),
		EngineFamily:      engineFamily,
		RoleARN:           roleARN,
		Auth:              auth,
		IdleClientTimeout: proxyDefaultIdleClientTimeout,
		CreatedDate:       time.Now(),
		UpdatedDate:       time.Now(),
		ConnectionPoolConfig: ConnectionPoolConfig{
			MaxConnectionsPercent:     proxyDefaultMaxConnectionsPct,
			MaxIdleConnectionsPercent: proxyDefaultMaxIdleConnectionsPct,
			ConnectionBorrowTimeout:   proxyDefaultBorrowTimeout,
		},
	}

	b.proxies[name] = proxy

	// Auto-create default target group
	tg := &DBProxyTargetGroup{
		DBProxyName:          name,
		TargetGroupName:      proxyDefaultTargetGroupName,
		TargetGroupARN:       arn.Build("rds", b.region, b.accountID, fmt.Sprintf("target-group:%s/default", name)),
		IsDefault:            true,
		Status:               instanceStatusAvailable,
		CreatedDate:          time.Now(),
		UpdatedDate:          time.Now(),
		ConnectionPoolConfig: proxy.ConnectionPoolConfig,
	}
	b.proxyTargetGroups[name+"/"+proxyDefaultTargetGroupName] = tg

	return proxy, nil
}

// DeleteDBProxy deletes a DB proxy.
func (b *InMemoryBackend) DeleteDBProxy(name string) (*DBProxy, error) {
	b.mu.Lock("DeleteDBProxy")
	defer b.mu.Unlock()

	proxy, exists := b.proxies[name]
	if !exists {
		return nil, fmt.Errorf("%w: DBProxy %s not found", ErrInvalidParameter, name)
	}

	delete(b.proxies, name)

	// Clean up associated target groups and targets
	for key := range b.proxyTargetGroups {
		if strings.HasPrefix(key, name+"/") {
			delete(b.proxyTargetGroups, key)
		}
	}
	delete(b.proxyTargets, name)
	for key := range b.proxyEndpoints {
		if b.proxyEndpoints[key].DBProxyName == name {
			delete(b.proxyEndpoints, key)
		}
	}

	return proxy, nil
}

// DescribeDBProxies returns DB proxies, optionally filtered by name.
func (b *InMemoryBackend) DescribeDBProxies(name string) ([]DBProxy, error) {
	b.mu.RLock("DescribeDBProxies")
	defer b.mu.RUnlock()

	result := make([]DBProxy, 0, len(b.proxies))
	for _, p := range b.proxies {
		if name != "" && p.DBProxyName != name {
			continue
		}
		result = append(result, *p)
	}

	if name != "" && len(result) == 0 {
		return nil, fmt.Errorf("%w: DBProxy %s not found", ErrInvalidParameter, name)
	}

	return result, nil
}

// ModifyDBProxy modifies a DB proxy's settings.
func (b *InMemoryBackend) ModifyDBProxy(
	name string,
	requireTLS *bool,
	idleClientTimeout *int,
	auth []UserAuthConfig,
) (*DBProxy, error) {
	b.mu.Lock("ModifyDBProxy")
	defer b.mu.Unlock()

	proxy, exists := b.proxies[name]
	if !exists {
		return nil, fmt.Errorf("%w: DBProxy %s not found", ErrInvalidParameter, name)
	}

	if requireTLS != nil {
		proxy.RequireTLS = *requireTLS
	}
	if idleClientTimeout != nil {
		proxy.IdleClientTimeout = *idleClientTimeout
	}
	if len(auth) > 0 {
		proxy.Auth = auth
	}
	proxy.UpdatedDate = time.Now()
	cp := *proxy

	return &cp, nil
}

// ---- DB Proxy Target operations ----

// RegisterDBProxyTargets registers DB instances or clusters as targets for a proxy.
func (b *InMemoryBackend) RegisterDBProxyTargets(
	proxyName, _ string,
	dbInstanceIDs, dbClusterIDs []string,
) ([]DBProxyTarget, error) {
	b.mu.Lock("RegisterDBProxyTargets")
	defer b.mu.Unlock()

	if _, exists := b.proxies[proxyName]; !exists {
		return nil, fmt.Errorf("%w: DBProxy %s not found", ErrInvalidParameter, proxyName)
	}

	if b.proxyTargets[proxyName] == nil {
		b.proxyTargets[proxyName] = make([]DBProxyTarget, 0)
	}

	targets := make([]DBProxyTarget, 0, len(dbInstanceIDs)+len(dbClusterIDs))

	for _, id := range dbInstanceIDs {
		target := DBProxyTarget{
			TargetARN:     arn.Build("rds", b.region, b.accountID, fmt.Sprintf("db:%s", id)),
			Endpoint:      fmt.Sprintf("%s.%s.rds.amazonaws.com", id, b.region),
			RdsResourceID: id,
			Port:          proxyDefaultPort,
			Type:          "RDS_INSTANCE",
			Role:          clusterEndpointReadWrite,
			TargetHealth:  "AVAILABLE",
		}
		targets = append(targets, target)
		b.proxyTargets[proxyName] = append(b.proxyTargets[proxyName], target)
	}

	for _, id := range dbClusterIDs {
		target := DBProxyTarget{
			TargetARN:        arn.Build("rds", b.region, b.accountID, fmt.Sprintf("cluster:%s", id)),
			Endpoint:         fmt.Sprintf("%s.cluster-%s.%s.rds.amazonaws.com", id, b.accountID[:8], b.region),
			TrackedClusterID: id,
			RdsResourceID:    id,
			Port:             proxyDefaultPort,
			Type:             "TRACKED_CLUSTER",
			Role:             clusterEndpointReadWrite,
			TargetHealth:     "AVAILABLE",
		}
		targets = append(targets, target)
		b.proxyTargets[proxyName] = append(b.proxyTargets[proxyName], target)
	}

	return targets, nil
}

// DeregisterDBProxyTargets removes targets from a DB proxy.
func (b *InMemoryBackend) DeregisterDBProxyTargets(
	proxyName, _ string,
	dbInstanceIDs, dbClusterIDs []string,
) error {
	b.mu.Lock("DeregisterDBProxyTargets")
	defer b.mu.Unlock()

	if _, exists := b.proxies[proxyName]; !exists {
		return fmt.Errorf("%w: DBProxy %s not found", ErrInvalidParameter, proxyName)
	}

	removeIDs := make(map[string]bool)
	for _, id := range dbInstanceIDs {
		removeIDs[id] = true
	}
	for _, id := range dbClusterIDs {
		removeIDs[id] = true
	}

	existing := b.proxyTargets[proxyName]
	updated := make([]DBProxyTarget, 0, len(existing))
	for _, t := range existing {
		if !removeIDs[t.RdsResourceID] {
			updated = append(updated, t)
		}
	}
	b.proxyTargets[proxyName] = updated

	return nil
}

// DescribeDBProxyTargets returns the targets for a DB proxy.
func (b *InMemoryBackend) DescribeDBProxyTargets(proxyName, _ string) ([]DBProxyTarget, error) {
	b.mu.RLock("DescribeDBProxyTargets")
	defer b.mu.RUnlock()

	if _, exists := b.proxies[proxyName]; !exists {
		return nil, fmt.Errorf("%w: DBProxy %s not found", ErrInvalidParameter, proxyName)
	}

	targets := b.proxyTargets[proxyName]
	if targets == nil {
		return []DBProxyTarget{}, nil
	}

	return targets, nil
}

// DescribeDBProxyTargetGroups returns target groups for a DB proxy.
func (b *InMemoryBackend) DescribeDBProxyTargetGroups(proxyName, targetGroupName string) ([]DBProxyTargetGroup, error) {
	b.mu.RLock("DescribeDBProxyTargetGroups")
	defer b.mu.RUnlock()

	if _, exists := b.proxies[proxyName]; !exists {
		return nil, fmt.Errorf("%w: DBProxy %s not found", ErrInvalidParameter, proxyName)
	}

	result := make([]DBProxyTargetGroup, 0, len(b.proxyTargetGroups))
	for key, tg := range b.proxyTargetGroups {
		if !strings.HasPrefix(key, proxyName+"/") {
			continue
		}
		if targetGroupName != "" && tg.TargetGroupName != targetGroupName {
			continue
		}
		result = append(result, *tg)
	}

	return result, nil
}

// ModifyDBProxyTargetGroup modifies the connection pool settings for a proxy target group.
func (b *InMemoryBackend) ModifyDBProxyTargetGroup(
	proxyName, targetGroupName string,
	cfg ConnectionPoolConfig,
) (*DBProxyTargetGroup, error) {
	b.mu.Lock("ModifyDBProxyTargetGroup")
	defer b.mu.Unlock()

	key := proxyName + "/" + targetGroupName
	tg, exists := b.proxyTargetGroups[key]
	if !exists {
		return nil, fmt.Errorf(
			"%w: target group %s for proxy %s not found",
			ErrInvalidParameter,
			targetGroupName,
			proxyName,
		)
	}

	tg.ConnectionPoolConfig = cfg
	tg.UpdatedDate = time.Now()

	return tg, nil
}

// ---- DB Proxy Endpoint operations ----

// CreateDBProxyEndpoint creates a custom endpoint for a DB proxy.
func (b *InMemoryBackend) CreateDBProxyEndpoint(
	proxyName, endpointName, targetRole string,
	vpcSubnetIDs, vpcSGIDs []string,
) (*DBProxyEndpoint, error) {
	b.mu.Lock("CreateDBProxyEndpoint")
	defer b.mu.Unlock()

	if _, exists := b.proxies[proxyName]; !exists {
		return nil, fmt.Errorf("%w: DBProxy %s not found", ErrInvalidParameter, proxyName)
	}
	if _, exists := b.proxyEndpoints[endpointName]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDBProxyEndpointAlreadyExists, endpointName)
	}

	if targetRole == "" {
		targetRole = clusterEndpointReadWrite
	}

	ep := &DBProxyEndpoint{
		DBProxyEndpointName: endpointName,
		DBProxyEndpointARN:  arn.Build("rds", b.region, b.accountID, fmt.Sprintf("db-proxy-endpoint:%s", endpointName)),
		DBProxyName:         proxyName,
		Status:              instanceStatusAvailable,
		Endpoint: fmt.Sprintf(
			"%s.endpoint.proxy-%s.%s.rds.amazonaws.com",
			endpointName, proxyRandSuffix(), b.region,
		),
		TargetRole:          targetRole,
		VpcSubnetIDs:        vpcSubnetIDs,
		VpcSecurityGroupIDs: vpcSGIDs,
		IsDefault:           false,
		CreatedDate:         time.Now(),
	}

	b.proxyEndpoints[endpointName] = ep

	return ep, nil
}

// DeleteDBProxyEndpoint deletes a custom proxy endpoint.
func (b *InMemoryBackend) DeleteDBProxyEndpoint(endpointName string) (*DBProxyEndpoint, error) {
	b.mu.Lock("DeleteDBProxyEndpoint")
	defer b.mu.Unlock()

	ep, exists := b.proxyEndpoints[endpointName]
	if !exists {
		return nil, fmt.Errorf("%w: DBProxyEndpoint %s not found", ErrInvalidParameter, endpointName)
	}
	if ep.IsDefault {
		return nil, ErrCannotDeleteDefaultProxyEndpoint
	}
	delete(b.proxyEndpoints, endpointName)

	return ep, nil
}

// DescribeDBProxyEndpoints returns proxy endpoints, optionally filtered.
func (b *InMemoryBackend) DescribeDBProxyEndpoints(proxyName, endpointName string) ([]DBProxyEndpoint, error) {
	b.mu.RLock("DescribeDBProxyEndpoints")
	defer b.mu.RUnlock()

	result := make([]DBProxyEndpoint, 0, len(b.proxyEndpoints))
	for _, ep := range b.proxyEndpoints {
		if proxyName != "" && ep.DBProxyName != proxyName {
			continue
		}
		if endpointName != "" && ep.DBProxyEndpointName != endpointName {
			continue
		}
		result = append(result, *ep)
	}

	if endpointName != "" && len(result) == 0 {
		return nil, fmt.Errorf("%w: DBProxyEndpoint %s not found", ErrInvalidParameter, endpointName)
	}

	return result, nil
}

// ModifyDBProxyEndpoint modifies a proxy endpoint's settings.
func (b *InMemoryBackend) ModifyDBProxyEndpoint(endpointName string, vpcSGIDs []string) (*DBProxyEndpoint, error) {
	b.mu.Lock("ModifyDBProxyEndpoint")
	defer b.mu.Unlock()

	ep, exists := b.proxyEndpoints[endpointName]
	if !exists {
		return nil, fmt.Errorf("%w: DBProxyEndpoint %s not found", ErrInvalidParameter, endpointName)
	}
	if len(vpcSGIDs) > 0 {
		ep.VpcSecurityGroupIDs = vpcSGIDs
	}

	return ep, nil
}

// ---- Activity Stream operations ----

// StartActivityStream starts the database activity stream for a DB cluster.
func (b *InMemoryBackend) StartActivityStream(clusterID, kmsKeyID, mode string) (*DBCluster, error) {
	b.mu.Lock("StartActivityStream")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: DBCluster %s not found", ErrInvalidParameter, clusterID)
	}

	if cluster.ActivityStreamStatus == activityStreamStatusStarted {
		return nil, fmt.Errorf(
			"%w: already started for cluster %s",
			ErrActivityStreamAlreadyStarted,
			clusterID,
		)
	}

	if mode == "" {
		mode = "async"
	}
	cluster.ActivityStreamStatus = activityStreamStatusStarted
	cluster.ActivityStreamMode = mode
	cluster.ActivityStreamKMSKeyID = kmsKeyID
	cluster.ActivityStreamKinesisStreamName = fmt.Sprintf("aws-rds-das-%s-%s", b.region, clusterID)

	return cluster, nil
}

// StopActivityStream stops the database activity stream for a DB cluster.
func (b *InMemoryBackend) StopActivityStream(clusterID string) (*DBCluster, error) {
	b.mu.Lock("StopActivityStream")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: DBCluster %s not found", ErrInvalidParameter, clusterID)
	}

	if cluster.ActivityStreamStatus != activityStreamStatusStarted {
		return nil, fmt.Errorf(
			"%w: activity stream is not started for cluster %s",
			ErrActivityStreamNotStarted,
			clusterID,
		)
	}

	cluster.ActivityStreamStatus = "stopped"
	cluster.ActivityStreamMode = ""
	cluster.ActivityStreamKMSKeyID = ""
	cluster.ActivityStreamKinesisStreamName = ""

	return cluster, nil
}

// ModifyActivityStream modifies activity stream settings for a DB cluster.
func (b *InMemoryBackend) ModifyActivityStream(clusterID string, auditPolicy string) (*DBCluster, error) {
	b.mu.Lock("ModifyActivityStream")
	defer b.mu.Unlock()

	cluster, exists := b.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: DBCluster %s not found", ErrInvalidParameter, clusterID)
	}

	if cluster.ActivityStreamStatus != activityStreamStatusStarted {
		return nil, fmt.Errorf(
			"%w: activity stream must be started to modify it for cluster %s",
			ErrActivityStreamNotStarted,
			clusterID,
		)
	}

	cluster.ActivityStreamAuditPolicy = auditPolicy

	return cluster, nil
}
