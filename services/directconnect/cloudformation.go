package directconnect

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode"
)

// CloudFormation resource types provisioned through CreateCFNResource.
const (
	CFNConnection                = "AWS::DirectConnect::Connection"
	CFNLag                       = "AWS::DirectConnect::Lag"
	CFNDirectConnectGateway      = "AWS::DirectConnect::DirectConnectGateway"
	CFNGatewayAssociation        = "AWS::DirectConnect::DirectConnectGatewayAssociation"
	CFNPrivateVirtualInterface   = "AWS::DirectConnect::PrivateVirtualInterface"
	CFNPublicVirtualInterface    = "AWS::DirectConnect::PublicVirtualInterface"
	CFNTransitVirtualInterface   = "AWS::DirectConnect::TransitVirtualInterface"
	cfnConnectionIDKey           = "ConnectionId"
	cfnNestedPrivateVIF          = "NewPrivateVirtualInterface"
	cfnNestedPublicVIF           = "NewPublicVirtualInterface"
	cfnNestedTransitVIF          = "NewTransitVirtualInterface"
	cfnLagNumberOfConnectionsKey = "NumberOfConnections"
	cfnLagMinimumLinksKey        = "MinimumLinks"
)

// ErrCFNDeletePending means deletion is under way (a LAG's member connections are still deleting); call
// DeleteCFNResource again once they settle.
var ErrCFNDeletePending = errors.New("deletion pending on dependent resources")

var errUnknownCFNType = clientError("unsupported CloudFormation resource type")

func clampInt32(n int64) int32 {
	return int32(min(max(n, math.MinInt32), math.MaxInt32))
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}

	r := []rune(s)
	r[0] = unicode.ToLower(r[0])

	return string(r)
}

// lowerKeys rewrites CloudFormation PascalCase property names to the camelCase wire names.
func lowerKeys(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[lowerFirst(k)] = lowerKeys(val)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = lowerKeys(val)
		}

		return out
	}

	return v
}

// coerceNumbers turns numeric strings (Number template parameters resolve to strings) into numbers.
func coerceNumbers(m map[string]any, keys ...string) {
	for _, k := range keys {
		s, ok := m[k].(string)
		if !ok {
			continue
		}

		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			m[k] = n
		}
	}
}

func decodeStrict(m map[string]any, out any) error {
	raw, err := json.Marshal(lowerKeys(m))
	if err != nil {
		return clientError(err.Error())
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	if err = dec.Decode(out); err != nil {
		return clientError("invalid properties: " + err.Error())
	}

	return nil
}

func vifProps(props map[string]any, nestedKey string) (string, map[string]any) {
	connID, _ := props[cfnConnectionIDKey].(string)

	if nested, ok := props[nestedKey].(map[string]any); ok {
		return connID, nested
	}

	flat := make(map[string]any, len(props))

	for k, v := range props {
		if k != cfnConnectionIDKey {
			flat[k] = v
		}
	}

	return connID, flat
}

// CreateCFNResource provisions the Direct Connect resource a CloudFormation type describes and returns its
// ID (the Ref value) and read-only attributes. Property names are the CloudFormation spellings; a virtual
// interface's settings may be given flat or under NewPrivate/Public/TransitVirtualInterface.
func (b *InMemoryBackend) CreateCFNResource(
	resourceType string,
	props map[string]any,
) (string, map[string]string, error) {
	switch resourceType {
	case CFNConnection:
		return b.createCFNConnection(props)
	case CFNLag:
		return b.createCFNLag(props)
	case CFNDirectConnectGateway:
		return b.createCFNGateway(props)
	case CFNGatewayAssociation:
		return b.createCFNAssociation(props)
	case CFNPrivateVirtualInterface, CFNPublicVirtualInterface, CFNTransitVirtualInterface:
		return b.createCFNVirtualInterface(resourceType, props)
	}

	return "", nil, fmt.Errorf("%w: %s", errUnknownCFNType, resourceType)
}

func (b *InMemoryBackend) createCFNConnection(props map[string]any) (string, map[string]string, error) {
	var req createConnectionRequest
	if err := decodeStrict(props, &req); err != nil {
		return "", nil, err
	}

	c, err := b.CreateConnection(&req)
	if err != nil {
		return "", nil, err
	}

	return c.ConnectionID, map[string]string{
		"ConnectionId": c.ConnectionID, "ConnectionArn": b.ConnectionARN(c.ConnectionID),
		"ConnectionState": c.ConnectionState,
	}, nil
}

func (b *InMemoryBackend) createCFNLag(props map[string]any) (string, map[string]string, error) {
	coerceNumbers(props, cfnLagNumberOfConnectionsKey, cfnLagMinimumLinksKey)

	minimum, _ := props[cfnLagMinimumLinksKey].(int64)
	_, hasNumber := props[cfnLagNumberOfConnectionsKey].(int64)

	rest := make(map[string]any, len(props))

	for k, v := range props {
		if k != cfnLagMinimumLinksKey {
			rest[k] = v
		}
	}

	var req createLagRequest
	if err := decodeStrict(rest, &req); err != nil {
		return "", nil, err
	}

	if !hasNumber {
		req.NumberOfConnections = max(clampInt32(minimum), 1)
	}

	l, err := b.CreateLag(&req)
	if err != nil {
		return "", nil, err
	}

	if minimum > 0 {
		if l, err = b.UpdateLag(&updateLagRequest{LagID: l.LagID, MinimumLinks: clampInt32(minimum)}); err != nil {
			return "", nil, err
		}
	}

	return l.LagID, map[string]string{
		"LagId": l.LagID, "LagArn": b.LagARN(l.LagID), "LagState": l.LagState,
	}, nil
}

func (b *InMemoryBackend) createCFNGateway(props map[string]any) (string, map[string]string, error) {
	coerceNumbers(props, "AmazonSideAsn")

	var req createDirectConnectGatewayRequest
	if err := decodeStrict(props, &req); err != nil {
		return "", nil, err
	}

	g, err := b.CreateDirectConnectGateway(&req)
	if err != nil {
		return "", nil, err
	}

	return g.DirectConnectGatewayID, map[string]string{
		"DirectConnectGatewayId":  g.DirectConnectGatewayID,
		"DirectConnectGatewayArn": b.GatewayARN(g.DirectConnectGatewayID),
	}, nil
}

func (b *InMemoryBackend) createCFNAssociation(props map[string]any) (string, map[string]string, error) {
	var req createGatewayAssociationRequest
	if err := decodeStrict(props, &req); err != nil {
		return "", nil, err
	}

	a, err := b.CreateDirectConnectGatewayAssociation(&req)
	if err != nil {
		return "", nil, err
	}

	return a.AssociationID, map[string]string{"AssociationId": a.AssociationID}, nil
}

func (b *InMemoryBackend) createCFNVirtualInterface(
	resourceType string, props map[string]any,
) (string, map[string]string, error) {
	var (
		vif *VirtualInterface
		err error
	)

	switch resourceType {
	case CFNPrivateVirtualInterface:
		connID, flat := vifProps(props, cfnNestedPrivateVIF)
		coerceNumbers(flat, "Vlan", "Asn", "Mtu", "AsnLong")

		var n newPrivateVifWire
		if err = decodeStrict(flat, &n); err == nil {
			vif, err = b.CreatePrivateVirtualInterface(connID, &n)
		}
	case CFNPublicVirtualInterface:
		connID, flat := vifProps(props, cfnNestedPublicVIF)
		coerceNumbers(flat, "Vlan", "Asn", "AsnLong")

		var n newPublicVifWire
		if err = decodeStrict(flat, &n); err == nil {
			vif, err = b.CreatePublicVirtualInterface(connID, &n)
		}
	default:
		connID, flat := vifProps(props, cfnNestedTransitVIF)
		coerceNumbers(flat, "Vlan", "Asn", "Mtu", "AsnLong")

		var n newTransitVifWire
		if err = decodeStrict(flat, &n); err == nil {
			vif, err = b.CreateTransitVirtualInterface(connID, &n)
		}
	}

	if err != nil {
		return "", nil, err
	}

	return vif.VirtualInterfaceID, map[string]string{
		"VirtualInterfaceId": vif.VirtualInterfaceID, "VirtualInterfaceArn": b.VifARN(vif.VirtualInterfaceID),
	}, nil
}

// CFNResourceState reports the lifecycle state of a CloudFormation-provisioned resource; ok is false once it
// no longer exists.
func (b *InMemoryBackend) CFNResourceState(resourceType, id string) (string, bool) {
	switch resourceType {
	case CFNConnection:
		if cs := b.DescribeConnections(id); len(cs) > 0 {
			return cs[0].ConnectionState, true
		}
	case CFNLag:
		if ls := b.DescribeLags(id); len(ls) > 0 {
			return ls[0].LagState, true
		}
	case CFNDirectConnectGateway:
		if gs := b.DescribeDirectConnectGateways(id); len(gs) > 0 {
			return gs[0].DirectConnectGatewayState, true
		}
	case CFNGatewayAssociation:
		return b.associationState(id)
	case CFNPrivateVirtualInterface, CFNPublicVirtualInterface, CFNTransitVirtualInterface:
		if vs := b.DescribeVirtualInterfaces("", id); len(vs) > 0 {
			return vs[0].VirtualInterfaceState, true
		}
	}

	return "", false
}

func (b *InMemoryBackend) associationState(id string) (string, bool) {
	b.mu.RLock("associationState")
	defer b.mu.RUnlock()

	a, ok := b.associations.Get(id)
	if !ok {
		return "", false
	}

	return a.AssociationState, true
}

// DeleteCFNResource starts deletion of a CloudFormation-provisioned resource; a LAG deletes its member
// connections first and returns ErrCFNDeletePending until they are gone. Use CFNResourceState to observe completion.
func (b *InMemoryBackend) DeleteCFNResource(resourceType, id string) error {
	var err error

	switch resourceType {
	case CFNConnection:
		_, err = b.DeleteConnection(id)
	case CFNLag:
		err = b.deleteCFNLag(id)
	case CFNDirectConnectGateway:
		_, err = b.DeleteDirectConnectGateway(id)
	case CFNGatewayAssociation:
		_, err = b.DeleteDirectConnectGatewayAssociation(&deleteGatewayAssociationRequest{AssociationID: id})
	case CFNPrivateVirtualInterface, CFNPublicVirtualInterface, CFNTransitVirtualInterface:
		_, err = b.DeleteVirtualInterface(id)
	default:
		return fmt.Errorf("%w: %s", errUnknownCFNType, resourceType)
	}

	return err
}

func (b *InMemoryBackend) deleteCFNLag(id string) error {
	b.mu.RLock("deleteCFNLag")

	var active, deleting []string

	for _, c := range b.connectionsByLagLocked(id) {
		switch c.ConnectionState {
		case ConnectionStateDeleted, ConnectionStateRejected:
		case ConnectionStateDeleting:
			deleting = append(deleting, c.ConnectionID)
		default:
			active = append(active, c.ConnectionID)
		}
	}

	b.mu.RUnlock()

	for _, m := range active {
		if _, err := b.DeleteConnection(m); err != nil {
			return err
		}
	}

	if len(active)+len(deleting) > 0 {
		return ErrCFNDeletePending
	}

	_, err := b.DeleteLag(id)

	return err
}
