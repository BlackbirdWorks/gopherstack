package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	servicediscoverybackend "github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

const (
	cloudMapARNParts       = 6
	cloudMapServicePrefix  = "service/"
	cloudMapRecordSetLimit = 100
)

var errCloudMapUnavailable = errors.New("servicediscovery backend unavailable")

// ecsCloudMap adapts Cloud Map to ecs.CloudMapRegistrar. Cloud Map is resolved at call time,
// never captured in Provider.Init.
type ecsCloudMap struct {
	byName map[string]service.Registerable
}

func (a *ecsCloudMap) backendFor(registryArn string) (servicediscoverybackend.StorageBackend, string, error) {
	parts := strings.SplitN(registryArn, ":", cloudMapARNParts)
	if len(parts) < cloudMapARNParts || parts[2] != "servicediscovery" ||
		!strings.HasPrefix(parts[5], cloudMapServicePrefix) {
		return nil, "", fmt.Errorf(
			"%w: %q is not a Cloud Map service ARN", servicediscoverybackend.ErrInvalidInput, registryArn,
		)
	}

	h, ok := a.byName["ServiceDiscovery"].(*servicediscoverybackend.Handler)
	if !ok {
		return nil, "", errCloudMapUnavailable
	}

	return h.RegionHandler(parts[3]).Backend, strings.TrimPrefix(parts[5], cloudMapServicePrefix), nil
}

func (a *ecsCloudMap) ValidateRegistry(_ context.Context, registryArn string) error {
	bk, id, err := a.backendFor(registryArn)
	if err != nil {
		return err
	}

	_, err = bk.GetService(id)

	return err
}

func (a *ecsCloudMap) RegisterInstance(
	_ context.Context, registryArn, instanceID string, attrs map[string]string,
) error {
	bk, id, err := a.backendFor(registryArn)
	if err != nil {
		return err
	}

	svc, err := bk.GetService(id)
	if err != nil {
		return err
	}

	if svc.HealthCheckCustomConfig != nil {
		attrs["AWS_INIT_HEALTH_STATUS"] = "HEALTHY"
	}

	_, err = bk.RegisterInstance(id, instanceID, attrs)

	return err
}

func (a *ecsCloudMap) DeregisterInstance(_ context.Context, registryArn, instanceID string) error {
	bk, id, err := a.backendFor(registryArn)
	if err != nil {
		return err
	}

	_, err = bk.DeregisterInstance(id, instanceID)
	if errors.Is(err, servicediscoverybackend.ErrInstanceNotFound) ||
		errors.Is(err, servicediscoverybackend.ErrServiceNotFound) {
		return nil
	}

	return err
}

// sdRecordSets adapts Route 53 to servicediscovery.RecordSetWriter.
type sdRecordSets struct {
	backend *route53backend.InMemoryBackend
}

func (a *sdRecordSets) PutRecordSet(zoneID, name, recordType string, ttl int64, values []string) error {
	records := make([]route53backend.ResourceRecord, len(values))
	for i, v := range values {
		records[i] = route53backend.ResourceRecord{Value: v}
	}

	_, err := a.backend.ChangeResourceRecordSets(zoneID, []route53backend.Change{{
		Action:            route53backend.ChangeActionUpsert,
		ResourceRecordSet: route53backend.ResourceRecordSet{Name: name, Type: recordType, TTL: ttl, Records: records},
	}})

	return err
}

func (a *sdRecordSets) DeleteRecordSet(zoneID, name, recordType string) error {
	page, err := a.backend.ListResourceRecordSets(zoneID, name, recordType, "", cloudMapRecordSetLimit)
	if err != nil {
		return err
	}

	for _, rrs := range page.Records {
		if !strings.EqualFold(strings.TrimSuffix(rrs.Name, "."), name) || rrs.Type != recordType {
			continue
		}

		_, err = a.backend.ChangeResourceRecordSets(zoneID, []route53backend.Change{{
			Action: route53backend.ChangeActionDelete, ResourceRecordSet: rrs,
		}})

		return err
	}

	return nil
}

// wireECSCloudMap registers ECS service tasks as Cloud Map instances and keeps DNS namespaces'
// Route 53 record sets in step with them.
func wireECSCloudMap(byName map[string]service.Registerable) {
	if bk := ecsBackendOf(byName); bk != nil {
		bk.SetCloudMapRegistrar(&ecsCloudMap{byName: byName})
	}

	sdH, ok := byName["ServiceDiscovery"].(*servicediscoverybackend.Handler)
	if !ok {
		return
	}

	r53H, ok := byName["Route53"].(*route53backend.Handler)
	if !ok {
		return
	}

	r53Bk, ok := r53H.Backend.(*route53backend.InMemoryBackend)
	if !ok {
		return
	}

	if sdBk, sdOk := sdH.Backend.(*servicediscoverybackend.InMemoryBackend); sdOk {
		sdBk.SetRecordSetWriter(&sdRecordSets{backend: r53Bk})
	}
}
