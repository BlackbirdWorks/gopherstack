package cloudformation

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

//go:embed cfn_provisionable_types.json
var cfnProvisionableTypesJSON []byte

//nolint:gochecknoglobals // lazily populated cache of the embedded type list
var (
	cfnProvisionableTypes     []string
	cfnProvisionableTypesOnce sync.Once
)

// ProvisionableTypes lists the AWS:: resource types the stack engine provisions
// against real backends; Cloud Control serves the same set.
func ProvisionableTypes() []string {
	cfnProvisionableTypesOnce.Do(func() {
		if err := json.Unmarshal(cfnProvisionableTypesJSON, &cfnProvisionableTypes); err != nil {
			panic("cloudformation: invalid embedded cfn_provisionable_types.json: " + err.Error())
		}
	})

	return append([]string(nil), cfnProvisionableTypes...)
}

// CloudControlProvisioner drives the stack engine's resource creators for Cloud Control.
type CloudControlProvisioner struct {
	rc        *ResourceCreator
	accountID string
	region    string
}

// CloudControlProvisioner returns the Cloud Control adapter over this backend's resource creators.
func (b *InMemoryBackend) CloudControlProvisioner() *CloudControlProvisioner {
	return &CloudControlProvisioner{rc: b.creator, accountID: b.accountID, region: b.region}
}

func (p *CloudControlProvisioner) context(logicalID, typeName string) map[string]string {
	return map[string]string{
		physIDAccountIDKey:               p.accountID,
		physIDRegionKey:                  p.region,
		physIDResourceTypeKey(logicalID): typeName,
	}
}

func ccLogicalID(typeName string) string {
	parts := strings.Split(typeName, "::")

	return parts[len(parts)-1] + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
}

func ccFailure(err error) error {
	if err == nil || isCloudControlSentinel(err) {
		return err
	}

	return fmt.Errorf("%w: %w", cloudcontrol.ErrValidation, err)
}

func isCloudControlSentinel(err error) bool {
	for _, s := range []error{
		cloudcontrol.ErrValidation, cloudcontrol.ErrUnsupportedAction,
		cloudcontrol.ErrNotUpdatable, cloudcontrol.ErrNotFound, cloudcontrol.ErrAlreadyExists,
	} {
		if errors.Is(err, s) {
			return true
		}
	}

	return false
}

// Provision creates the resource through the same creator CreateStack uses.
func (p *CloudControlProvisioner) Provision(
	ctx context.Context, typeName string, props map[string]any,
) (string, error) {
	if p.rc == nil || p.rc.backends == nil {
		return "", unsupportedCC(typeName, "CREATE")
	}

	logicalID := ccLogicalID(typeName)

	id, err := p.rc.Create(ctx, logicalID, typeName, props, map[string]string{}, p.context(logicalID, typeName))
	if err != nil {
		return "", ccFailure(err)
	}

	if id == logicalID+"-stub" {
		return "", unsupportedCC(typeName, "CREATE")
	}

	return id, nil
}

// Update applies desired in place when the creator has an in-place updater for
// the type; otherwise any real change is NotUpdatable (stacks would replace).
func (p *CloudControlProvisioner) Update(
	_ context.Context, typeName, identifier string, current, desired map[string]any,
) error {
	if reflect.DeepEqual(current, desired) {
		return nil
	}

	handled, err := p.rc.UpdateInPlace(typeName, identifier, desired, map[string]string{}, p.context("", typeName))
	if err != nil {
		return ccFailure(err)
	}

	if !handled {
		return fmt.Errorf("%w: Resource of type '%s' cannot be updated in place; changed properties: [%s]",
			cloudcontrol.ErrNotUpdatable, typeName, strings.Join(changedKeys(current, desired), ", "))
	}

	return nil
}

// Deprovision deletes the resource through the creator CloudFormation deletes with.
func (p *CloudControlProvisioner) Deprovision(
	ctx context.Context, typeName, identifier string, props map[string]any,
) error {
	return ccFailure(p.rc.Delete(ctx, typeName, identifier, props, p.context("", typeName)))
}

func unsupportedCC(typeName, action string) error {
	return fmt.Errorf("%w: Resource type %s does not support %s action",
		cloudcontrol.ErrUnsupportedAction, typeName, action)
}

func changedKeys(current, desired map[string]any) []string {
	set := map[string]struct{}{}

	for k, v := range desired {
		if cv, ok := current[k]; !ok || !reflect.DeepEqual(cv, v) {
			set[k] = struct{}{}
		}
	}

	for k := range current {
		if _, ok := desired[k]; !ok {
			set[k] = struct{}{}
		}
	}

	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}
