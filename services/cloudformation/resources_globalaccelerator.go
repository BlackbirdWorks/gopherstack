package cloudformation

import (
	"context"
	"errors"
	"fmt"

	globalacceleratorbackend "github.com/blackbirdworks/gopherstack/services/globalaccelerator"
)

const (
	resTypeGAAccelerator   = "AWS::GlobalAccelerator::Accelerator"
	resTypeGAListener      = "AWS::GlobalAccelerator::Listener"
	resTypeGAEndpointGroup = "AWS::GlobalAccelerator::EndpointGroup"
	resTypeGAAttachment    = "AWS::GlobalAccelerator::CrossAccountAttachment"
)

var errGlobalAcceleratorNotWired = errors.New("global accelerator backend is not available")

func isGlobalAcceleratorCFNType(resourceType string) bool {
	switch resourceType {
	case resTypeGAAccelerator, resTypeGAListener, resTypeGAEndpointGroup, resTypeGAAttachment:
		return true
	}

	return false
}

func (rc *ResourceCreator) globalAcceleratorBackend() *globalacceleratorbackend.InMemoryBackend {
	if rc.backends.GlobalAccelerator == nil {
		return nil
	}

	return rc.backends.GlobalAccelerator.Backend
}

// createGlobalAcceleratorResource provisions the AWS::GlobalAccelerator::* types. Fn::GetAtt values are
// stashed at create time because DNS names and addresses come from the backend.
func (rc *ResourceCreator) createGlobalAcceleratorResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if !isGlobalAcceleratorCFNType(resourceType) {
		return "", false, nil
	}

	bk := rc.globalAcceleratorBackend()
	if bk == nil {
		return "", true, errGlobalAcceleratorNotWired
	}

	resolved, _ := resolveDeep(props, params, physicalIDs).(map[string]any)

	id, attrs, err := bk.CreateCFNResource(resourceType, resolved)
	if err != nil {
		return "", true, err
	}

	if resourceType == resTypeGAAccelerator {
		err = awaitResource("accelerator "+id, func() (bool, error) { return bk.CFNAcceleratorDeployed(id) })
		if err != nil {
			return "", true, err
		}
	}

	for k, v := range attrs {
		physicalIDs[logicalID+"/"+k] = v
	}

	return id, true, nil
}

func (rc *ResourceCreator) deleteGlobalAcceleratorResource(resourceType, physicalID string) (bool, error) {
	if !isGlobalAcceleratorCFNType(resourceType) {
		return false, nil
	}

	bk := rc.globalAcceleratorBackend()
	if bk == nil {
		return true, nil
	}

	return true, bk.DeleteCFNResource(resourceType, physicalID)
}

func (rc *ResourceCreator) updateGlobalAcceleratorResource(
	resourceType, physicalID string, props map[string]any, params, physicalIDs map[string]string,
) error {
	bk := rc.globalAcceleratorBackend()
	if bk == nil {
		return errGlobalAcceleratorNotWired
	}

	resolved, _ := resolveDeep(props, params, physicalIDs).(map[string]any)

	if err := bk.UpdateCFNResource(resourceType, physicalID, resolved); err != nil {
		return fmt.Errorf("update %s: %w", resourceType, err)
	}

	return nil
}

// SetGlobalAccelerator wires the Global Accelerator handler the AWS::GlobalAccelerator::* types provision against.
func (b *InMemoryBackend) SetGlobalAccelerator(h *globalacceleratorbackend.Handler) {
	b.mu.Lock("SetGlobalAccelerator")
	defer b.mu.Unlock()

	b.creator.backends.GlobalAccelerator = h
}

// deleteGlobalAcceleratorThenNetworkManager tries the Global Accelerator types before the Network Manager chain.
func (rc *ResourceCreator) deleteGlobalAcceleratorThenNetworkManager(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteGlobalAcceleratorResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteNetworkManagerThenDirectConnect(ctx, resourceType, physicalID)
}

func (rc *ResourceCreator) createGlobalAcceleratorThenNetworkManager(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createGlobalAcceleratorResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	return rc.createNetworkManagerThenDirectConnect(ctx, logicalID, resourceType, props, params, physicalIDs)
}

func globalAcceleratorRecreation(resourceType, propName string) string {
	switch resourceType {
	case resTypeGAListener:
		return alwaysIfOneOf(propName, "AcceleratorArn")
	case resTypeGAEndpointGroup:
		return alwaysIfOneOf(propName, "ListenerArn", "EndpointGroupRegion")
	}

	return recreateNever
}
