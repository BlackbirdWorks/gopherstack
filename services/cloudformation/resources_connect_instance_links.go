package cloudformation

import (
	"fmt"
	"strconv"
	"strings"

	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

// connectCompositeID joins the parts of a composite physical ID, which is not an ARN so that GetAtt reads the
// attributes stashed at create time.
// connectCompositeParts is the part count of storage config and data table attribute IDs.
const connectCompositeParts = 3

func connectCompositeID(parts ...string) string { return strings.Join(parts, "|") }

// ---- ApprovedOrigin ----

func createConnectApprovedOrigin(c *connectCall) (string, error) {
	inst, origin := lastPathSegment(c.instance()), c.str("Origin")
	if err := c.be.AssociateApprovedOrigin(inst, origin); err != nil {
		return "", err
	}

	return connectCompositeID(inst, origin), nil
}

func deleteConnectApprovedOrigin(c *connectCall, physicalID string) error {
	inst, origin, _ := strings.Cut(physicalID, "|")

	return c.be.DisassociateApprovedOrigin(inst, origin)
}

// ---- SecurityKey ----

// createConnectSecurityKey associates a key. Ref is "<instance ID>|<association ID>" and GetAtt AssociationId
// the association ID.
func createConnectSecurityKey(c *connectCall) (string, error) {
	inst := lastPathSegment(c.instance())

	id, err := c.be.AssociateSecurityKey(inst, c.str("Key"))
	if err != nil {
		return "", err
	}

	c.stash(map[string]string{"AssociationId": id})

	return connectCompositeID(inst, id), nil
}

func deleteConnectSecurityKey(c *connectCall, physicalID string) error {
	inst, id, _ := strings.Cut(physicalID, "|")

	return c.be.DisassociateSecurityKey(inst, id)
}

// ---- InstanceStorageConfig ----

func (c *connectCall) storageConfig() (*connectbackend.InstanceStorageConfig, error) {
	var cfg connectbackend.InstanceStorageConfig
	if err := c.decode(&cfg, "ResourceType"); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// createConnectStorageConfig associates a storage config. Ref is "<instance ID>|<resource type>|<association ID>".
func createConnectStorageConfig(c *connectCall) (string, error) {
	cfg, err := c.storageConfig()
	if err != nil {
		return "", err
	}

	inst, resourceType := lastPathSegment(c.instance()), c.str("ResourceType")

	id, err := c.be.AssociateInstanceStorageConfig(c.ctx, inst, resourceType, cfg)
	if err != nil {
		return "", err
	}

	c.stash(map[string]string{"AssociationId": id})

	return connectCompositeID(inst, resourceType, id), nil
}

func updateConnectStorageConfig(c *connectCall, physicalID string) error {
	cfg, err := c.storageConfig()
	if err != nil {
		return err
	}

	parts := strings.Split(physicalID, "|")
	if len(parts) != connectCompositeParts {
		return fmt.Errorf("%w: storage config ID %q", errUnsupportedCFNValue, physicalID)
	}

	return c.be.UpdateInstanceStorageConfig(c.ctx, parts[0], parts[1], parts[2], cfg)
}

func deleteConnectStorageConfig(c *connectCall, physicalID string) error {
	parts := strings.Split(physicalID, "|")
	if len(parts) != connectCompositeParts {
		return nil
	}

	return c.be.DisassociateInstanceStorageConfig(parts[0], parts[1], parts[2])
}

// ---- IntegrationAssociation ----

func createConnectIntegrationAssociation(c *connectCall) (string, error) {
	var in connectbackend.CreateIntegrationAssociationInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateIntegrationAssociation(c.ctx, &in)
	if err != nil {
		return "", err
	}

	return out.IntegrationAssociationArn, nil
}

func updateConnectIntegrationAssociation(c *connectCall, physicalID string) error {
	return c.syncTags(physicalID)
}

func deleteConnectIntegrationAssociation(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteIntegrationAssociation(c.ctx, inst, id)

	return err
}

// ---- UseCase ----

func createConnectUseCase(c *connectCall) (string, error) {
	out, err := c.be.CreateUseCase(c.ctx, &connectbackend.CreateUseCaseInput{
		UseCaseType:   c.str("UseCaseType"),
		AssociationID: c.str("IntegrationAssociationId"),
		Tags:          c.tags(),
		InstanceID:    c.instance(),
	})
	if err != nil {
		return "", err
	}

	return out.UseCaseArn, nil
}

func updateConnectUseCase(c *connectCall, physicalID string) error { return c.syncTags(physicalID) }

func deleteConnectUseCase(c *connectCall, physicalID string) error {
	inst, _ := splitConnectARN(physicalID)
	assoc := between(physicalID, "/integration-association/", "/use-case/")
	_, err := c.be.DeleteUseCase(inst, assoc, lastPathSegment(physicalID))

	return err
}

func between(s, start, end string) string {
	_, after, _ := strings.Cut(s, start)
	before, _, _ := strings.Cut(after, end)

	return before
}

// ---- TrafficDistributionGroup ----

func createConnectTrafficDistributionGroup(c *connectCall) (string, error) {
	out, err := c.be.CreateTrafficDistributionGroup(c.ctx, &connectbackend.TDGRequest{
		Name: c.str(attrNameName), Description: c.str("Description"), InstanceID: c.instance(), Tags: c.tags(),
	})
	if err != nil {
		return "", err
	}

	if g, derr := c.be.DescribeTrafficDistributionGroup(out.Arn); derr == nil {
		c.stash(map[string]string{connectStatusProp: g.Status, "IsDefault": strconv.FormatBool(g.IsDefault)})
	}

	return out.Arn, nil
}

func updateConnectTrafficDistributionGroup(c *connectCall, physicalID string) error {
	return c.syncTags(physicalID)
}

func deleteConnectTrafficDistributionGroup(c *connectCall, physicalID string) error {
	_, err := c.be.DeleteTrafficDistributionGroup(physicalID)

	return err
}
