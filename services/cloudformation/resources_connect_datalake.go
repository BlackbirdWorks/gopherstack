package cloudformation

import (
	"fmt"
	"strings"

	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

const (
	resTypeConnectDataLakeAssociation = "AWS::Connect::DataLakeAssociation"
	connectInstanceIDProp             = "InstanceId"
)

// createConnectDataLakeAssociation shares one dataset; Ref is "<instance ID>|<dataset ID>|<target account ID>".
func createConnectDataLakeAssociation(c *connectCall) (string, error) {
	in := connectbackend.AssociateAnalyticsDataSetInput{TargetAccountID: c.str("TargetAccountId")}
	in.InstanceID, in.DataSetID = lastPathSegment(c.instance()), c.str("DataSetId")

	out, err := c.be.AssociateAnalyticsDataSet(c.ctx, &in)
	if err != nil {
		return "", err
	}

	c.stash(map[string]string{"ResourceShareArn": out.ResourceShareArn, "ResourceShareId": out.ResourceShareID})

	return connectCompositeID(in.InstanceID, out.DataSetID, out.TargetAccountID), nil
}

func deleteConnectDataLakeAssociation(c *connectCall, physicalID string) error {
	parts := strings.Split(physicalID, "|")
	if len(parts) != connectCompositeParts {
		return fmt.Errorf("%w: data lake association ID %q", errUnsupportedCFNValue, physicalID)
	}

	in := connectbackend.AssociateAnalyticsDataSetInput{TargetAccountID: parts[2]}
	in.InstanceID, in.DataSetID = parts[0], parts[1]
	_, err := c.be.DisassociateAnalyticsDataSet(c.ctx, &in)

	return err
}
