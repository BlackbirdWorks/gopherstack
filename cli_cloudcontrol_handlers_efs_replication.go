package main

import (
	"context"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const ccKeyReplication = "ReplicationConfiguration"

type ccReplicationDestination struct {
	AvailabilityZoneName *string
	FileSystemID         *string `json:"FileSystemId"`
	KmsKeyID             *string `json:"KmsKeyId"`
	Region               *string
	RoleArn              *string
}

type ccReplication struct {
	Destinations []ccReplicationDestination
}

func ccPropChanged(current, desired map[string]any, key string) bool {
	have, had := current[key]
	want, has := desired[key]

	return had != has || (has && !reflect.DeepEqual(normalizeJSON(have), normalizeJSON(want)))
}

func (h *ccFileSystem) replicationFor(
	ctx context.Context, id string,
) (efstypes.ReplicationConfigurationDescription, bool, error) {
	out, err := h.client.DescribeReplicationConfigurations(ctx, &efs.DescribeReplicationConfigurationsInput{
		FileSystemId: aws.String(id),
	})
	if err != nil {
		return efstypes.ReplicationConfigurationDescription{}, false, ccMapError(err)
	}

	for _, rc := range out.Replications {
		if aws.ToString(rc.SourceFileSystemId) == id {
			return rc, true, nil
		}
	}

	return efstypes.ReplicationConfigurationDescription{}, false, nil
}

func (h *ccFileSystem) dropReplication(ctx context.Context, id string) error {
	_, found, err := h.replicationFor(ctx, id)
	if err != nil || !found {
		return err
	}

	_, err = h.client.DeleteReplicationConfiguration(ctx, &efs.DeleteReplicationConfigurationInput{
		SourceFileSystemId: aws.String(id),
	})

	return ccMapError(err)
}

func ccReplicationFrom(desired map[string]any) (ccReplication, bool, error) {
	if desired[ccKeyReplication] == nil {
		return ccReplication{}, false, nil
	}

	want, err := ccDecode[ccReplication](desired[ccKeyReplication])
	if err != nil {
		return want, false, err
	}

	if len(want.Destinations) != 1 {
		return want, false, fmt.Errorf("%w: ReplicationConfiguration.Destinations takes exactly one destination",
			cloudcontrolbackend.ErrValidation)
	}

	return want, true, nil
}

func (h *ccFileSystem) syncReplication(ctx context.Context, id string, desired map[string]any) error {
	want, given, err := ccReplicationFrom(desired)
	if err != nil {
		return err
	}

	if err = h.dropReplication(ctx, id); err != nil || !given {
		return err
	}

	d := want.Destinations[0]
	_, err = h.client.CreateReplicationConfiguration(ctx, &efs.CreateReplicationConfigurationInput{
		SourceFileSystemId: aws.String(id),
		Destinations: []efstypes.DestinationToCreate{{
			AvailabilityZoneName: d.AvailabilityZoneName, FileSystemId: d.FileSystemID,
			KmsKeyId: d.KmsKeyID, Region: d.Region, RoleArn: d.RoleArn,
		}},
	})

	return ccMapError(err)
}

func (h *ccFileSystem) readReplication(ctx context.Context, id string, model map[string]any) {
	rc, found, err := h.replicationFor(ctx, id)
	if err != nil || !found {
		return
	}

	dests := make([]map[string]any, 0, len(rc.Destinations))
	for _, d := range rc.Destinations {
		dests = append(dests, map[string]any{
			"FileSystemId": aws.ToString(d.FileSystemId), "Region": aws.ToString(d.Region),
			"Status": string(d.Status), "StatusMessage": d.StatusMessage, "RoleArn": d.RoleArn,
		})
	}

	model[ccKeyReplication] = map[string]any{"Destinations": dests}
}
