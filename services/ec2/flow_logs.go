package ec2

import "fmt"

// GetFlowLogsIntegrationTemplate generates a CloudFormation template string
// wiring an existing flow log to Athena, referencing the flow log's real ID
// and the requested S3 destinations. The Athena WorkGroup's OutputLocation
// comes from IntegrateServices.AthenaIntegrations[0].IntegrationResultS3DestinationArn
// (serializers.go:60999-61005, wire key "IntegrateServices.AthenaIntegration.
// 1.IntegrationResultS3DestinationArn") -- a distinct field from
// ConfigDeliveryS3DestinationArn, which is where the generated template
// config itself is delivered, not where Athena query results land.
func (b *InMemoryBackend) GetFlowLogsIntegrationTemplate(
	flowLogID, s3DestinationArn, athenaResultS3DestinationArn, partitionLoadFrequency string,
) (string, error) {
	if flowLogID == "" {
		return "", fmt.Errorf("%w: FlowLogId is required", ErrInvalidParameter)
	}

	if s3DestinationArn == "" {
		return "", fmt.Errorf("%w: ConfigDeliveryS3DestinationArn is required", ErrInvalidParameter)
	}

	if athenaResultS3DestinationArn == "" || partitionLoadFrequency == "" {
		return "", fmt.Errorf(
			"%w: IntegrateServices.AthenaIntegrations[].IntegrationResultS3DestinationArn"+
				" and PartitionLoadFrequency are required",
			ErrInvalidParameter,
		)
	}

	b.mu.RLock("GetFlowLogsIntegrationTemplate")
	defer b.mu.RUnlock()

	fl, ok := b.flowLogs.Get(flowLogID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrFlowLogNotFound, flowLogID)
	}

	tmpl := fmt.Sprintf(`{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Description": "CloudFormation template to integrate VPC Flow Log %s with Athena",
  "Resources": {
    "FlowLogAthenaWorkGroup": {
      "Type": "AWS::Athena::WorkGroup",
      "Properties": {
        "Name": "flow-log-%s-athena",
        "WorkGroupConfiguration": {
          "ResultConfiguration": {
            "OutputLocation": %q
          }
        }
      }
    }
  }
}`, fl.FlowLogID, fl.FlowLogID, athenaResultS3DestinationArn)

	return tmpl, nil
}

// ---- Misc singletons ----
