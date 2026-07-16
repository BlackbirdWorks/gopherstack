package ec2

import "fmt"

// GetFlowLogsIntegrationTemplate generates a CloudFormation template string
// wiring an existing flow log to Athena, referencing the flow log's real ID
// and the requested S3 destination ARN.
func (b *InMemoryBackend) GetFlowLogsIntegrationTemplate(flowLogID, s3DestinationArn string) (string, error) {
	if flowLogID == "" {
		return "", fmt.Errorf("%w: FlowLogId is required", ErrInvalidParameter)
	}

	if s3DestinationArn == "" {
		return "", fmt.Errorf("%w: ConfigDeliveryS3DestinationArn is required", ErrInvalidParameter)
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
}`, fl.FlowLogID, fl.FlowLogID, s3DestinationArn)

	return tmpl, nil
}

// ---- Misc singletons ----
