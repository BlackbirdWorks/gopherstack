package cloudformation

import (
	"context"
	"fmt"

	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
)

// ---- MSK (Kafka) ----

func (rc *ResourceCreator) createMSKCluster(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Kafka == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "ClusterName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	kafkaVersion := strProp(props, "KafkaVersion", params, physicalIDs)
	if kafkaVersion == "" {
		kafkaVersion = "3.4.0"
	}

	var numBrokers int32 = 3
	if n, ok := props["NumberOfBrokerNodes"].(float64); ok {
		numBrokers = int32(n)
	}

	var brokerInfo kafkabackend.BrokerNodeGroupInfo
	if b, ok := props["BrokerNodeGroupInfo"].(map[string]any); ok {
		brokerInfo.InstanceType = resolve(b["InstanceType"], params, physicalIDs)
	}
	if brokerInfo.InstanceType == "" {
		brokerInfo.InstanceType = "kafka.m5.large"
	}

	cluster, err := rc.backends.Kafka.Backend.CreateCluster(
		ctx, name, kafkaVersion, numBrokers, brokerInfo, nil, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create MSK cluster %s: %w", name, err)
	}

	return cluster.ClusterArn, nil
}

func (rc *ResourceCreator) deleteMSKCluster(ctx context.Context, arn string) error {
	if rc.backends.Kafka == nil {
		return nil
	}

	return rc.backends.Kafka.Backend.DeleteCluster(ctx, arn)
}
