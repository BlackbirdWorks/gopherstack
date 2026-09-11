package awsconfig

import "context"

// S3Writer writes a delivered configuration snapshot object to S3. Mirrors
// stepfunctions' asl.S3Writer contract (services/stepfunctions/asl/executor.go:121).
type S3Writer interface {
	PutObjectBytes(ctx context.Context, bucket, key string, data []byte) error
}

// SNSPublisher publishes a configuration-stream notification to an SNS
// topic. Mirrors the consuming-service-declares-the-interface convention
// used by ses.SNSPublisher (services/ses/interfaces.go:13).
type SNSPublisher interface {
	PublishToTopic(topicARN, message string) error
}
