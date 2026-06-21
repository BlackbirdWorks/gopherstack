package main

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var s3Client *s3.Client

func init() {
	// Initialize the AWS SDK v2. The endpoint will be resolved from AWS_ENDPOINT_URL automatically.
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("unable to load SDK config: %v", err)
	}

	// For S3, we must use path-style addressing when hitting a local mock like Gopherstack
	s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		
		// If running in docker, we might have passed LAMBDA_ENDPOINT. If so, override.
		// AWS_ENDPOINT_URL is standard but older SDKs might need explicit EndpointResolver.
		if ep := os.Getenv("AWS_ENDPOINT_URL"); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
	})
}

func handler(ctx context.Context, s3Event events.S3Event) (string, error) {
	log.Printf("Received %d records", len(s3Event.Records))
	for _, record := range s3Event.Records {
		s3Entity := record.S3
		log.Printf("[%s] Bucket = %s, Key = %s", record.EventName, s3Entity.Bucket.Name, s3Entity.Object.Key)

		// Fetch the object from S3
		obj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s3Entity.Bucket.Name),
			Key:    aws.String(s3Entity.Object.Key),
		})
		if err != nil {
			log.Printf("Error getting object %s/%s: %v", s3Entity.Bucket.Name, s3Entity.Object.Key, err)
			return "", err
		}
		
		body, err := io.ReadAll(obj.Body)
		_ = obj.Body.Close()
		if err != nil {
			log.Printf("Error reading object body: %v", err)
			return "", err
		}

		log.Printf("Successfully read object! Size = %d bytes, Content = %q", len(body), string(body))
	}

	return "Successfully read object!", nil
}

func main() {
	lambda.Start(handler)
}
