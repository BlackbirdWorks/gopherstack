package main

import (
	"context"

	amplifybackend "github.com/blackbirdworks/gopherstack/services/amplify"
	apigwv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	databrewbackend "github.com/blackbirdworks/gopherstack/services/databrew"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	iotanalyticsbackend "github.com/blackbirdworks/gopherstack/services/iotanalytics"
	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	textractbackend "github.com/blackbirdworks/gopherstack/services/textract"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// wireResourceGroupsTaggingSweep7 wires Amplify, IoT, IoT Analytics, DataBrew,
// MSK and Textract; API Gateway v2 is wired from Sweep6 ahead of API Gateway v1.
func wireResourceGroupsTaggingSweep7(
	bk rgtapibackend.StorageBackend,
	byName map[string]service.Registerable,
) {
	wireTaggingAmplify(bk, byName["Amplify"])
	wireTaggingIoT(bk, byName["IoT"])
	wireTaggingIoTAnalytics(bk, byName["IoTAnalytics"])
	wireTaggingDataBrew(bk, byName["DataBrew"])
	wireTaggingKafka(bk, byName["Kafka"])
	wireTaggingTextract(bk, byName["Textract"])
}

func toTaggedResources[T ~struct {
	Tags map[string]string
	ARN  string
}](items []T, resourceTypeOf func(string) string) []rgtapibackend.TaggedResource {
	out := make([]rgtapibackend.TaggedResource, 0, len(items))
	for _, it := range items {
		e := taggedARNEntry(it)
		out = append(out, rgtapibackend.TaggedResource{
			ResourceARN: e.ARN, ResourceType: resourceTypeOf(e.ARN), Tags: e.Tags,
		})
	}

	return out
}

func requestRegion(ctx context.Context, fallback string) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return fallback
}

func wireTaggingAmplify(bk rgtapibackend.StorageBackend, reg service.Registerable) {
	if h, ok := reg.(*amplifybackend.Handler); ok {
		wireStdRegionalTagging[*amplifybackend.InMemoryBackend, amplifybackend.TaggedEntry](
			bk, "amplify", arnResourceType("amplify"),
			func(region string) any { return h.BackendFor(region) })
	}
}

func wireTaggingIoT(bk rgtapibackend.StorageBackend, reg service.Registerable) {
	if h, ok := reg.(*iotbackend.Handler); ok {
		wireStdRegionalTagging[*iotbackend.InMemoryBackend, iotbackend.TaggedEntry](
			bk, "iot", arnResourceType("iot"),
			func(region string) any { return h.BackendFor(region) })
	}
}

func wireTaggingIoTAnalytics(bk rgtapibackend.StorageBackend, reg service.Registerable) {
	h, ok := reg.(*iotanalyticsbackend.Handler)
	if !ok {
		return
	}

	wireRegionalTagging(bk, regionalTagSpec[*iotanalyticsbackend.InMemoryBackend]{
		arnService:     "iotanalytics",
		resourceTypeOf: arnResourceType("iotanalytics"),
		backendFor: regionalBackendFor[*iotanalyticsbackend.InMemoryBackend](
			func(region string) any { return h.BackendFor(region) }),
		list: func(b *iotanalyticsbackend.InMemoryBackend) []taggedARNEntry {
			return taggedEntries(b.TaggedResources())
		},
		tag: func(b *iotanalyticsbackend.InMemoryBackend, _ context.Context, arn string, tags map[string]string) error {
			return b.TagResource(arn, mapToTagSlice(tags, func(k, v string) iotanalyticsbackend.TagDTO {
				return iotanalyticsbackend.TagDTO{Key: k, Value: v}
			}))
		},
		untag: func(b *iotanalyticsbackend.InMemoryBackend, _ context.Context, arn string, keys []string) error {
			return b.UntagResource(arn, keys)
		},
	})
}

func wireTaggingDataBrew(bk rgtapibackend.StorageBackend, reg service.Registerable) {
	h, ok := reg.(*databrewbackend.Handler)
	if !ok {
		return
	}

	dbBk, ok := h.Backend.(*databrewbackend.InMemoryBackend)
	if !ok {
		return
	}

	registerTaggingService(
		bk,
		func(ctx context.Context) []rgtapibackend.TaggedResource {
			return toTaggedResources(
				dbBk.TaggedResources(requestRegion(ctx, dbBk.Region())), arnResourceType("databrew"))
		},
		"databrew",
		func(ctx context.Context, arn string, tags map[string]string) error {
			return dbBk.UpdateTagsByArn(databrewbackend.WithRegion(ctx, originRegion(ctx, arn)), arn, tags, nil)
		},
		func(ctx context.Context, arn string, keys []string) error {
			return dbBk.UpdateTagsByArn(databrewbackend.WithRegion(ctx, originRegion(ctx, arn)), arn, nil, keys)
		},
	)
}

func wireTaggingKafka(bk rgtapibackend.StorageBackend, reg service.Registerable) {
	h, ok := reg.(*kafkabackend.Handler)
	if !ok {
		return
	}

	kBk, ok := h.Backend.(*kafkabackend.InMemoryBackend)
	if !ok {
		return
	}

	registerTaggingService(
		bk,
		func(ctx context.Context) []rgtapibackend.TaggedResource {
			return toTaggedResources(
				kBk.TaggedResources(requestRegion(ctx, kBk.Region())), arnResourceType("kafka"))
		},
		"kafka",
		kBk.TagResource,
		kBk.UntagResource,
	)
}

func wireTaggingTextract(bk rgtapibackend.StorageBackend, reg service.Registerable) {
	h, ok := reg.(*textractbackend.Handler)
	if !ok {
		return
	}

	tBk, ok := h.Backend.(*textractbackend.InMemoryBackend)
	if !ok {
		return
	}

	registerTaggingService(
		bk,
		func(ctx context.Context) []rgtapibackend.TaggedResource {
			return toTaggedResources(
				tBk.TaggedResources(requestRegion(ctx, tBk.Region())), arnResourceType("textract"))
		},
		"textract",
		tBk.TagResource,
		tBk.UntagResource,
	)
}

// wireTaggingAPIGatewayV2 shares the "apigateway" ARN namespace with API Gateway v1
// (domainnames and vpclinks collide), so it claims an ARN only when a v2 resource
// exists and must be registered ahead of wireTaggingAPIGateway.
func wireTaggingAPIGatewayV2(bk rgtapibackend.StorageBackend, reg service.Registerable) {
	h, ok := reg.(*apigwv2backend.Handler)
	if !ok {
		return
	}

	backendFor := func(ctx context.Context, arn string) *apigwv2backend.InMemoryBackend {
		b, _ := h.BackendFor(originRegion(ctx, arn)).(*apigwv2backend.InMemoryBackend)

		return b
	}

	ownsARN := func(b *apigwv2backend.InMemoryBackend, arn string) bool {
		if b == nil || !arnServiceIs(arn, "apigateway") {
			return false
		}

		_, err := b.GetTags(arn)

		return err == nil
	}

	bk.RegisterProvider(func(ctx context.Context) []rgtapibackend.TaggedResource {
		b := backendFor(ctx, "")
		if b == nil {
			return nil
		}

		return toTaggedResources(b.TaggedResources(), apigatewayResourceType)
	})
	bk.RegisterARNTagger(func(ctx context.Context, arn string, tags map[string]string) (bool, error) {
		b := backendFor(ctx, arn)
		if !ownsARN(b, arn) {
			return false, nil
		}

		return true, b.TagResource(arn, tags)
	})
	bk.RegisterARNUntagger(func(ctx context.Context, arn string, keys []string) (bool, error) {
		b := backendFor(ctx, arn)
		if !ownsARN(b, arn) {
			return false, nil
		}

		return true, b.UntagResource(arn, keys)
	})
}
