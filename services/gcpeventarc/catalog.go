package gcpeventarc

import (
	"slices"
	"strings"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
)

const (
	typePubsubPublished  = "google.cloud.pubsub.topic.v1.messagePublished"
	typeStoragePrefix    = "google.cloud.storage.object.v1."
	attrType             = "type"
	attrBucket           = "bucket"
	attrSubject          = "subject"
	operatorPathPattern  = "match-path-pattern"
	providerPubsub       = "pubsub.googleapis.com"
	providerStorage      = "storage.googleapis.com"
	schemaURIBase        = "https://github.com/googleapis/google-cloudevents/blob/main/proto/google/events/cloud/"
	globalLocation       = "global"
	storageSourcePrefix  = "//storage.googleapis.com/projects/_/buckets/"
	pubsubSourcePrefix   = "//pubsub.googleapis.com/"
	storageSubjectPrefix = "objects/"
)

var storageEventTypes = []string{"finalized", "deleted", "archived", "metadataUpdated"}

// locations are the regions Eventarc is offered in, plus the global endpoint.
var locations = []string{
	globalLocation,
	"africa-south1", "asia-east1", "asia-east2", "asia-northeast1", "asia-northeast2", "asia-northeast3",
	"asia-south1", "asia-south2", "asia-southeast1", "asia-southeast2", "australia-southeast1",
	"australia-southeast2", "europe-central2", "europe-north1", "europe-southwest1", "europe-west1",
	"europe-west2", "europe-west3", "europe-west4", "europe-west6", "europe-west8", "europe-west9",
	"europe-west10", "europe-west12", "me-central1", "me-central2", "me-west1", "northamerica-northeast1",
	"northamerica-northeast2", "southamerica-east1", "southamerica-west1", "us-central1", "us-east1",
	"us-east4", "us-east5", "us-south1", "us-west1", "us-west2", "us-west3", "us-west4",
	"us", "eu", "asia",
}

func knownLocation(id string) bool { return slices.Contains(locations, id) }

func storageType(suffix string) string { return typeStoragePrefix + suffix }

func isStorageType(t string) bool {
	suffix, ok := strings.CutPrefix(t, typeStoragePrefix)

	return ok && slices.Contains(storageEventTypes, suffix)
}

func isSupportedType(t string) bool { return t == typePubsubPublished || isStorageType(t) }

type attrSpec struct {
	name        string
	description string
	required    bool
	pathPattern bool
}

func filteringAttrs(t string) []attrSpec {
	if isStorageType(t) {
		return []attrSpec{
			{name: attrBucket, description: "The Cloud Storage bucket the object is in.", required: true},
			{name: attrSubject, description: "The object, as objects/{name}.", pathPattern: true},
		}
	}

	return nil
}

func eventTypeCatalog(provider string) []*eventarcpb.EventType {
	switch provider {
	case providerPubsub:
		return []*eventarcpb.EventType{{
			Type:           typePubsubPublished,
			Description:    "A message was published to a Pub/Sub topic.",
			EventSchemaUri: schemaURIBase + "pubsub/v1/data.proto",
		}}
	case providerStorage:
		out := make([]*eventarcpb.EventType, 0, len(storageEventTypes))

		for _, s := range storageEventTypes {
			et := &eventarcpb.EventType{
				Type:           storageType(s),
				Description:    storageDescriptions[s],
				EventSchemaUri: schemaURIBase + "storage/v1/data.proto",
			}

			for _, a := range filteringAttrs(storageType(s)) {
				et.FilteringAttributes = append(et.FilteringAttributes, &eventarcpb.FilteringAttribute{
					Attribute: a.name, Description: a.description, Required: a.required, PathPatternSupported: a.pathPattern,
				})
			}

			out = append(out, et)
		}

		return out
	default:
		return nil
	}
}

var storageDescriptions = map[string]string{
	"finalized":       "An object was created or overwritten in a Cloud Storage bucket.",
	"deleted":         "An object was permanently deleted from a Cloud Storage bucket.",
	"archived":        "A live object version became noncurrent in a versioned Cloud Storage bucket.",
	"metadataUpdated": "The metadata of an object in a Cloud Storage bucket was updated.",
}

var providerNames = map[string]string{
	providerPubsub:  "Cloud Pub/Sub",
	providerStorage: "Cloud Storage",
}

func providerIDs() []string { return []string{providerPubsub, providerStorage} }

func buildProvider(project, location, id string) *eventarcpb.Provider {
	return &eventarcpb.Provider{
		Name:        childName(project, location, "providers", id),
		DisplayName: providerNames[id],
		EventTypes:  eventTypeCatalog(id),
	}
}
