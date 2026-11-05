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

	typeAuditLog        = "google.cloud.audit.log.v1.written"
	typeFirestorePrefix = "google.cloud.firestore.document.v1."
	providerAudit       = "cloudaudit.googleapis.com"
	providerFirestore   = "firestore.googleapis.com"
	attrServiceName     = "serviceName"
	attrMethodName      = "methodName"
	attrResourceName    = "resourceName"
	attrDatabase        = "database"
	attrNamespace       = "namespace"
	attrDocument        = "document"
	auditSourcePrefix   = "//cloudaudit.googleapis.com/projects/"
	firestoreSourceBase = "//firestore.googleapis.com/"
	firestoreWritten    = "written"
	defaultFirestoreNS  = "(default)"
)

func firestoreEventTypes() []string { return []string{"created", "updated", kindDeleted, "written"} }

func firestoreType(suffix string) string { return typeFirestorePrefix + suffix }

func isFirestoreType(t string) bool {
	suffix, ok := strings.CutPrefix(t, typeFirestorePrefix)

	return ok && slices.Contains(firestoreEventTypes(), suffix)
}

func defaultContentType(t string) string {
	if isFirestoreType(t) {
		return contentTypeProto
	}

	return contentTypeJSON
}

func storageEventTypes() []string {
	return []string{"finalized", "deleted", "archived", "metadataUpdated"}
}

// locations are the regions Eventarc is offered in, plus the global endpoint.
func locations() []string {
	return []string{
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
}

func knownLocation(id string) bool { return slices.Contains(locations(), id) }

func storageType(suffix string) string { return typeStoragePrefix + suffix }

func isStorageType(t string) bool {
	suffix, ok := strings.CutPrefix(t, typeStoragePrefix)

	return ok && slices.Contains(storageEventTypes(), suffix)
}

func isSupportedType(t string) bool {
	return t == typePubsubPublished || t == typeAuditLog || isStorageType(t) || isFirestoreType(t)
}

type attrSpec struct {
	name        string
	description string
	required    bool
	pathPattern bool
}

func filteringAttrs(t string) []attrSpec {
	switch {
	case t == typeAuditLog:
		return []attrSpec{
			{
				name:        attrServiceName,
				description: "The Google API service name, e.g. storage.googleapis.com.",
				required:    true,
			},
			{name: attrMethodName, description: "The audited method name.", required: true},
			{name: attrResourceName, description: "The audited resource name.", pathPattern: true},
		}
	case isFirestoreType(t):
		return []attrSpec{
			{name: attrDatabase, description: "The Firestore database id.", required: true},
			{name: attrNamespace, description: "The Firestore namespace.", required: true},
			{name: attrDocument, description: "The document path, e.g. users/{id}.", pathPattern: true},
		}
	}

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
		out := make([]*eventarcpb.EventType, 0, len(storageEventTypes()))

		for _, s := range storageEventTypes() {
			et := &eventarcpb.EventType{
				Type:           storageType(s),
				Description:    storageDescription(s),
				EventSchemaUri: schemaURIBase + "storage/v1/data.proto",
			}

			for _, a := range filteringAttrs(storageType(s)) {
				et.FilteringAttributes = append(et.FilteringAttributes, &eventarcpb.FilteringAttribute{
					Attribute:            a.name,
					Description:          a.description,
					Required:             a.required,
					PathPatternSupported: a.pathPattern,
				})
			}

			out = append(out, et)
		}

		return out
	case providerAudit:
		return []*eventarcpb.EventType{{
			Type:                typeAuditLog,
			Description:         "A Cloud Audit Logs entry was written.",
			EventSchemaUri:      schemaURIBase + "audit/v1/data.proto",
			FilteringAttributes: filteringAttributes(typeAuditLog),
		}}
	case providerFirestore:
		out := make([]*eventarcpb.EventType, 0, len(firestoreEventTypes()))

		for _, k := range firestoreEventTypes() {
			out = append(out, &eventarcpb.EventType{
				Type:                firestoreType(k),
				Description:         "A Firestore document was " + k + ".",
				EventSchemaUri:      schemaURIBase + "firestore/v1/data.proto",
				FilteringAttributes: filteringAttributes(firestoreType(k)),
			})
		}

		return out
	default:
		return nil
	}
}

func filteringAttributes(t string) []*eventarcpb.FilteringAttribute {
	specs := filteringAttrs(t)
	out := make([]*eventarcpb.FilteringAttribute, 0, len(specs))

	for _, a := range specs {
		out = append(out, &eventarcpb.FilteringAttribute{
			Attribute: a.name, Description: a.description, Required: a.required, PathPatternSupported: a.pathPattern,
		})
	}

	return out
}

func storageDescription(suffix string) string {
	switch suffix {
	case "finalized":
		return "An object was created or overwritten in a Cloud Storage bucket."
	case "deleted":
		return "An object was permanently deleted from a Cloud Storage bucket."
	case "archived":
		return "A live object version became noncurrent in a versioned Cloud Storage bucket."
	default:
		return "The metadata of an object in a Cloud Storage bucket was updated."
	}
}

func providerDisplayName(id string) string {
	switch id {
	case providerPubsub:
		return "Cloud Pub/Sub"
	case providerAudit:
		return "Cloud Audit Logs"
	case providerFirestore:
		return "Cloud Firestore"
	}

	return "Cloud Storage"
}

func providerIDs() []string {
	return []string{providerPubsub, providerStorage, providerAudit, providerFirestore}
}

func buildProvider(project, location, id string) *eventarcpb.Provider {
	return &eventarcpb.Provider{
		Name:        childName(project, location, "providers", id),
		DisplayName: providerDisplayName(id),
		EventTypes:  eventTypeCatalog(id),
	}
}
