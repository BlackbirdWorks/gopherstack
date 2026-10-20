package gcpiam

import (
	"slices"
	"strings"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
)

const (
	roleOwner  = "roles/owner"
	roleEditor = "roles/editor"
	roleViewer = "roles/viewer"
)

// predefinedDef is one built-in role; basic roles are derived from the permission catalog.
type predefinedDef struct {
	name  string
	title string
	desc  string
	perms []string
}

// perms expands "service.resource" prefixes with verbs: perms("pubsub.topics", "get", "list").
func perms(prefix string, verbs ...string) []string {
	out := make([]string, 0, len(verbs))
	for _, v := range verbs {
		out = append(out, prefix+"."+v)
	}

	return out
}

func join(parts ...[]string) []string {
	var out []string
	for _, p := range parts {
		out = append(out, p...)
	}

	slices.Sort(out)

	return slices.Compact(out)
}

func projectRead() []string { return perms("resourcemanager.projects", "get", "list") }

func catalogDefs() []predefinedDef {
	base := slices.Concat(iamRoles(), storageRoles(), pubsubRoles(), secretRoles(), kmsRoles(), opsRoles())

	return slices.Concat(base, basicRoles(base))
}

func iamRoles() []predefinedDef {
	saRead := perms("iam.serviceAccounts", "get", "list")

	return []predefinedDef{
		{"roles/iam.serviceAccountUser", "Service Account User", "Run operations as the service account.",
			join(saRead, perms("iam.serviceAccounts", "actAs"))},
		{"roles/iam.serviceAccountTokenCreator", "Service Account Token Creator",
			"Impersonate service accounts (create OAuth2 access tokens, sign blobs and JWTs).",
			join(saRead, perms("iam.serviceAccounts",
				"getAccessToken", "getOpenIdToken", "implicitDelegation", "signBlob", "signJwt"))},
		{"roles/iam.serviceAccountAdmin", "Service Account Admin", "Create and manage service accounts.",
			join(projectRead(), perms("iam.serviceAccounts", "actAs", "create", "delete", "disable", "enable", "get",
				"getIamPolicy", "list", "setIamPolicy", "undelete", "update"))},
		{"roles/iam.serviceAccountCreator", "Create Service Accounts", "Allows creating service accounts.",
			join(projectRead(), saRead, perms("iam.serviceAccounts", "create"))},
		{"roles/iam.serviceAccountViewer", "Service Account Viewer", "Read-only access to service accounts.",
			join(saRead, perms("iam.serviceAccounts", "getIamPolicy"))},
		{"roles/iam.serviceAccountKeyAdmin", "Service Account Key Admin", "Create and manage service account keys.",
			join(saRead, perms("iam.serviceAccountKeys", "create", "delete", "disable", "enable", "get", "list"))},
		{"roles/iam.roleAdmin", "Role Administrator", "Create and manage custom roles.",
			join(projectRead(), perms("iam.roles", "create", "delete", "get", "list", "undelete", "update"))},
		{"roles/iam.roleViewer", "Role Viewer", "Read access to all custom roles in the project.",
			join(projectRead(), perms("iam.roles", "get", "list"))},
	}
}

func storageRoles() []predefinedDef {
	objectsRW := perms("storage.objects", "create", "delete", "get", "list", "update")
	objectsIAM := perms("storage.objects", "getIamPolicy", "setIamPolicy")
	bucketsAll := perms("storage.buckets", "create", "delete", "get", "getIamPolicy", "list", "setIamPolicy", "update")

	return []predefinedDef{
		{"roles/storage.objectViewer", "Storage Object Viewer", "Read objects and list objects in buckets.",
			join(projectRead(), perms("storage.objects", "get", "list"))},
		{"roles/storage.objectCreator", "Storage Object Creator", "Create objects.",
			join(projectRead(), perms("storage.objects", "create"))},
		{"roles/storage.objectAdmin", "Storage Object Admin", "Full control of objects.",
			join(projectRead(), objectsRW, objectsIAM)},
		{"roles/storage.admin", "Storage Admin", "Full control of buckets and objects.",
			join(projectRead(), objectsRW, bucketsAll, objectsIAM)},
	}
}

func pubsubRoles() []predefinedDef {
	topicsRead := perms("pubsub.topics", "get", "list", "getIamPolicy")
	subsRead := perms("pubsub.subscriptions", "get", "list", "getIamPolicy")
	snapsRW := perms("pubsub.snapshots", "create", "delete", "get", "list", "update")
	topicsWrite := perms("pubsub.topics", "create", "delete", "publish", "update", "attachSubscription")
	subsWrite := perms("pubsub.subscriptions", "consume", "create", "delete", "update")

	return []predefinedDef{
		{"roles/pubsub.publisher", "Pub/Sub Publisher", "Publish messages to topics.",
			perms("pubsub.topics", "publish")},
		{"roles/pubsub.subscriber", "Pub/Sub Subscriber", "Consume messages from subscriptions.",
			join(perms("pubsub.subscriptions", "consume", "get", "list"), perms("pubsub.snapshots", "get", "list"),
				perms("pubsub.topics", "attachSubscription"))},
		{"roles/pubsub.viewer", "Pub/Sub Viewer", "Read topics and subscriptions.",
			join(projectRead(), topicsRead, subsRead, perms("pubsub.snapshots", "get", "list"))},
		{"roles/pubsub.editor", "Pub/Sub Editor", "Manage topics, subscriptions and snapshots.",
			join(projectRead(), topicsRead, subsRead, topicsWrite, subsWrite, snapsRW)},
		{"roles/pubsub.admin", "Pub/Sub Admin", "Full control of Pub/Sub resources.",
			join(projectRead(), topicsRead, subsRead, topicsWrite, subsWrite, snapsRW,
				perms("pubsub.topics", "setIamPolicy"), perms("pubsub.subscriptions", "setIamPolicy"))},
	}
}

func secretRoles() []predefinedDef {
	secretsRead := join(perms("secretmanager.secrets", "get", "list", "getIamPolicy"),
		perms("secretmanager.versions", "get", "list"))

	return []predefinedDef{
		{"roles/secretmanager.secretAccessor", "Secret Manager Secret Accessor", "Access secret payloads.",
			perms("secretmanager.versions", "access")},
		{"roles/secretmanager.secretVersionManager", "Secret Manager Secret Version Manager",
			"Manage secret versions.",
			join(perms("secretmanager.versions", "add", "destroy", "disable", "enable", "get", "list"),
				perms("secretmanager.secrets", "get", "list"))},
		{"roles/secretmanager.viewer", "Secret Manager Viewer", "Read secret metadata.",
			join(projectRead(), secretsRead)},
		{
			"roles/secretmanager.admin",
			"Secret Manager Admin",
			"Full control of secrets.",
			join(
				projectRead(),
				secretsRead,
				perms("secretmanager.secrets", "create", "delete", "update", "setIamPolicy"),
				perms("secretmanager.versions", "access", "add", "destroy", "disable", "enable"),
			),
		},
	}
}

func kmsRoles() []predefinedDef {
	keysRead := perms("cloudkms.cryptoKeys", "get", "list")
	kmsRead := join(keysRead, perms("cloudkms.cryptoKeys", "getIamPolicy"),
		perms("cloudkms.cryptoKeyVersions", "get", "list", "viewPublicKey"), perms("cloudkms.keyRings", "get", "list"))

	return []predefinedDef{
		{"roles/cloudkms.cryptoKeyEncrypter", "Cloud KMS CryptoKey Encrypter", "Encrypt data with a key.",
			join(perms("cloudkms.cryptoKeyVersions", "useToEncrypt"), keysRead)},
		{"roles/cloudkms.cryptoKeyDecrypter", "Cloud KMS CryptoKey Decrypter", "Decrypt data with a key.",
			join(perms("cloudkms.cryptoKeyVersions", "useToDecrypt"), keysRead)},
		{"roles/cloudkms.cryptoKeyEncrypterDecrypter", "Cloud KMS CryptoKey Encrypter/Decrypter",
			"Encrypt and decrypt data with a key.",
			join(perms("cloudkms.cryptoKeyVersions", "useToDecrypt", "useToEncrypt"), keysRead)},
		{"roles/cloudkms.signerVerifier", "Cloud KMS CryptoKey Signer/Verifier",
			"Sign and verify with an asymmetric key.",
			join(perms("cloudkms.cryptoKeyVersions", "useToSign", "useToVerify", "viewPublicKey"), keysRead)},
		{"roles/cloudkms.viewer", "Cloud KMS Viewer", "Read key metadata.", join(projectRead(), kmsRead)},
		{"roles/cloudkms.admin", "Cloud KMS Admin", "Full control of KMS resources.",
			join(projectRead(), kmsRead, perms("cloudkms.cryptoKeys", "create", "update", "setIamPolicy"),
				perms("cloudkms.cryptoKeyVersions", "create", "destroy", "restore", "update"),
				perms("cloudkms.keyRings", "create", "getIamPolicy", "setIamPolicy"))},
	}
}

func opsRoles() []predefinedDef {
	queuesRead := join(
		perms("cloudtasks.queues", "get", "list", "getIamPolicy"),
		perms("cloudtasks.tasks", "get", "list"),
	)
	queuesAdmin := perms("cloudtasks.queues", "create", "delete", "pause", "purge", "resume", "setIamPolicy", "update")
	jobsRead := perms("cloudscheduler.jobs", "get", "list")
	jobsAdmin := perms("cloudscheduler.jobs", "create", "delete", "enable", "fullView", "pause", "run", "update")

	return []predefinedDef{
		{"roles/cloudtasks.enqueuer", "Cloud Tasks Enqueuer", "Enqueue tasks.",
			join(perms("cloudtasks.tasks", "create"), perms("cloudtasks.queues", "get"))},
		{"roles/cloudtasks.viewer", "Cloud Tasks Viewer", "Read queues and tasks.", join(projectRead(), queuesRead)},
		{
			"roles/cloudtasks.admin",
			"Cloud Tasks Admin",
			"Full control of queues and tasks.",
			join(
				projectRead(),
				queuesRead,
				queuesAdmin,
				perms("cloudtasks.tasks", "create", "delete", "fullView", "run"),
			),
		},
		{"roles/cloudscheduler.viewer", "Cloud Scheduler Viewer", "Read jobs.", join(projectRead(), jobsRead)},
		{"roles/cloudscheduler.admin", "Cloud Scheduler Admin", "Full control of jobs.",
			join(projectRead(), jobsRead, jobsAdmin)},
		{"roles/logging.logWriter", "Logs Writer", "Write log entries.", perms("logging.logEntries", "create")},
		{"roles/logging.viewer", "Logs Viewer", "Read logs.", join(projectRead(), perms("logging.logEntries", "list"))},
	}
}

// basicRoles derives owner/editor/viewer/browser from the permission catalog.
func basicRoles(defs []predefinedDef) []predefinedDef {
	var all []string
	for _, d := range defs {
		all = append(all, d.perms...)
	}

	all = join(all, perms("resourcemanager.projects", "create", "delete", "getIamPolicy", "setIamPolicy", "update"))

	var editor, viewer []string

	for _, p := range all {
		verb := p[strings.LastIndex(p, ".")+1:]

		if verb != "setIamPolicy" && !strings.HasPrefix(p, "iam.roles.") {
			editor = append(editor, p)
		}

		if verb == "get" || verb == "list" || verb == "getIamPolicy" {
			viewer = append(viewer, p)
		}
	}

	return []predefinedDef{
		{roleOwner, "Owner", "Full access to all resources.", all},
		{roleEditor, "Editor", "Edit access to all resources.", editor},
		{roleViewer, "Viewer", "Read access to all resources.", viewer},
		{"roles/browser", "Browser", "Read access to browse the hierarchy for a project.", projectRead()},
	}
}

// allPermissions is the sorted permission universe known to the emulator.
func allPermissions() []string {
	for _, d := range catalogDefs() {
		if d.name == roleOwner {
			return slices.Clone(d.perms)
		}
	}

	return nil
}

func (d predefinedDef) role(full bool) *adminpb.Role {
	r := &adminpb.Role{Name: d.name, Title: d.title, Description: d.desc, Stage: adminpb.Role_GA, Etag: []byte{0}}
	if full {
		r.IncludedPermissions = slices.Clone(d.perms)
	}

	return r
}

func predefinedByName(name string) (predefinedDef, bool) {
	for _, d := range catalogDefs() {
		if d.name == name {
			return d, true
		}
	}

	return predefinedDef{}, false
}

// auditableServices lists services that write Cloud Audit Logs.
func auditableServices() []string {
	return []string{
		"cloudkms.googleapis.com", "cloudscheduler.googleapis.com", "cloudtasks.googleapis.com",
		"iam.googleapis.com", "iamcredentials.googleapis.com", "logging.googleapis.com",
		"pubsub.googleapis.com", "secretmanager.googleapis.com", "storage.googleapis.com",
	}
}
