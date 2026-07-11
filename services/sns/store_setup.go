package sns

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend that has a value type whose
// identity is a pure function of its own fields is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/sqs pilot (commit 0f09d77c) / services/ec2 sweep (commit
// 12e611a4) for the pattern this follows.
//
// A handful of fields are deliberately NOT registered here and remain plain
// maps -- see the comment block above registerAllTables for the list and why.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func topicKeyFn(v *Topic) string                             { return v.TopicArn }
func subscriptionKeyFn(v *Subscription) string               { return v.SubscriptionArn }
func subscriptionTopicKeyFn(v *Subscription) string          { return v.TopicArn }
func platformApplicationKeyFn(v *PlatformApplication) string { return v.PlatformApplicationArn }
func platformEndpointKeyFn(v *PlatformEndpoint) string       { return v.EndpointArn }
func sandboxPhoneKeyFn(v *SandboxPhoneNumber) string         { return v.PhoneNumber }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
//
// The following resource fields are deliberately left as plain maps (not
// registered here) because their value type is not a pure keyed identity or
// carries a slice/scalar value store.Table cannot key by:
//   - topicTags: value type *tags.Tags carries no identity field of its own;
//     keyed externally by topic ARN (same shape as EC2's VerifiedAccessPolicy
//     maps left raw in services/ec2/store_setup.go)
//   - optedOutPhoneNumbers: value type is bool, which store.Table cannot key
//     (no identity field at all)
//   - smsAttributes: value type is string, same reason as above
//   - originationNumbers: slice-valued (map[string][]XMLOriginationPhone)
//   - topicMessageArchive: slice-valued (map[string][]*ArchivedMessage); this
//     is the parity-sweep ArchivePolicy/ReplayPolicy replay buffer and must
//     keep its own persistence path (see persistence.go)
func registerAllTables(b *InMemoryBackend) {
	b.topics = store.Register(b.registry, "topics", store.New(topicKeyFn))
	b.subscriptions = store.Register(b.registry, "subscriptions", store.New(subscriptionKeyFn))
	b.subscriptionsByTopic = b.subscriptions.AddIndex("topic", subscriptionTopicKeyFn)
	b.platformApplications = store.Register(
		b.registry, "platformApplications", store.New(platformApplicationKeyFn),
	)
	b.platformEndpoints = store.Register(b.registry, "platformEndpoints", store.New(platformEndpointKeyFn))
	b.smsSandbox = store.Register(b.registry, "smsSandbox", store.New(sandboxPhoneKeyFn))
}
