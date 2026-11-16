package ocifunctions

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// Destination kinds (SDK functions/*_destination_details.go).
const (
	destNone         = "NONE"
	destStream       = "STREAM"
	destQueue        = "QUEUE"
	destNotification = "NOTIFICATION"
)

// Network resolves subnets and network security groups of OCI Core; each returns the owning VCN.
type Network struct {
	Subnet func(id string) (vcnID string, ok bool)
	NSG    func(id string) (vcnID string, ok bool)
}

// ImageInfo identifies an image held by the emulated container registry.
type ImageInfo struct {
	ID     string
	Digest string
}

// Images resolves function images against the emulated container registry and verifies their signatures.
type Images struct {
	// Resolve reports whether the reference names a stored image; owned is false for references outside the
	// emulated tenancy namespace.
	Resolve func(ref string) (img ImageInfo, found, owned bool)
	// Verified reports whether the image carries a valid signature made by one of the keys.
	Verified func(img ImageInfo, keyIDs []string) bool
}

// Destinations delivers detached invocation results to other emulated OCI services and checks targets exist.
type Destinations struct {
	Stream func(ctx context.Context, streamID string, value []byte) error
	Queue  func(ctx context.Context, queueID, channelID, content string) error
	Notify func(ctx context.Context, topicID, title, body string) error
	Exists func(kind, id string) bool
}

type links struct {
	network Network
	images  Images
	dests   Destinations
}

// SetNetwork validates application subnets and network security groups against OCI Core.
func (b *InMemoryBackend) SetNetwork(n Network) {
	b.mu.Lock("SetNetwork")
	b.links.network = n
	b.mu.Unlock()
}

// SetImages validates function images and image policy signatures against OCI Artifacts.
func (b *InMemoryBackend) SetImages(i Images) {
	b.mu.Lock("SetImages")
	b.links.images = i
	b.mu.Unlock()
}

// SetDestinations delivers detached results to OCI Streaming, Queue and Notifications.
func (b *InMemoryBackend) SetDestinations(d Destinations) {
	b.mu.Lock("SetDestinations")
	b.links.dests = d
	b.mu.Unlock()
}

// checkNetwork must be called with the lock held.
func (b *InMemoryBackend) checkNetwork(subnets, nsgs []string) *APIError {
	n := b.links.network

	var vcns []string

	if n.Subnet != nil {
		for _, id := range subnets {
			vcn, ok := n.Subnet(id)
			if !ok {
				return errNotFound(id)
			}

			vcns = append(vcns, vcn)
		}
	}

	if n.NSG == nil {
		return nil
	}

	for _, id := range nsgs {
		vcn, ok := n.NSG(id)
		if !ok {
			return errNotFound(id)
		}

		if len(vcns) > 0 && !slices.Contains(vcns, vcn) {
			return errParam("networkSecurityGroup " + id + " is not in the VCN of the application's subnets")
		}
	}

	return nil
}

type destSpec struct {
	Kind      string `json:"kind"`
	StreamID  string `json:"streamId"`
	QueueID   string `json:"queueId"`
	ChannelID string `json:"channelId"`
	TopicID   string `json:"topicId"`
}

func (d destSpec) target() string {
	switch d.Kind {
	case destStream:
		return d.StreamID
	case destQueue:
		return d.QueueID
	case destNotification:
		return d.TopicID
	}

	return ""
}

func parseDest(raw jsontext.Value, field string) (destSpec, *APIError) {
	var d destSpec
	if len(raw) == 0 {
		return d, nil
	}

	if err := json.Unmarshal(raw, &d, jsonx.V1()); err != nil {
		return d, errParam(field + " is not a valid destination")
	}

	switch d.Kind {
	case destNone:
		return d, nil
	case destStream, destQueue, destNotification:
		if d.target() == "" {
			return d, errMissing(field + " requires the " + map[string]string{
				destStream: "streamId", destQueue: "queueId", destNotification: "topicId",
			}[d.Kind])
		}

		return d, nil
	}

	return d, errParam(field + ".kind must be NONE, STREAM, QUEUE or NOTIFICATION")
}

// checkDestinations must be called with the lock held.
func (b *InMemoryBackend) checkDestinations(success, failure jsontext.Value) *APIError {
	for field, raw := range map[string]jsontext.Value{"successDestination": success, "failureDestination": failure} {
		d, aerr := parseDest(raw, field)
		if aerr != nil {
			return aerr
		}

		if d.Kind != "" && d.Kind != destNone && b.links.dests.Exists != nil &&
			!b.links.dests.Exists(d.Kind, d.target()) {
			return errNotFound(d.target())
		}
	}

	return nil
}

// checkImage must be called with the lock held.
func (b *InMemoryBackend) checkImage(app *Application, src *sourceIn) *APIError {
	i := b.links.images
	if i.Resolve == nil {
		return nil
	}

	img, found, owned := i.Resolve(src.Image)

	switch {
	case owned && !found:
		return errParam("The image " + src.Image + " was not found in the container registry")
	case found && src.ImageDigest != "" && src.ImageDigest != img.Digest:
		return errParam("sourceDetails.imageDigest does not match the digest of " + src.Image)
	}

	return b.checkSignature(app, img, found)
}

// checkSignature must be called with the lock held.
func (b *InMemoryBackend) checkSignature(app *Application, img ImageInfo, found bool) *APIError {
	p := app.ImagePolicy
	if p == nil || p.IsPolicyEnabled == nil || !*p.IsPolicyEnabled || b.links.images.Verified == nil {
		return nil
	}

	keys := make([]string, 0, len(p.KeyDetails))
	for _, k := range p.KeyDetails {
		keys = append(keys, k.KmsKeyID)
	}

	if !found || !b.links.images.Verified(img, keys) {
		return errParam("The image has no valid signature from a key of the application's image policy")
	}

	return nil
}
