package gcpeventarc

import (
	"cmp"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var providerNameRe = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/providers/[^/]+$`)

var cryptoKeyRe = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+$`)

func newToken() string { return strings.ReplaceAll(uuid.NewString(), "-", "") }

// orderTriggers sorts by one of name, create_time or update_time, optionally "desc".
func orderTriggers(all []*eventarcpb.Trigger, orderBy string) error {
	cmpFn, err := orderFunc(orderBy, func(t *eventarcpb.Trigger, field string) (string, bool) {
		switch field {
		case fieldName:
			return t.GetName(), true
		case "create_time":
			return timeKey(t.GetCreateTime()), true
		case "update_time":
			return timeKey(t.GetUpdateTime()), true
		}

		return "", false
	})
	if err != nil {
		return err
	}

	slices.SortStableFunc(all, cmpFn)

	return nil
}

func timeKey(t *timestamppb.Timestamp) string {
	return t.AsTime().UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func orderFunc[T any](orderBy string, key func(T, string) (string, bool)) (func(a, b T) int, error) {
	field, desc := fieldName, false

	if orderBy != "" {
		f, dir, _ := strings.Cut(strings.TrimSpace(orderBy), " ")
		field = f

		switch strings.ToLower(dir) {
		case "":
		case "desc":
			desc = true
		case "asc":
		default:
			return nil, errInvalid("Invalid order_by %q.", orderBy)
		}
	}

	var zero T
	if _, ok := key(zero, field); !ok {
		return nil, errInvalid("Invalid order_by field %q.", field)
	}

	return func(a, b T) int {
		ka, _ := key(a, field)
		kb, _ := key(b, field)

		c := cmp.Compare(ka, kb)
		if desc {
			return -c
		}

		return c
	}, nil
}

func validateChannel(c *eventarcpb.Channel) error {
	if p := c.GetProvider(); p != "" && !providerNameRe.MatchString(p) {
		return errInvalid("provider %q must be projects/{project}/locations/{location}/providers/{provider}.", p)
	}

	if k := c.GetCryptoKeyName(); k != "" && !cryptoKeyRe.MatchString(k) {
		return errInvalid("crypto_key_name %q must be projects/*/locations/*/keyRings/*/cryptoKeys/*.", k)
	}

	return validateLabels(c.GetLabels())
}

// CreateChannel creates a third-party channel with its Pub/Sub topic; it starts PENDING until a connection
// presents its activation token.
func (b *InMemoryBackend) CreateChannel(in *eventarcpb.CreateChannelRequest) (*longrunningpb.Operation, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if !knownLocation(location) {
		return nil, errInvalid("Location %q is not supported by Eventarc.", location)
	}

	if !channelIDRe.MatchString(in.GetChannelId()) {
		return nil, errInvalid(
			"Invalid channel id %q: use 3-63 lowercase letters, digits and hyphens, starting with a letter.",
			in.GetChannelId(),
		)
	}

	if in.GetChannel() == nil {
		return nil, errInvalid("channel is required.")
	}

	if err = validateChannel(in.GetChannel()); err != nil {
		return nil, err
	}

	ch := clone(in.GetChannel())
	ch.Name = childName(project, location, kindChannels, in.GetChannelId())
	ch.Uid = uuid.NewString()
	ch.CreateTime = b.ts()
	ch.UpdateTime = ch.GetCreateTime()
	ch.State = eventarcpb.Channel_PENDING
	ch.ActivationToken = newToken()
	ch.SatisfiesPzs = false
	topic := "projects/" + project + "/topics/eventarc-channel-" + location + "-" + in.GetChannelId() + "-" + shortID()
	ch.Transport = &eventarcpb.Channel_PubsubTopic{PubsubTopic: topic}

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	if _, ok := b.channels[ch.GetName()]; ok {
		return nil, errExists("Channel %q already exists.", ch.GetName())
	}

	if !in.GetValidateOnly() {
		if b.svc.PubSub != nil {
			if _, err = b.svc.PubSub.CreateTopic(&pubsubpb.Topic{Name: topic}); err != nil {
				return nil, errPrecondition("Could not create channel topic %q: %v", topic, err)
			}
		}

		b.channels[ch.GetName()] = &channelRec{pb: ch}
	}

	return b.finish(project, location, ch.GetName(), "create", clone(ch))
}

// GetChannel returns a channel.
func (b *InMemoryBackend) GetChannel(name string) (*eventarcpb.Channel, error) {
	if _, _, _, err := parseChild(name, kindChannels); err != nil {
		return nil, err
	}

	b.mu.RLock("GetChannel")
	defer b.mu.RUnlock()

	rec, ok := b.channels[name]
	if !ok {
		return nil, errNotFound("Channel %q not found.", name)
	}

	return clone(rec.pb), nil
}

// ListChannels lists the channels of a location, or of every location for "-".
func (b *InMemoryBackend) ListChannels(in *eventarcpb.ListChannelsRequest) (*eventarcpb.ListChannelsResponse, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	cmpFn, err := orderFunc(in.GetOrderBy(), func(c *eventarcpb.Channel, field string) (string, bool) {
		switch field {
		case fieldName:
			return c.GetName(), true
		case "create_time":
			return timeKey(c.GetCreateTime()), true
		case "update_time":
			return timeKey(c.GetUpdateTime()), true
		}

		return "", false
	})
	if err != nil {
		return nil, err
	}

	b.mu.RLock("ListChannels")

	var all []*eventarcpb.Channel

	for _, rec := range b.channels {
		if inScope(rec.pb.GetName(), project, location) {
			all = append(all, clone(rec.pb))
		}
	}

	b.mu.RUnlock()

	slices.SortStableFunc(all, cmpFn)

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &eventarcpb.ListChannelsResponse{Channels: pg.Data, NextPageToken: pg.Next}, nil
}

// UpdateChannel updates crypto_key_name and labels.
func (b *InMemoryBackend) UpdateChannel(in *eventarcpb.UpdateChannelRequest) (*longrunningpb.Operation, error) {
	src := in.GetChannel()

	project, location, _, err := parseChild(src.GetName(), kindChannels)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	rec, ok := b.channels[src.GetName()]
	if !ok {
		return nil, errNotFound("Channel %q not found.", src.GetName())
	}

	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{maskCryptoKey, maskLabels}
	}

	next := clone(rec.pb)

	for _, p := range paths {
		switch p {
		case updateMaskAll:
			next.CryptoKeyName, next.Labels = src.GetCryptoKeyName(), src.GetLabels()
		case maskCryptoKey:
			next.CryptoKeyName = src.GetCryptoKeyName()
		case maskLabels:
			next.Labels = src.GetLabels()
		default:
			return nil, errInvalid("Unsupported update_mask path %q for Channel.", p)
		}
	}

	if err = validateChannel(next); err != nil {
		return nil, err
	}

	next.UpdateTime = b.ts()

	if !in.GetValidateOnly() {
		rec.pb = next
	}

	return b.finish(project, location, next.GetName(), "update", clone(next))
}

// DeleteChannel removes a channel, its topic and its connections; triggers still on the channel block it.
func (b *InMemoryBackend) DeleteChannel(in *eventarcpb.DeleteChannelRequest) (*longrunningpb.Operation, error) {
	project, location, _, err := parseChild(in.GetName(), kindChannels)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	rec, ok := b.channels[in.GetName()]
	if !ok {
		return nil, errNotFound("Channel %q not found.", in.GetName())
	}

	for _, t := range b.triggers {
		if t.pb.GetChannel() == in.GetName() {
			return nil, errPrecondition("Channel %q is used by trigger %q.", in.GetName(), t.pb.GetName())
		}
	}

	if !in.GetValidateOnly() {
		if b.svc.PubSub != nil {
			_ = b.svc.PubSub.DeleteTopic(rec.pb.GetPubsubTopic())
		}

		for name, c := range b.conns {
			if c.GetChannel() == in.GetName() {
				delete(b.conns, name)
			}
		}

		delete(b.channels, in.GetName())
	}

	return b.finish(project, location, in.GetName(), "delete", clone(rec.pb))
}

// CreateChannelConnection activates a channel by presenting its activation token.
func (b *InMemoryBackend) CreateChannelConnection(
	in *eventarcpb.CreateChannelConnectionRequest,
) (*longrunningpb.Operation, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if !knownLocation(location) {
		return nil, errInvalid("Location %q is not supported by Eventarc.", location)
	}

	if !connIDRe.MatchString(in.GetChannelConnectionId()) {
		return nil, errInvalid("Invalid channel connection id %q.", in.GetChannelConnectionId())
	}

	src := in.GetChannelConnection()
	if src == nil {
		return nil, errInvalid("channel_connection is required.")
	}

	if _, _, _, err = parseChild(src.GetChannel(), kindChannels); err != nil {
		return nil, err
	}

	if src.GetActivationToken() == "" {
		return nil, errInvalid("channel_connection.activation_token is required.")
	}

	name := childName(project, location, "channelConnections", in.GetChannelConnectionId())

	b.mu.Lock("CreateChannelConnection")
	defer b.mu.Unlock()

	if _, ok := b.conns[name]; ok {
		return nil, errExists("Channel connection %q already exists.", name)
	}

	ch, ok := b.channels[src.GetChannel()]
	if !ok {
		return nil, errNotFound("Channel %q not found.", src.GetChannel())
	}

	if ch.pb.GetState() != eventarcpb.Channel_PENDING || ch.pb.GetActivationToken() != src.GetActivationToken() {
		return nil, errInvalid("The activation token is not valid for channel %q.", src.GetChannel())
	}

	conn := &eventarcpb.ChannelConnection{
		Name: name, Uid: uuid.NewString(), Channel: src.GetChannel(), CreateTime: b.ts(),
		ActivationToken: src.GetActivationToken(), Labels: src.GetLabels(),
	}
	conn.UpdateTime = conn.GetCreateTime()

	if err = validateLabels(conn.GetLabels()); err != nil {
		return nil, err
	}

	b.conns[name] = conn
	ch.pb.State, ch.pb.ActivationToken, ch.pb.UpdateTime = eventarcpb.Channel_ACTIVE, "", b.ts()

	return b.finish(project, location, name, "create", clone(conn))
}

// GetChannelConnection returns a channel connection.
func (b *InMemoryBackend) GetChannelConnection(name string) (*eventarcpb.ChannelConnection, error) {
	if _, _, _, err := parseChild(name, "channelConnections"); err != nil {
		return nil, err
	}

	b.mu.RLock("GetChannelConnection")
	defer b.mu.RUnlock()

	c, ok := b.conns[name]
	if !ok {
		return nil, errNotFound("Channel connection %q not found.", name)
	}

	return clone(c), nil
}

// ListChannelConnections lists the connections of a location, or of every location for "-".
func (b *InMemoryBackend) ListChannelConnections(
	in *eventarcpb.ListChannelConnectionsRequest,
) (*eventarcpb.ListChannelConnectionsResponse, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListChannelConnections")

	var all []*eventarcpb.ChannelConnection

	for name, c := range b.conns {
		p, l, _, perr := parseChild(name, "channelConnections")
		if perr == nil && p == project && (location == wildcardLocation || l == location) {
			all = append(all, clone(c))
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, b *eventarcpb.ChannelConnection) int { return cmp.Compare(a.GetName(), b.GetName()) })

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &eventarcpb.ListChannelConnectionsResponse{ChannelConnections: pg.Data, NextPageToken: pg.Next}, nil
}

// DeleteChannelConnection removes a connection; its channel goes INACTIVE.
func (b *InMemoryBackend) DeleteChannelConnection(
	in *eventarcpb.DeleteChannelConnectionRequest,
) (*longrunningpb.Operation, error) {
	project, location, _, err := parseChild(in.GetName(), "channelConnections")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteChannelConnection")
	defer b.mu.Unlock()

	c, ok := b.conns[in.GetName()]
	if !ok {
		return nil, errNotFound("Channel connection %q not found.", in.GetName())
	}

	delete(b.conns, in.GetName())

	if ch, found := b.channels[c.GetChannel()]; found {
		ch.pb.State, ch.pb.UpdateTime = eventarcpb.Channel_INACTIVE, b.ts()
	}

	return b.finish(project, location, in.GetName(), "delete", clone(c))
}

func configName(project, location string) string {
	return locationName(project, location) + "/" + googleChannelCfg
}

// GetGoogleChannelConfig returns the location's config, an empty one until it is first updated.
func (b *InMemoryBackend) GetGoogleChannelConfig(name string) (*eventarcpb.GoogleChannelConfig, error) {
	project, location, err := parseConfigName(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetGoogleChannelConfig")
	defer b.mu.RUnlock()

	if c, ok := b.configs[configName(project, location)]; ok {
		return clone(c), nil
	}

	return &eventarcpb.GoogleChannelConfig{Name: configName(project, location)}, nil
}

func parseConfigName(name string) (string, string, error) {
	ids, ok := parseRes(strings.TrimSuffix(name, "/"+googleChannelCfg), "projects", "locations")
	if !ok || !strings.HasSuffix(name, "/"+googleChannelCfg) {
		return "", "", errInvalid(
			"Invalid name %q: want projects/{project}/locations/{location}/%s.",
			name,
			googleChannelCfg,
		)
	}

	if !knownLocation(ids[1]) {
		return "", "", errInvalid("Location %q is not supported by Eventarc.", ids[1])
	}

	return ids[0], ids[1], nil
}

// UpdateGoogleChannelConfig sets crypto_key_name and labels.
func (b *InMemoryBackend) UpdateGoogleChannelConfig(
	in *eventarcpb.UpdateGoogleChannelConfigRequest,
) (*eventarcpb.GoogleChannelConfig, error) {
	src := in.GetGoogleChannelConfig()

	project, location, err := parseConfigName(src.GetName())
	if err != nil {
		return nil, err
	}

	if k := src.GetCryptoKeyName(); k != "" && !cryptoKeyRe.MatchString(k) {
		return nil, errInvalid("crypto_key_name %q must be projects/*/locations/*/keyRings/*/cryptoKeys/*.", k)
	}

	if err = validateLabels(src.GetLabels()); err != nil {
		return nil, err
	}

	name := configName(project, location)

	b.mu.Lock("UpdateGoogleChannelConfig")
	defer b.mu.Unlock()

	next := &eventarcpb.GoogleChannelConfig{Name: name}
	if cur, ok := b.configs[name]; ok {
		next = clone(cur)
	}

	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{maskCryptoKey, maskLabels}
	}

	for _, p := range paths {
		switch p {
		case updateMaskAll:
			next.CryptoKeyName, next.Labels = src.GetCryptoKeyName(), src.GetLabels()
		case maskCryptoKey:
			next.CryptoKeyName = src.GetCryptoKeyName()
		case maskLabels:
			next.Labels = src.GetLabels()
		default:
			return nil, errInvalid("Unsupported update_mask path %q for GoogleChannelConfig.", p)
		}
	}

	next.UpdateTime = b.ts()
	b.configs[name] = next

	return clone(next), nil
}
