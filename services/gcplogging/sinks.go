package gcplogging

import (
	"fmt"
	"hash/fnv"
	"slices"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	sharedWriterIdentity = "serviceAccount:cloud-logs@system.gserviceaccount.com"
	maxSinkExclusions    = 50
	sinkSuffixMod        = 1_000_000
	maxDescriptionLen    = 8192
)

func cloneSink(s *loggingpb.LogSink) *loggingpb.LogSink {
	out, _ := proto.Clone(s).(*loggingpb.LogSink)

	return out
}

func kindLetter(parent string) string {
	switch {
	case strings.HasPrefix(parent, "folders/"):
		return "f"
	case strings.HasPrefix(parent, "organizations/"):
		return "o"
	case strings.HasPrefix(parent, "billingAccounts/"):
		return "b"
	}

	return "p"
}

// writerIdentity derives a stable per-sink service account like the real unique identities.
func writerIdentity(parent, id string, unique bool) string {
	if !unique {
		return sharedWriterIdentity
	}

	h := fnv.New64a()
	_, _ = h.Write([]byte(parent))
	number := h.Sum64() % projectNumberMod

	_, _ = h.Write([]byte("/" + id))
	suffix := h.Sum64() % sinkSuffixMod

	return fmt.Sprintf(
		"serviceAccount:%s%012d-%06d@gcp-sa-logging.iam.gserviceaccount.com",
		kindLetter(parent),
		number,
		suffix,
	)
}

func validateDestination(dest string) error {
	switch {
	case strings.HasPrefix(dest, destPubSub):
		return matchDest(dest, destPubSub, 4, "topics", 2) //nolint:mnd // projects/P/topics/T
	case strings.HasPrefix(dest, destBigQuery):
		return matchDest(dest, destBigQuery, 4, "datasets", 2) //nolint:mnd // projects/P/datasets/D
	case strings.HasPrefix(dest, destStorage):
		if strings.Trim(strings.TrimPrefix(dest, destStorage), "/") == "" ||
			strings.Contains(strings.TrimPrefix(dest, destStorage), "/") {
			return errInvalid("Invalid Cloud Storage destination [%s]: expected storage.googleapis.com/[BUCKET].", dest)
		}

		return nil
	case strings.HasPrefix(dest, destLogging):
		rest := strings.TrimPrefix(dest, destLogging)
		if parentRe.MatchString(rest) || bucketRe.MatchString(rest) {
			return nil
		}
	}

	return errInvalid("Sink destination is not supported or malformed: [%s].", dest)
}

func matchDest(dest, prefix string, parts int, collection string, idx int) error {
	segs := strings.Split(strings.TrimPrefix(dest, prefix), "/")
	if len(segs) != parts || segs[0] != "projects" || segs[idx] != collection || segs[1] == "" || segs[parts-1] == "" {
		return errInvalid("Invalid destination [%s].", dest)
	}

	return nil
}

func validateSink(s *loggingpb.LogSink) error {
	if s == nil {
		return errInvalid("sink is required.")
	}

	if s.GetDestination() == "" {
		return errInvalid("Sink destination must be set.")
	}

	if err := validateDestination(s.GetDestination()); err != nil {
		return err
	}

	if len(s.GetDescription()) > maxDescriptionLen {
		return errInvalid("Sink description is too long.")
	}

	if _, err := compileFilter(s.GetFilter()); err != nil {
		return err
	}

	if len(s.GetExclusions()) > maxSinkExclusions {
		return errInvalid("A sink can have at most %d exclusions.", maxSinkExclusions)
	}

	for _, ex := range s.GetExclusions() {
		if _, err := compileFilter(ex.GetFilter()); err != nil {
			return err
		}
	}

	return nil
}

// CreateSink adds a sink under the parent.
func (b *InMemoryBackend) CreateSink(req *loggingpb.CreateSinkRequest) (*loggingpb.LogSink, error) {
	parent := req.GetParent()
	if err := checkParent(parent); err != nil {
		return nil, err
	}

	in := req.GetSink()
	if err := validateSink(in); err != nil {
		return nil, err
	}

	if err := checkResourceID("sink", in.GetName()); err != nil {
		return nil, err
	}

	b.seed(parent)
	b.mu.Lock("CreateSink")
	defer b.mu.Unlock()

	key := sinkName(parent, in.GetName())
	if _, ok := b.sinks[key]; ok {
		return nil, errExists("Sink %s already exists.", in.GetName())
	}

	now := timestamppb.New(b.now())
	sink := cloneSink(in)
	sink.WriterIdentity = writerIdentity(parent, in.GetName(), req.GetUniqueWriterIdentity())
	sink.CreateTime, sink.UpdateTime = now, now
	b.sinks[key] = sink

	return cloneSink(sink), nil
}

func sinkKey(name string) (string, string, error) {
	parent, id, err := childName(name, "sinks")
	if err != nil {
		return "", "", err
	}

	return parent, id, nil
}

// GetSink returns one sink.
func (b *InMemoryBackend) GetSink(name string) (*loggingpb.LogSink, error) {
	parent, _, err := sinkKey(name)
	if err != nil {
		return nil, err
	}

	b.seed(parent)
	b.mu.RLock("GetSink")
	defer b.mu.RUnlock()

	s, ok := b.sinks[name]
	if !ok {
		return nil, errNotFound("Sink %s does not exist.", name)
	}

	return cloneSink(s), nil
}

// ListSinks returns the parent's sinks ordered by name.
func (b *InMemoryBackend) ListSinks(req *loggingpb.ListSinksRequest) ([]*loggingpb.LogSink, string, error) {
	parent := req.GetParent()
	if err := checkParent(parent); err != nil {
		return nil, "", err
	}

	if err := page.ValidateToken(req.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	b.seed(parent)
	b.mu.RLock("ListSinks")

	var all []*loggingpb.LogSink

	for key, s := range b.sinks {
		if strings.HasPrefix(key, parent+"/sinks/") {
			all = append(all, cloneSink(s))
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c *loggingpb.LogSink) int { return strings.Compare(a.GetName(), c.GetName()) })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

const sinkMaskPaths = "destination, filter, description, disabled, exclusions, include_children, " +
	"bigquery_options, bigquery_options.use_partitioned_tables, output_version_format"

func applySinkMask(dst, src *loggingpb.LogSink, mask *fieldmaskpb.FieldMask) error {
	paths := mask.GetPaths()
	if len(paths) == 0 {
		paths = []string{
			pathDestination, pathFilter, pathDescription, pathDisabled, "exclusions", "include_children", pathBigQuery,
		}
	}

	for _, p := range paths {
		switch p {
		case pathDestination:
			dst.Destination = src.GetDestination()
		case pathFilter:
			dst.Filter = src.GetFilter()
		case pathDescription:
			dst.Description = src.GetDescription()
		case pathDisabled:
			dst.Disabled = src.GetDisabled()
		case "exclusions":
			dst.Exclusions = src.GetExclusions()
		case "include_children":
			dst.IncludeChildren = src.GetIncludeChildren()
		case "output_version_format":
			dst.OutputVersionFormat = src.GetOutputVersionFormat() //nolint:staticcheck // deprecated field is still accepted
		case pathBigQuery, pathBigQuery + ".use_partitioned_tables":
			dst.Options = src.GetOptions()
		default:
			return errInvalid("Invalid update_mask path [%s]: supported paths are %s.", p, sinkMaskPaths)
		}
	}

	return nil
}

// UpdateSink applies the masked fields of the request sink.
func (b *InMemoryBackend) UpdateSink(req *loggingpb.UpdateSinkRequest) (*loggingpb.LogSink, error) {
	parent, id, err := sinkKey(req.GetSinkName())
	if err != nil {
		return nil, err
	}

	if id == requiredBucketID {
		return nil, errInvalid("The _Required sink cannot be modified.")
	}

	if req.GetSink() == nil {
		return nil, errInvalid("sink is required.")
	}

	b.seed(parent)
	b.mu.Lock("UpdateSink")
	defer b.mu.Unlock()

	cur, ok := b.sinks[req.GetSinkName()]
	if !ok {
		return nil, errNotFound("Sink %s does not exist.", req.GetSinkName())
	}

	next := cloneSink(cur)
	if err = applySinkMask(next, req.GetSink(), req.GetUpdateMask()); err != nil {
		return nil, err
	}

	if err = validateSink(next); err != nil {
		return nil, err
	}

	if req.GetUniqueWriterIdentity() {
		next.WriterIdentity = writerIdentity(parent, id, true)
	}

	next.UpdateTime = timestamppb.New(b.now())
	b.sinks[req.GetSinkName()] = next

	return cloneSink(next), nil
}

// DeleteSink removes a sink.
func (b *InMemoryBackend) DeleteSink(name string) error {
	parent, id, err := sinkKey(name)
	if err != nil {
		return err
	}

	if id == requiredBucketID {
		return errInvalid("The _Required sink cannot be deleted.")
	}

	b.seed(parent)
	b.mu.Lock("DeleteSink")
	defer b.mu.Unlock()

	if _, ok := b.sinks[name]; !ok {
		return errNotFound("Sink %s does not exist.", name)
	}

	delete(b.sinks, name)

	return nil
}
