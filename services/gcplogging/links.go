package gcplogging

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const destBigQueryDataset = "bigquery.googleapis.com/projects/"

var (
	linkIDRe = regexp.MustCompile(`^[A-Za-z0-9_]{1,100}$`)

	// ErrDatasetExists is returned by a DatasetStore when the dataset is already there.
	ErrDatasetExists = errors.New("dataset already exists")
)

// DatasetStore is the slice of BigQuery that linked datasets need.
type DatasetStore interface {
	CreateDataset(project, dataset string) error
	DeleteDataset(project, dataset string) error
}

func linkParent(name string) (string, string, bool) {
	before, after, ok := strings.CutLast(name, "/links/")
	if !ok {
		return "", "", false
	}

	return before, after, bucketRe.MatchString(before) && linkIDRe.MatchString(after)
}

func (b *InMemoryBackend) liveBucketLocked(name string) (*bucketData, error) {
	if err := checkBucketName(name); err != nil {
		return nil, err
	}

	b.purgeDeletedLocked()

	bd, ok := b.buckets[name]
	if !ok || !bd.deleteRequested.IsZero() {
		return nil, errNotFound("Bucket %s does not exist.", name)
	}

	return bd, nil
}

func (b *InMemoryBackend) datasetStore() DatasetStore {
	b.mu.RLock("datasetStore")
	defer b.mu.RUnlock()

	ds, _ := b.tables.(DatasetStore)

	return ds
}

func projectOfName(name string) string {
	rest, ok := strings.CutPrefix(name, "projects/")
	if !ok {
		return ""
	}

	id, _, _ := strings.Cut(rest, "/")

	return id
}

// CreateLink links a bucket to a BigQuery dataset of the same name as the link.
func (b *InMemoryBackend) CreateLink(req *loggingpb.CreateLinkRequest) (*longrunningpb.Operation, error) {
	if !linkIDRe.MatchString(req.GetLinkId()) {
		return nil, errInvalid(
			"Invalid link id [%s]: use letters, digits and underscores, at most 100 characters.",
			req.GetLinkId(),
		)
	}

	if len(req.GetLink().GetDescription()) > maxDescriptionLen {
		return nil, errInvalid("Link description is too long.")
	}

	link, err := b.createLinkRec(req)
	if err != nil {
		return nil, err
	}

	meta := &loggingpb.LinkMetadata{Request: &loggingpb.LinkMetadata_CreateLinkRequest{CreateLinkRequest: req}}

	return b.finishLinkOp(parentLocation(req.GetParent()), meta, link)
}

func parentLocation(bucket string) string {
	if before, _, ok := strings.Cut(bucket, "/buckets/"); ok {
		return before
	}

	return bucket
}

func (b *InMemoryBackend) createLinkRec(req *loggingpb.CreateLinkRequest) (*loggingpb.Link, error) {
	ds := b.datasetStore()

	b.mu.Lock("CreateLink")
	defer b.mu.Unlock()

	bd, err := b.liveBucketLocked(req.GetParent())
	if err != nil {
		return nil, err
	}

	if !bd.pb.GetAnalyticsEnabled() {
		return nil, errPrecondition(
			"Bucket %s must be upgraded to use Log Analytics before it can be linked.",
			req.GetParent(),
		)
	}

	name := req.GetParent() + "/links/" + req.GetLinkId()
	if _, ok := b.links[name]; ok {
		return nil, errExists("Link %s already exists.", name)
	}

	project := projectOfName(req.GetParent())

	if ds != nil {
		if cerr := ds.CreateDataset(project, req.GetLinkId()); cerr != nil {
			if errors.Is(cerr, ErrDatasetExists) {
				return nil, errExists("BigQuery dataset %s already exists in project %s.", req.GetLinkId(), project)
			}

			return nil, errInvalid("Cannot create the linked dataset: %v", cerr)
		}
	}

	link := &loggingpb.Link{
		Name: name, Description: req.GetLink().GetDescription(), CreateTime: timestamppb.New(b.now()),
		LifecycleState: loggingpb.LifecycleState_ACTIVE,
		BigqueryDataset: &loggingpb.BigQueryDataset{
			DatasetId: destBigQueryDataset + project + "/datasets/" + req.GetLinkId(),
		},
	}
	b.links[name] = link

	return cloneLink(link), nil
}

func cloneLink(l *loggingpb.Link) *loggingpb.Link {
	out, _ := proto.Clone(l).(*loggingpb.Link)

	return out
}

func (b *InMemoryBackend) finishLinkOp(
	location string, meta *loggingpb.LinkMetadata, resp proto.Message,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("linkOp")
	n := b.nextSeq()
	b.mu.Unlock()

	now := timestamppb.New(b.now())
	meta.StartTime, meta.EndTime = now, now
	meta.State = loggingpb.OperationState_OPERATION_STATE_SUCCEEDED

	op, err := gcplro.Done(fmt.Sprintf("%s/operations/link-%d", location, n), meta, resp)
	if err != nil {
		return nil, errInvalid("operation: %v", err)
	}

	return b.ops.Put(op), nil
}

// DeleteLink removes a link and its BigQuery dataset.
func (b *InMemoryBackend) DeleteLink(req *loggingpb.DeleteLinkRequest) (*longrunningpb.Operation, error) {
	bucket, id, ok := linkParent(req.GetName())
	if !ok {
		return nil, errInvalid("Invalid link name [%s]: expected [BUCKET]/links/[LINK].", req.GetName())
	}

	ds := b.datasetStore()

	b.mu.Lock("DeleteLink")

	if _, ok = b.links[req.GetName()]; !ok {
		b.mu.Unlock()

		return nil, errNotFound("Link %s does not exist.", req.GetName())
	}

	delete(b.links, req.GetName())
	b.mu.Unlock()

	if ds != nil {
		_ = ds.DeleteDataset(projectOfName(bucket), id)
	}

	meta := &loggingpb.LinkMetadata{Request: &loggingpb.LinkMetadata_DeleteLinkRequest{DeleteLinkRequest: req}}

	return b.finishLinkOp(parentLocation(bucket), meta, &emptypb.Empty{})
}

// GetLink returns one link.
func (b *InMemoryBackend) GetLink(name string) (*loggingpb.Link, error) {
	if _, _, ok := linkParent(name); !ok {
		return nil, errInvalid("Invalid link name [%s]: expected [BUCKET]/links/[LINK].", name)
	}

	b.mu.RLock("GetLink")
	defer b.mu.RUnlock()

	l, ok := b.links[name]
	if !ok {
		return nil, errNotFound("Link %s does not exist.", name)
	}

	return cloneLink(l), nil
}

// ListLinks lists the links of a bucket ordered by name.
func (b *InMemoryBackend) ListLinks(req *loggingpb.ListLinksRequest) ([]*loggingpb.Link, string, error) {
	if req.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	b.mu.Lock("ListLinks")
	defer b.mu.Unlock()

	if _, err := b.liveBucketLocked(req.GetParent()); err != nil {
		return nil, "", err
	}

	var all []*loggingpb.Link

	for name, l := range b.links {
		if strings.HasPrefix(name, req.GetParent()+"/links/") {
			all = append(all, cloneLink(l))
		}
	}

	slices.SortFunc(all, func(a, c *loggingpb.Link) int { return strings.Compare(a.GetName(), c.GetName()) })

	pg := page.New(all, req.GetPageToken(), int(req.GetPageSize()), defaultEntriesPage)

	return pg.Data, pg.Next, nil
}

func (b *InMemoryBackend) bucketHasLinksLocked(bucket string) bool {
	for name := range b.links {
		if strings.HasPrefix(name, bucket+"/links/") {
			return true
		}
	}

	return false
}
