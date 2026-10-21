package gcplogging

import (
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func cloneBucket(bd *bucketData) *loggingpb.LogBucket {
	out, _ := proto.Clone(bd.pb).(*loggingpb.LogBucket)

	return out
}

func validateBucketFields(bk *loggingpb.LogBucket) error {
	if bk.GetRetentionDays() < 0 || bk.GetRetentionDays() > maxRetentionDays {
		return errInvalid("retention_days must be between 1 and %d.", maxRetentionDays)
	}

	if len(bk.GetDescription()) > maxDescriptionLen {
		return errInvalid("Bucket description is too long.")
	}

	return nil
}

// purgeDeletedLocked removes buckets whose undelete window has passed.
func (b *InMemoryBackend) purgeDeletedLocked() {
	cutoff := b.now().Add(-undeleteWindow)

	for name, bd := range b.buckets {
		if bd.deleteRequested.IsZero() || bd.deleteRequested.After(cutoff) {
			continue
		}

		delete(b.buckets, name)

		for vn := range b.views {
			if strings.HasPrefix(vn, name+"/views/") {
				delete(b.views, vn)
			}
		}
	}
}

// CreateBucket adds a user-defined log bucket.
func (b *InMemoryBackend) CreateBucket(req *loggingpb.CreateBucketRequest) (*loggingpb.LogBucket, error) {
	m := locParentRe.FindStringSubmatch(req.GetParent())
	if m == nil {
		return nil, errInvalid("Invalid parent [%s]: expected [PARENT]/locations/[LOCATION].", req.GetParent())
	}

	if !bucketIDRe.MatchString(req.GetBucketId()) {
		return nil, errInvalid("Invalid bucket id [%s].", req.GetBucketId())
	}

	in := req.GetBucket()
	if in == nil {
		in = &loggingpb.LogBucket{}
	}

	if err := validateBucketFields(in); err != nil {
		return nil, err
	}

	b.seed(m[1])
	b.mu.Lock("CreateBucket")
	defer b.mu.Unlock()

	b.purgeDeletedLocked()

	name := bucketName(m[1], m[2], req.GetBucketId())
	if _, ok := b.buckets[name]; ok {
		return nil, errExists("Bucket %s already exists.", name)
	}

	now := timestamppb.New(b.now())
	pb, _ := proto.Clone(in).(*loggingpb.LogBucket)
	pb.Name, pb.CreateTime, pb.UpdateTime = name, now, now
	pb.LifecycleState = loggingpb.LifecycleState_ACTIVE

	if pb.GetRetentionDays() == 0 {
		pb.RetentionDays = defaultRetentionDays
	}

	bd := newBucketData(pb)
	b.buckets[name] = bd
	b.putAllLogsView(name, now)

	return cloneBucket(bd), nil
}

func checkBucketName(name string) error {
	if !bucketRe.MatchString(name) {
		return errInvalid("Invalid bucket name [%s]: expected [PARENT]/locations/[LOCATION]/buckets/[BUCKET].", name)
	}

	return nil
}

// GetBucket returns one bucket.
func (b *InMemoryBackend) GetBucket(name string) (*loggingpb.LogBucket, error) {
	if err := checkBucketName(name); err != nil {
		return nil, err
	}

	b.seed(parentOf(name))
	b.mu.Lock("GetBucket")
	defer b.mu.Unlock()

	b.purgeDeletedLocked()

	bd, ok := b.buckets[name]
	if !ok {
		return nil, errNotFound("Bucket %s does not exist.", name)
	}

	return cloneBucket(bd), nil
}

// ListBuckets returns the buckets of a location ("-" lists every location), ordered by name.
func (b *InMemoryBackend) ListBuckets(req *loggingpb.ListBucketsRequest) ([]*loggingpb.LogBucket, string, error) {
	m := locParentRe.FindStringSubmatch(req.GetParent())
	if m == nil {
		return nil, "", errInvalid("Invalid parent [%s]: expected [PARENT]/locations/[LOCATION].", req.GetParent())
	}

	if err := pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	b.seed(m[1])
	b.mu.Lock("ListBuckets")
	b.purgeDeletedLocked()

	var all []*loggingpb.LogBucket

	for name, bd := range b.buckets {
		bm := bucketRe.FindStringSubmatch(name)
		if bm != nil && bm[1] == m[1] && (m[2] == "-" || bm[2] == m[2]) {
			all = append(all, cloneBucket(bd))
		}
	}

	b.mu.Unlock()

	slices.SortFunc(all, func(a, c *loggingpb.LogBucket) int { return strings.Compare(a.GetName(), c.GetName()) })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

func pageTokenOK(token string) error {
	if err := page.ValidateToken(token); err != nil {
		return errInvalid("Invalid page_token.")
	}

	return nil
}

func applyBucketMask(cur, src *loggingpb.LogBucket, paths []string) error {
	for _, p := range paths {
		switch p {
		case pathDescription:
			cur.Description = src.GetDescription()
		case "retention_days":
			if cur.GetLocked() && src.GetRetentionDays() < cur.GetRetentionDays() {
				return errPrecondition("Retention of a locked bucket cannot be reduced.")
			}

			cur.RetentionDays = src.GetRetentionDays()
		case "locked":
			if cur.GetLocked() && !src.GetLocked() {
				return errPrecondition("A locked bucket cannot be unlocked.")
			}

			cur.Locked = src.GetLocked()
		case "restricted_fields":
			cur.RestrictedFields = src.GetRestrictedFields()
		case "index_configs":
			cur.IndexConfigs = src.GetIndexConfigs()
		case "analytics_enabled":
			cur.AnalyticsEnabled = src.GetAnalyticsEnabled()
		case "cmek_settings":
			cur.CmekSettings = src.GetCmekSettings()
		default:
			return errInvalid("Invalid update_mask path [%s].", p)
		}
	}

	return nil
}

// UpdateBucket applies the masked fields to a bucket.
func (b *InMemoryBackend) UpdateBucket(req *loggingpb.UpdateBucketRequest) (*loggingpb.LogBucket, error) {
	name := req.GetName()
	if err := checkBucketName(name); err != nil {
		return nil, err
	}

	if len(req.GetUpdateMask().GetPaths()) == 0 {
		return nil, errInvalid("update_mask must list the fields to change.")
	}

	if req.GetBucket() == nil {
		return nil, errInvalid("bucket is required.")
	}

	b.seed(parentOf(name))
	b.mu.Lock("UpdateBucket")
	defer b.mu.Unlock()

	b.purgeDeletedLocked()

	bd, ok := b.buckets[name]
	if !ok {
		return nil, errNotFound("Bucket %s does not exist.", name)
	}

	if strings.HasSuffix(name, "/buckets/"+requiredBucketID) {
		return nil, errInvalid("The _Required bucket cannot be updated.")
	}

	if !bd.deleteRequested.IsZero() {
		return nil, errPrecondition("Bucket %s is pending deletion.", name)
	}

	next := cloneBucket(bd)
	if err := applyBucketMask(next, req.GetBucket(), req.GetUpdateMask().GetPaths()); err != nil {
		return nil, err
	}

	if err := validateBucketFields(next); err != nil {
		return nil, err
	}

	if next.GetRetentionDays() == 0 {
		next.RetentionDays = defaultRetentionDays
	}

	next.UpdateTime = timestamppb.New(b.now())
	bd.pb = next
	b.expireLocked(bd)

	return cloneBucket(bd), nil
}

// DeleteBucket marks a bucket DELETE_REQUESTED; it can be undeleted for seven days.
func (b *InMemoryBackend) DeleteBucket(name string) error {
	if err := checkBucketName(name); err != nil {
		return err
	}

	b.seed(parentOf(name))
	b.mu.Lock("DeleteBucket")
	defer b.mu.Unlock()

	b.purgeDeletedLocked()

	bd, ok := b.buckets[name]
	if !ok {
		return errNotFound("Bucket %s does not exist.", name)
	}

	switch {
	case strings.HasSuffix(name, "/buckets/"+defaultBucketID), strings.HasSuffix(name, "/buckets/"+requiredBucketID):
		return errInvalid("The %s bucket cannot be deleted.", name[strings.LastIndex(name, "/")+1:])
	case bd.pb.GetLocked():
		return errPrecondition("A locked bucket cannot be deleted.")
	}

	now := b.now()
	bd.deleteRequested = now
	bd.pb.LifecycleState = loggingpb.LifecycleState_DELETE_REQUESTED
	bd.pb.UpdateTime = timestamppb.New(now)

	return nil
}

// UndeleteBucket restores a bucket that is pending deletion.
func (b *InMemoryBackend) UndeleteBucket(name string) error {
	if err := checkBucketName(name); err != nil {
		return err
	}

	b.mu.Lock("UndeleteBucket")
	defer b.mu.Unlock()

	b.purgeDeletedLocked()

	bd, ok := b.buckets[name]
	if !ok {
		return errNotFound("Bucket %s does not exist.", name)
	}

	if bd.deleteRequested.IsZero() {
		return errPrecondition("Bucket %s is not pending deletion.", name)
	}

	bd.deleteRequested = time.Time{}
	bd.pb.LifecycleState = loggingpb.LifecycleState_ACTIVE
	bd.pb.UpdateTime = timestamppb.New(b.now())

	return nil
}
