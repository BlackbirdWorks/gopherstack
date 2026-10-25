package gcpbigtable

import (
	"strconv"
	"strings"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const defaultProfilePage = 100

func (b *InMemoryBackend) profileEtag() string { return strconv.FormatUint(b.nextSeq(), 10) }

func validateRouting(rec *instRec, p *adminpb.AppProfile) error {
	switch rt := p.GetRoutingPolicy().(type) {
	case *adminpb.AppProfile_SingleClusterRouting_:
		if _, ok := rec.clusters[rt.SingleClusterRouting.GetClusterId()]; !ok {
			return errInvalid("Cluster %q does not exist in the instance.", rt.SingleClusterRouting.GetClusterId())
		}
	case *adminpb.AppProfile_MultiClusterRoutingUseAny_:
		for _, id := range rt.MultiClusterRoutingUseAny.GetClusterIds() {
			if _, ok := rec.clusters[id]; !ok {
				return errInvalid("Cluster %q does not exist in the instance.", id)
			}
		}
	default:
		return errInvalid("A routing policy must be set.")
	}

	return nil
}

// CreateAppProfile creates an app profile.
func (b *InMemoryBackend) CreateAppProfile(in *adminpb.CreateAppProfileRequest) (*adminpb.AppProfile, error) {
	if !profileIDRe.MatchString(in.GetAppProfileId()) {
		return nil, errInvalid("Invalid app profile id %q.", in.GetAppProfileId())
	}

	if in.GetAppProfile() == nil {
		return nil, errInvalid("app_profile is required.")
	}

	b.mu.Lock("CreateAppProfile")
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = validateRouting(rec, in.GetAppProfile()); err != nil {
		return nil, err
	}

	name := profileName(in.GetParent(), in.GetAppProfileId())
	if _, exists := rec.profiles[in.GetAppProfileId()]; exists {
		return nil, errExists("App profile %q already exists.", name)
	}

	pb := clone(in.GetAppProfile())
	pb.Name, pb.Etag = name, b.profileEtag()
	rec.profiles[in.GetAppProfileId()] = pb

	return clone(pb), nil
}

func (b *InMemoryBackend) profileLocked(name string) (*instRec, *adminpb.AppProfile, error) {
	inst, id, err := parseProfile(name)
	if err != nil {
		return nil, nil, err
	}

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, nil, err
	}

	pb, ok := rec.profiles[id]
	if !ok {
		return nil, nil, errNotFound("App profile %q not found.", name)
	}

	return rec, pb, nil
}

// GetAppProfile returns an app profile.
func (b *InMemoryBackend) GetAppProfile(name string) (*adminpb.AppProfile, error) {
	b.mu.RLock("GetAppProfile")
	defer b.mu.RUnlock()

	_, pb, err := b.profileLocked(name)
	if err != nil {
		return nil, err
	}

	return clone(pb), nil
}

// ListAppProfiles lists the app profiles of an instance.
func (b *InMemoryBackend) ListAppProfiles(in *adminpb.ListAppProfilesRequest) (*adminpb.ListAppProfilesResponse, error) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListAppProfiles")
	defer b.mu.RUnlock()

	rec, err := b.instanceLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	all := make([]*adminpb.AppProfile, 0, len(rec.profiles))
	for _, id := range collections.SortedKeys(rec.profiles) {
		all = append(all, clone(rec.profiles[id]))
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultProfilePage)

	return &adminpb.ListAppProfilesResponse{AppProfiles: pg.Data, NextPageToken: pg.Next}, nil
}

// UpdateAppProfile updates the masked fields of an app profile; an empty mask replaces every field.
func (b *InMemoryBackend) UpdateAppProfile(in *adminpb.UpdateAppProfileRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("UpdateAppProfile")
	defer b.mu.Unlock()

	rec, cur, err := b.profileLocked(in.GetAppProfile().GetName())
	if err != nil {
		return nil, err
	}

	if e := in.GetAppProfile().GetEtag(); e != "" && e != cur.GetEtag() {
		return nil, errAborted("The app profile was modified since it was read; read it again.")
	}

	next := clone(cur)
	src := in.GetAppProfile()

	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{"description", "single_cluster_routing", "standard_isolation"}
		next.Isolation = clone(src).Isolation
		next.RoutingPolicy = clone(src).RoutingPolicy
		next.Description = src.GetDescription()
	}

	for _, p := range paths {
		switch {
		case p == "description":
			next.Description = src.GetDescription()
		case p == "multi_cluster_routing_use_any" || strings.HasPrefix(p, "multi_cluster_routing_use_any."),
			p == "single_cluster_routing" || strings.HasPrefix(p, "single_cluster_routing."):
			next.RoutingPolicy = clone(src).RoutingPolicy
		case p == "standard_isolation" || p == "data_boost_isolation_read_only" || p == "priority":
			next.Isolation = clone(src).Isolation
		default:
			return nil, errInvalid("Unsupported update_mask path %q for AppProfile.", p)
		}
	}

	if err = validateRouting(rec, next); err != nil {
		return nil, err
	}

	next.Etag = b.profileEtag()
	rec.profiles[lastSegment(next.GetName())] = next

	return b.finish(rec.pb.GetName(), &adminpb.UpdateAppProfileMetadata{}, clone(next))
}

// DeleteAppProfile deletes an app profile.
func (b *InMemoryBackend) DeleteAppProfile(in *adminpb.DeleteAppProfileRequest) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteAppProfile")
	defer b.mu.Unlock()

	rec, pb, err := b.profileLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	delete(rec.profiles, lastSegment(pb.GetName()))

	return &emptypb.Empty{}, nil
}
