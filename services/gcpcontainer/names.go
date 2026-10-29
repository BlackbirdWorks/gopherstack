package gcpcontainer

import (
	"regexp"
	"slices"
	"strings"
)

const (
	apiHost  = "https://container.googleapis.com/v1"
	computeV = "https://www.googleapis.com/compute/v1"

	kindProjects  = "projects"
	kindLocations = "locations"
	kindClusters  = "clusters"
	kindNodePools = "nodePools"
	kindOps       = "operations"

	wildcard = "-"

	maxNameLen = 40
)

var nameRE = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)

// ref identifies a cluster, node pool or operation. project and location are always set.
type ref struct {
	project  string
	location string
	cluster  string
	pool     string
}

func (r ref) clusterKey() string { return r.project + "/" + r.location + "/" + r.cluster }

func (r ref) clusterName() string {
	return kindProjects + "/" + r.project + "/" + kindLocations + "/" + r.location + "/" + kindClusters + "/" + r.cluster
}

func (r ref) poolName() string { return r.clusterName() + "/" + kindNodePools + "/" + r.pool }

func locationPath(project, location string) string {
	return kindProjects + "/" + project + "/" + kindLocations + "/" + location
}

func clusterSelfLink(project, location, cluster string) string {
	return apiHost + "/" + locationPath(project, location) + "/" + kindClusters + "/" + cluster
}

func parseRes(name string, kinds ...string) ([]string, bool) {
	p := strings.Split(name, "/")
	if len(p) != 2*len(kinds) {
		return nil, false
	}

	ids := make([]string, len(kinds))

	for i, k := range kinds {
		if p[2*i] != k || p[2*i+1] == "" {
			return nil, false
		}

		ids[i] = p[2*i+1]
	}

	return ids, true
}

// resolveLocation reads a parent given as `projects/*/locations/*` or as the legacy project_id and zone fields.
func resolveLocation(parent, project, zone string) (string, string, error) {
	if parent != "" {
		ids, ok := parseRes(parent, kindProjects, kindLocations)
		if !ok {
			return "", "", errInvalid(
				"parent",
				"Invalid parent %q: want projects/{project}/locations/{location}.",
				parent,
			)
		}

		return ids[0], ids[1], nil
	}

	if project == "" {
		return "", "", errInvalid("project_id", "Project ID must be specified.")
	}

	if zone == "" {
		return "", "", errInvalid("zone", "Location must be specified.")
	}

	return project, zone, nil
}

func resolveCluster(name, project, zone, cluster string) (ref, error) {
	if name != "" {
		ids, ok := parseRes(name, kindProjects, kindLocations, kindClusters)
		if !ok {
			return ref{}, errInvalid(
				"name",
				"Invalid cluster name %q: want projects/{project}/locations/{location}/clusters/{cluster}.",
				name,
			)
		}

		return ref{project: ids[0], location: ids[1], cluster: ids[2]}, nil
	}

	p, l, err := resolveLocation("", project, zone)
	if err != nil {
		return ref{}, err
	}

	if cluster == "" {
		return ref{}, errInvalid("cluster_id", "Cluster ID must be specified.")
	}

	return ref{project: p, location: l, cluster: cluster}, nil
}

func resolvePool(name, project, zone, cluster, pool string) (ref, error) {
	if name != "" {
		ids, ok := parseRes(name, kindProjects, kindLocations, kindClusters, kindNodePools)
		if !ok {
			return ref{}, errInvalid(
				"name",
				"Invalid node pool name %q: want projects/{project}/locations/{location}/clusters/{cluster}/nodePools/{nodePool}.",
				name,
			)
		}

		return ref{project: ids[0], location: ids[1], cluster: ids[2], pool: ids[3]}, nil
	}

	r, err := resolveCluster("", project, zone, cluster)
	if err != nil {
		return ref{}, err
	}

	if pool == "" {
		return ref{}, errInvalid("node_pool_id", "Node pool ID must be specified.")
	}

	r.pool = pool

	return r, nil
}

func resolveOperation(name, project, zone, op string) (ref, string, error) {
	if name != "" {
		ids, ok := parseRes(name, kindProjects, kindLocations, kindOps)
		if !ok {
			return ref{}, "", errInvalid(
				"name",
				"Invalid operation name %q: want projects/{project}/locations/{location}/operations/{operation}.",
				name,
			)
		}

		return ref{project: ids[0], location: ids[1]}, ids[2], nil
	}

	p, l, err := resolveLocation("", project, zone)
	if err != nil {
		return ref{}, "", err
	}

	if op == "" {
		return ref{}, "", errInvalid("operation_id", "Operation ID must be specified.")
	}

	return ref{project: p, location: l}, op, nil
}

func validName(field, name string) error {
	if name == "" {
		return errInvalid(field, "%s must be specified.", field)
	}

	if len(name) > maxNameLen || !nameRE.MatchString(name) {
		return errInvalid(
			field,
			"Invalid value for field %q: %q must be 1-%d lowercase letters, digits or hyphens, "+
				"start with a letter and not end with a hyphen.",
			field,
			name,
			maxNameLen,
		)
	}

	return nil
}

// regions are the regions Kubernetes Engine clusters can be created in.
func regions() []string {
	return []string{
		"africa-south1", "asia-east1", "asia-east2", "asia-northeast1", "asia-northeast2", "asia-northeast3",
		"asia-south1", "asia-south2", "asia-southeast1", "asia-southeast2", "australia-southeast1",
		"australia-southeast2", "europe-central2", "europe-north1", "europe-southwest1", "europe-west1",
		"europe-west2", "europe-west3", "europe-west4", "europe-west6", "europe-west8", "europe-west9",
		"europe-west10", "europe-west12", "me-central1", "me-central2", "me-west1", "northamerica-northeast1",
		"northamerica-northeast2", "northamerica-south1", "southamerica-east1", "southamerica-west1",
		"us-central1", "us-east1", "us-east4", "us-east5", "us-south1", "us-west1", "us-west2", "us-west3",
		"us-west4",
	}
}

var zoneSuffixes = []string{"a", "b", "c", "d", "f"} //nolint:gochecknoglobals // immutable catalog

func isRegion(l string) bool { return slices.Contains(regions(), l) }

func isZone(l string) bool {
	region, suffix, ok := cutLast(l, "-")

	return ok && isRegion(region) && slices.Contains(zoneSuffixes, suffix)
}

func cutLast(s, sep string) (string, string, bool) {
	return strings.CutLast(s, sep)
}

func validLocation(l string) bool { return isRegion(l) || isZone(l) }

func regionOf(location string) string {
	if isZone(location) {
		r, _, _ := cutLast(location, "-")

		return r
	}

	return location
}

// defaultZones are the node locations of a regional cluster that does not name any.
func defaultZones(region string) []string {
	return []string{region + "-a", region + "-b", region + "-c"}
}
