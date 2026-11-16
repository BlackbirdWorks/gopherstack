package gcpspanner

import (
	"slices"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
)

const (
	regionalReplicas = 3
	dual             = 2
)

type replicaSpec struct {
	loc    string
	n      int
	typ    instancepb.ReplicaInfo_ReplicaType
	leader bool
}

type configDef struct {
	replicas []replicaSpec
	optional []replicaSpec
}

func rw(loc string, n int, leader bool) replicaSpec {
	return replicaSpec{loc: loc, typ: instancepb.ReplicaInfo_READ_WRITE, n: n, leader: leader}
}

func ro(loc string, n int) replicaSpec {
	return replicaSpec{loc: loc, typ: instancepb.ReplicaInfo_READ_ONLY, n: n}
}

func wit(loc string) replicaSpec {
	return replicaSpec{loc: loc, typ: instancepb.ReplicaInfo_WITNESS, n: 1}
}

func def(r ...replicaSpec) []replicaSpec { return r }

func optRO(pairs ...any) []replicaSpec {
	out := make([]replicaSpec, 0, len(pairs)/dual)

	for i := 0; i+1 < len(pairs); i += dual {
		loc, _ := pairs[i].(string)
		n, _ := pairs[i+1].(int)
		out = append(out, ro(loc, n))
	}

	return out
}

// regionalOptional and multiRegionConfigs follow the replica tables of
// https://docs.cloud.google.com/spanner/docs/instance-configurations. Configs the page marks allowlisted are left out.
var regionalOptional = map[string][]replicaSpec{ //nolint:gochecknoglobals // immutable catalog
	usCentral1: optRO(asiaNortheast1, 1, "asia-south1", 1, europeWest1, dual, "europe-west2", 1, "europe-west9", 1,
		"us-west3", 1),
	usEast1:     optRO(usCentral1, 1, "us-west1", 1, europeWest1, 1, "europe-west3", 1),
	"us-east5":  optRO(usCentral1, 1),
	europeWest1: optRO(usCentral1, 1, "us-west1", 1),
}

var multiRegionConfigs = map[string]configDef{ //nolint:gochecknoglobals // immutable catalog
	"asia1": {
		def(rw(asiaNortheast1, dual, true), rw("asia-northeast2", dual, false), wit("asia-northeast3")),
		optRO("us-west1", 1, "us-east5", 1),
	},
	"eur3": {
		def(rw(europeWest1, dual, true), rw(europeWest4, dual, false), wit("europe-north1")),
		optRO(usCentral1, 1, "us-east4", 1),
	},
	"eur5": {
		def(rw("europe-west2", dual, true), rw(europeWest1, dual, false), wit(europeWest4)),
		optRO(usCentral1, 1, usEast1, 1),
	},
	"eur6": {
		def(rw(europeWest4, dual, true), rw("europe-west3", dual, false), wit("europe-west6")),
		optRO(usEast1, dual),
	},
	"eur7": {def(rw("europe-west8", dual, true), rw("europe-west3", dual, false), wit("europe-west12")), nil},
	"nam3": {
		def(rw("us-east4", dual, true), rw(usEast1, dual, false), wit(usCentral1)),
		optRO("asia-south1", 1, "asia-southeast1", 1, "asia-southeast2", 1, europeWest1, 1, "europe-west2", 1,
			europeWest4, 1, "europe-west9", 1, "us-east5", 1, "us-west1", 1, "us-west2", 1, "us-west4", 1,
			"us-west8", 1),
	},
	"nam6": {
		def(rw(usCentral1, dual, true), rw(usEast1, dual, false), ro("us-west1", 1), ro("us-west2", 1),
			wit("us-central2")),
		nil,
	},
	"nam7": {
		def(rw(usCentral1, dual, true), rw("us-east4", dual, false), wit("us-central2")),
		optRO(usEast1, dual, "us-south1", 1, "us-west1", 1, europeWest1, dual),
	},
	"nam8": {
		def(rw("us-west2", dual, true), rw("us-west1", dual, false), wit("us-west3")),
		optRO(asiaNortheast1, dual, "asia-southeast1", dual, "europe-west2", dual, "europe-west3", dual, "us-east5", 1),
	},
	"nam9": {
		def(rw("us-east4", dual, true), rw(usCentral1, dual, false), ro("us-west1", dual), wit(usEast1)),
		nil,
	},
	"nam10": {
		def(rw(usCentral1, dual, true), rw("us-west3", dual, false), wit("us-central2")),
		optRO("us-east4", 1),
	},
	"nam11": {
		def(rw(usCentral1, dual, true), rw(usEast1, dual, false), wit("us-central2")),
		optRO("us-west1", 1),
	},
	"nam12": {
		def(rw(usCentral1, dual, true), rw("us-east4", dual, false), ro("us-west1", dual), wit("us-central2")),
		nil,
	},
	"nam13": {def(rw("us-central2", dual, true), rw(usCentral1, dual, false), wit("us-west3")), nil},
	"nam14": {
		def(rw("us-east4", dual, true), rw("northamerica-northeast1", dual, false), wit(usEast1)),
		nil,
	},
	"nam15": {
		def(rw("us-south1", dual, true), rw("us-east4", dual, false), wit(usCentral1)),
		optRO("us-central2", 1, usEast1, 1, "us-west3", 1),
	},
	"nam16": {
		def(rw(usCentral1, dual, true), rw("us-east4", dual, false), wit("us-east5")),
		optRO("us-west2", dual),
	},
	"nam22": {
		def(rw("us-east5", dual, true), rw(usEast1, dual, false), wit("us-east4")),
		optRO("us-west1", 1),
	},
	"nam23": {
		def(rw(usCentral1, dual, true), rw("us-west4", dual, false), wit("us-south1")),
		optRO(asiaNortheast1, 1, "australia-southeast2", 1, europeWest1, 1, "europe-west3", 1),
	},
	"nam-eur-asia1": {
		def(rw(usCentral1, dual, true), rw("us-central2", dual, false), ro(europeWest1, dual), ro("asia-east1", dual),
			wit(usEast1)),
		optRO("us-west2", 1),
	},
	"nam-eur-asia3": {
		def(rw(usCentral1, dual, true), rw(usEast1, dual, false), ro(europeWest1, 1), ro(europeWest4, 1),
			ro("asia-east1", dual), wit("us-central2")),
		nil,
	},
}

func expand(specs []replicaSpec) []*instancepb.ReplicaInfo {
	var out []*instancepb.ReplicaInfo

	for _, s := range specs {
		for i := range s.n {
			out = append(
				out,
				&instancepb.ReplicaInfo{Location: s.loc, Type: s.typ, DefaultLeaderLocation: s.leader && i == 0},
			)
		}
	}

	return out
}

func leaderOptions(replicas []*instancepb.ReplicaInfo) []string {
	var out []string

	for _, r := range replicas {
		if r.GetType() == instancepb.ReplicaInfo_READ_WRITE && !slices.Contains(out, r.GetLocation()) {
			out = append(out, r.GetLocation())
		}
	}

	return out
}

func buildConfig(project, id, display string, replicas, optional []replicaSpec) *instancepb.InstanceConfig {
	reps := expand(replicas)

	return &instancepb.InstanceConfig{
		Name:             configName(project, id),
		DisplayName:      display,
		ConfigType:       instancepb.InstanceConfig_GOOGLE_MANAGED,
		Replicas:         reps,
		OptionalReplicas: expand(optional),
		LeaderOptions:    leaderOptions(reps),
		State:            instancepb.InstanceConfig_READY,
	}
}
