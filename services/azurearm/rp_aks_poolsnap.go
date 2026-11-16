package azurearm

import (
	"net/http"
	"strings"
)

const aksFieldCreationData = "creationData"

type aksSnapshotSource struct {
	version string
	image   string
	vmSize  string
	osType  string
	osSKU   string
	fips    bool
}

func (p *ContainerServiceProvider) aksSnapshotOf(source string) (*aksSnapshotSource, error) {
	sid, err := ParseGenericResourcePath(source)
	if err != nil || aksTargetOf(sid).kind != aksKindSnapshot || len(sid.Names) != 1 {
		return nil, aksBad("creationData.sourceResourceId '%s' must be a node pool snapshot resource ID.", source)
	}

	p.mu.RLock("aksSnapshotOf")
	defer p.mu.RUnlock()

	s, ok := p.snapshots[aksSnapshotKey(sid.ResourceGroup, sid.Names[0])]
	if !ok {
		return nil, netErr(
			http.StatusNotFound,
			aksCodeNotFound,
			"The node pool snapshot '%s' was not found.",
			sid.Names[0],
		)
	}

	return &aksSnapshotSource{
		version: strOf(s.Props, aksFieldK8sVersion), image: strOf(s.Props, "nodeImageVersion"),
		vmSize: strOf(s.Props, aksFieldVMSize), osType: strOf(s.Props, aksFieldOSType),
		osSKU: strOf(s.Props, "osSku"), fips: s.Props["enableFIPS"] == true,
	}, nil
}

// aksApplySnapshot fills the pool from creationData.sourceResourceId and returns the snapshot's node image.
// Same-family VM size rule: Learn aks/node-pool-snapshot, Limitations.
func (p *ContainerServiceProvider) aksApplySnapshot(name string, props map[string]any) (string, error) {
	source := strOf(mapOf(props[aksFieldCreationData]), "sourceResourceId")
	if source == "" {
		return "", nil
	}

	snap, err := p.aksSnapshotOf(source)
	if err != nil {
		return "", err
	}

	for _, f := range []struct{ key, want string }{
		{aksFieldOSType, snap.osType}, {"osSKU", snap.osSKU}, {aksFieldOrchVer, snap.version},
	} {
		if got := strOf(props, f.key); got != "" && !strings.EqualFold(got, f.want) {
			return "", aksBad("Node pool '%s' %s '%s' differs from the snapshot value '%s'.", name, f.key, got, f.want)
		}

		props[f.key] = f.want
	}

	if got := strOf(props, aksFieldVMSize); got != "" && !strings.EqualFold(got, snap.vmSize) {
		a, aok := findVMSize(got)
		b, bok := findVMSize(snap.vmSize)

		if !aok || !bok || a.family != b.family {
			return "", aksBad("Node pool '%s' vmSize '%s' must be from the same VM family as the snapshot's '%s'.",
				name, got, snap.vmSize)
		}
	} else if got == "" {
		props[aksFieldVMSize] = snap.vmSize
	}

	if snap.fips {
		props["enableFIPS"] = true
	}

	return snap.image, nil
}
