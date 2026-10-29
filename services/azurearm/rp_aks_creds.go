package azurearm

import (
	"encoding/base64"
	"net"
	"net/http"
	"strconv"
	"strings"
)

const (
	aksCredUser    = "clusterUser"
	aksCredAdmin   = "clusterAdmin"
	aksCredMonitor = "clusterMonitoringUser"
	aksAPIPort     = 443
)

type aksServing struct {
	server string
	token  string
	ca     []byte
}

// servingFor is where a cluster's API server answers: the container when the engine runs it, else the cluster's
// emulated endpoint with its generated CA and token.
func (p *ContainerServiceProvider) servingFor(c *storedAKSCluster) (aksServing, error) {
	if p.eng.enabled() && !c.Stopped {
		sv, ready := p.eng.access(aksKey(c.ResourceGroup, c.Name))
		if !ready {
			return aksServing{}, netErr(http.StatusConflict, aksCodeStarting,
				"The API server of managed cluster '%s' is still starting.", c.Name)
		}

		return sv, nil
	}

	return aksServing{
		server: "https://" + net.JoinHostPort(c.Fqdn, strconv.Itoa(aksAPIPort)), ca: []byte(c.CACert), token: c.Token,
	}, nil
}

// credentials implements listCluster{User,Admin,MonitoringUser}Credential.
func (p *ContainerServiceProvider) credentials(id ResourceID, role string) (map[string]any, error) {
	p.mu.RLock("credentials")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.LeafName())
	}

	if role == aksCredAdmin && acaBool(c.Props, "disableLocalAccounts") {
		return nil, netErr(http.StatusBadRequest, "BadRequest",
			"Getting static credential is not allowed because this cluster is set to disable local accounts.")
	}

	sv, err := p.servingFor(c)
	if err != nil {
		return nil, err
	}

	cfg := aksKubeconfig(c, role, sv)

	return map[string]any{"kubeconfigs": []any{map[string]any{fieldName: role, fieldValue: []byte(cfg)}}}, nil
}

func aksKubeconfig(c *storedAKSCluster, role string, sv aksServing) string {
	user := role + "_" + c.ResourceGroup + "_" + c.Name

	var b strings.Builder

	b.WriteString("apiVersion: v1\nkind: Config\nclusters:\n- cluster:\n")
	b.WriteString("    certificate-authority-data: " + base64.StdEncoding.EncodeToString(sv.ca) + "\n")
	b.WriteString("    server: " + sv.server + "\n")
	b.WriteString("  name: " + c.Name + "\ncontexts:\n- context:\n    cluster: " + c.Name + "\n")
	b.WriteString("    user: " + user + "\n  name: " + c.Name + "\ncurrent-context: " + c.Name + "\n")
	b.WriteString("preferences: {}\nusers:\n- name: " + user + "\n  user:\n    token: " + sv.token + "\n")

	return b.String()
}

func (p *ContainerServiceProvider) upgradeProfile(id ResourceID) (map[string]any, error) {
	p.mu.RLock("upgradeProfile")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	pools := []any{}

	for _, pool := range aksSortedPools(c.Pools) {
		pools = append(pools, aksPoolUpgrade(pool.Name, pool.Current, strOf(pool.Props, aksFieldOSType)))
	}

	return map[string]any{
		"id": c.armID() + "/upgradeprofiles/default", fieldName: aksDefaultName,
		fieldType: namespaceMicrosoftContainerService + "/" + aksClusterType + "/upgradeprofiles",
		fieldProperties: map[string]any{
			"controlPlaneProfile": aksPoolUpgrade("", c.Current, aksOSLinux), aksFieldPools: pools,
		},
	}, nil
}

func aksPoolUpgrade(name, current, osType string) map[string]any {
	if osType == "" {
		osType = aksOSLinux
	}

	out := map[string]any{
		aksFieldK8sVersion: current, aksFieldOSType: osType, "upgrades": aksPoolUpgradeTargets(current),
	}
	if name != "" {
		out[fieldName] = name
	}

	return out
}

func aksPoolUpgradeTargets(current string) []any {
	ups := aksUpgradesFrom(current)
	out := make([]any, 0, len(ups))

	for _, u := range ups {
		out = append(out, u)
	}

	return out
}

func (p *ContainerServiceProvider) poolUpgradeProfile(id ResourceID) (map[string]any, error) {
	p.mu.RLock("poolUpgradeProfile")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	pool, found := c.Pools[strings.ToLower(id.Names[1])]
	if !found {
		return nil, netErr(http.StatusNotFound, aksCodeNotFound, "The agent pool '%s' was not found.", id.Names[1])
	}

	props := aksPoolUpgrade("", pool.Current, strOf(pool.Props, aksFieldOSType))
	delete(props, fieldName)
	props["latestNodeImageVersion"] = aksNodeImage(pool.Props)

	return map[string]any{
		"id": c.armID() + "/agentPools/" + pool.Name + "/upgradeProfiles/default", fieldName: aksDefaultName,
		fieldType:       namespaceMicrosoftContainerService + "/" + aksClusterType + "/agentPools/upgradeProfiles",
		fieldProperties: props,
	}, nil
}

func (p *ContainerServiceProvider) availablePoolVersions(id ResourceID) (map[string]any, error) {
	p.mu.RLock("availablePoolVersions")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	versions := []any{}

	for _, m := range aksMinors {
		for _, v := range m.patches {
			if aksPoolSkewOK(v, c.Current) != nil {
				continue
			}

			versions = append(versions, map[string]any{
				"kubernetesVersion": v, "default": v == c.Current, "isPreview": m.isPreview,
			})
		}
	}

	return map[string]any{
		"id": c.armID() + "/availableAgentPoolVersions", fieldName: aksDefaultName,
		fieldType:       namespaceMicrosoftContainerService + "/" + aksClusterType + "/availableAgentPoolVersions",
		fieldProperties: map[string]any{"agentPoolVersions": versions},
	}, nil
}
