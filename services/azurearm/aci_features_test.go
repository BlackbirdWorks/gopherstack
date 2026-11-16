package azurearm_test

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *aciStack) put(t *testing.T, g armcontainerinstance.ContainerGroup) error {
	t.Helper()

	pl, err := s.factory.NewContainerGroupsClient().BeginCreateOrUpdate(t.Context(), "rg", "feat", g, nil)
	if err == nil {
		_, err = pl.PollUntilDone(t.Context(), pollFast())
	}

	return err
}

func TestACISecurityContext(t *testing.T) {
	t.Parallel()

	seccomp := base64.StdEncoding.EncodeToString([]byte(`{"defaultAction":"SCMP_ACT_ALLOW"}`))
	secure := func(sc *armcontainerinstance.SecurityContextDefinition) func(*armcontainerinstance.ContainerGroup) {
		return func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.Containers[0].Properties.SecurityContext = sc
		}
	}

	tests := []struct {
		mutate   func(*armcontainerinstance.ContainerGroup)
		check    func(*testing.T, []string, string, []string, []string)
		name     string
		wantCode string
	}{
		{
			name: "user and group", mutate: secure(&armcontainerinstance.SecurityContextDefinition{
				RunAsUser: new(int32(1000)), RunAsGroup: new(int32(2000)),
			}),
			check: func(t *testing.T, _ []string, user string, _, _ []string) {
				t.Helper()
				assert.Equal(t, "1000:2000", user)
			},
		},
		{
			name: "group only runs as root user", mutate: secure(&armcontainerinstance.SecurityContextDefinition{
				RunAsGroup: new(int32(7)),
			}),
			check: func(t *testing.T, _ []string, user string, _, _ []string) {
				t.Helper()
				assert.Equal(t, "0:7", user)
			},
		},
		{
			name: "capabilities", mutate: secure(&armcontainerinstance.SecurityContextDefinition{
				Capabilities: &armcontainerinstance.SecurityContextCapabilitiesDefinition{
					Add: []*string{new("CAP_NET_ADMIN")}, Drop: []*string{new("MKNOD")},
				},
			}),
			check: func(t *testing.T, _ []string, _ string, add, drop []string) {
				t.Helper()

				assert.Equal(t, []string{"NET_ADMIN"}, add)
				assert.Equal(t, []string{"MKNOD"}, drop)
			},
		},
		{
			name: "no escalation and seccomp", mutate: secure(&armcontainerinstance.SecurityContextDefinition{
				AllowPrivilegeEscalation: new(false), SeccompProfile: &seccomp,
			}),
			check: func(t *testing.T, opts []string, _ string, _, _ []string) {
				t.Helper()

				assert.Equal(t, []string{"no-new-privileges", `seccomp={"defaultAction":"SCMP_ACT_ALLOW"}`}, opts)
			},
		},
		{
			name:     "privileged refused",
			mutate:   secure(&armcontainerinstance.SecurityContextDefinition{Privileged: new(true)}),
			wantCode: "InvalidRequestContent",
		},
		{
			name: "bad capability", wantCode: "InvalidRequestContent",
			mutate: secure(&armcontainerinstance.SecurityContextDefinition{
				Capabilities: &armcontainerinstance.SecurityContextCapabilitiesDefinition{
					Add: []*string{new("net admin")},
				},
			}),
		},
		{
			name: "bad seccomp", wantCode: "InvalidRequestContent",
			mutate: secure(&armcontainerinstance.SecurityContextDefinition{SeccompProfile: new("not base64!")}),
		},
		{
			name: "negative uid", wantCode: "InvalidRequestContent",
			mutate: secure(&armcontainerinstance.SecurityContextDefinition{RunAsUser: new(int32(-1))}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			g := aciOpts{}.group()
			tt.mutate(&g)

			err := s.put(t, g)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			specs := s.rt.started()
			require.Len(t, specs, 1)
			tt.check(t, specs[0].SecurityOpt, specs[0].User, specs[0].CapAdd, specs[0].CapDrop)

			sc := s.get(t, "rg", "feat").Properties.Containers[0].Properties.SecurityContext
			require.NotNil(t, sc)
		})
	}
}

func TestACIInitContainerSecurityContext(t *testing.T) {
	t.Parallel()

	rt := newFakeACIRuntime()
	rt.scripts["busybox:init"] = []int{0}

	s := newACIStackWith(t, rt)
	g := aciOpts{}.group()
	g.Properties.InitContainers = []*armcontainerinstance.InitContainerDefinition{{
		Name: new("init"),
		Properties: &armcontainerinstance.InitContainerPropertiesDefinition{
			Image:           new("busybox:init"),
			SecurityContext: &armcontainerinstance.SecurityContextDefinition{RunAsUser: new(int32(33))},
		},
	}}

	require.NoError(t, s.put(t, g))

	var initUser, appUser string

	for _, sp := range s.rt.started() {
		if sp.Image == "busybox:init" {
			initUser = sp.User
		} else {
			appUser = sp.User
		}
	}

	assert.Equal(t, "33", initUser)
	assert.Empty(t, appUser)

	got := s.get(t, "rg", "feat").Properties.InitContainers[0].Properties.SecurityContext
	require.NotNil(t, got)
	assert.Equal(t, int32(33), *got.RunAsUser)
}

func TestACIGroupFeatures(t *testing.T) {
	t.Parallel()

	policy := base64.StdEncoding.EncodeToString([]byte(`{"allow_all":true}`))
	enc := func() *armcontainerinstance.EncryptionProperties {
		return &armcontainerinstance.EncryptionProperties{
			VaultBaseURL: new("https://kv.vault.azure.net"), KeyName: new("k"), KeyVersion: new("v1"),
			Identity: new(
				"/subscriptions/s/resourcegroups/r/providers/Microsoft.ManagedIdentity/userAssignedIdentities/i",
			),
		}
	}
	ext := func(name, typ, ver string) *armcontainerinstance.DeploymentExtensionSpec {
		return &armcontainerinstance.DeploymentExtensionSpec{
			Name: &name,
			Properties: &armcontainerinstance.DeploymentExtensionSpecProperties{
				ExtensionType: &typ, Version: &ver,
				Settings:          map[string]any{"clusterCidr": "10.0.0.0/16"},
				ProtectedSettings: map[string]any{"kubeConfig": "secret"},
			},
		}
	}
	sku := func(v armcontainerinstance.ContainerGroupSKU) func(*armcontainerinstance.ContainerGroup) {
		return func(g *armcontainerinstance.ContainerGroup) { g.Properties.SKU = &v }
	}

	tests := []struct {
		mutate   func(*armcontainerinstance.ContainerGroup)
		check    func(*testing.T, *armcontainerinstance.ContainerGroup)
		name     string
		wantCode string
	}{
		{
			name:   "default sku",
			mutate: func(*armcontainerinstance.ContainerGroup) {},
			check: func(t *testing.T, g *armcontainerinstance.ContainerGroup) {
				t.Helper()

				assert.Equal(t, armcontainerinstance.ContainerGroupSKUStandard, *g.Properties.SKU)
			},
		},
		{
			name:   "dedicated",
			mutate: sku(armcontainerinstance.ContainerGroupSKUDedicated),
			check: func(t *testing.T, g *armcontainerinstance.ContainerGroup) {
				t.Helper()

				assert.Equal(t, armcontainerinstance.ContainerGroupSKUDedicated, *g.Properties.SKU)
			},
		},
		{name: "confidential", mutate: func(g *armcontainerinstance.ContainerGroup) {
			sku(armcontainerinstance.ContainerGroupSKUConfidential)(g)
			g.Properties.ConfidentialComputeProperties = &armcontainerinstance.ConfidentialComputeProperties{
				CcePolicy: &policy,
			}
		}, check: func(t *testing.T, g *armcontainerinstance.ContainerGroup) {
			t.Helper()

			assert.Equal(t, armcontainerinstance.ContainerGroupSKUConfidential, *g.Properties.SKU)
			assert.Equal(t, policy, *g.Properties.ConfidentialComputeProperties.CcePolicy)
		}},
		{name: "unknown sku", wantCode: "InvalidRequestContent", mutate: sku("Gold")},
		{
			name:     "bad cce policy",
			wantCode: "InvalidRequestContent",
			mutate: func(g *armcontainerinstance.ContainerGroup) {
				g.Properties.ConfidentialComputeProperties = &armcontainerinstance.ConfidentialComputeProperties{
					CcePolicy: new("***"),
				}
			},
		},
		{name: "encryption", mutate: func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.EncryptionProperties = enc()
		}, check: func(t *testing.T, g *armcontainerinstance.ContainerGroup) {
			t.Helper()

			require.NotNil(t, g.Properties.EncryptionProperties)
			assert.Equal(t, "k", *g.Properties.EncryptionProperties.KeyName)
			assert.Equal(t, "https://kv.vault.azure.net", *g.Properties.EncryptionProperties.VaultBaseURL)
		}},
		{
			name:     "encryption http vault",
			wantCode: "InvalidRequestContent",
			mutate: func(g *armcontainerinstance.ContainerGroup) {
				e := enc()
				e.VaultBaseURL = new("http://kv.vault.azure.net")
				g.Properties.EncryptionProperties = e
			},
		},
		{
			name:     "encryption missing key",
			wantCode: "InvalidRequestContent",
			mutate: func(g *armcontainerinstance.ContainerGroup) {
				e := enc()
				e.KeyName = new("")
				g.Properties.EncryptionProperties = e
			},
		},
		{name: "extensions", mutate: func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.Extensions = []*armcontainerinstance.DeploymentExtensionSpec{
				ext("kube-proxy", "kube-proxy", "1.0"), ext("metrics", "realtime-metrics", "1.0"),
			}
		}, check: func(t *testing.T, g *armcontainerinstance.ContainerGroup) {
			t.Helper()

			require.Len(t, g.Properties.Extensions, 2)
			x := g.Properties.Extensions[0].Properties
			assert.Equal(t, "kube-proxy", *x.ExtensionType)
			assert.Equal(t, map[string]any{"clusterCidr": "10.0.0.0/16"}, x.Settings)
			assert.Nil(t, x.ProtectedSettings)
		}},
		{
			name:     "extension without version",
			wantCode: "InvalidRequestContent",
			mutate: func(g *armcontainerinstance.ContainerGroup) {
				g.Properties.Extensions = []*armcontainerinstance.DeploymentExtensionSpec{ext("x", "kube-proxy", "")}
			},
		},
		{
			name:     "duplicate extension",
			wantCode: "InvalidRequestContent",
			mutate: func(g *armcontainerinstance.ContainerGroup) {
				g.Properties.Extensions = []*armcontainerinstance.DeploymentExtensionSpec{
					ext("x", "kube-proxy", "1.0"), ext("X", "realtime-metrics", "1.0"),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			g := aciOpts{}.group()
			tt.mutate(&g)

			err := s.put(t, g)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
			tt.check(t, s.get(t, "rg", "feat"))

			snap := s.handler.Snapshot(t.Context())
			assert.NotContains(t, string(snap), "kubeConfig", "protectedSettings must not be persisted")

			dst := newACIStack(t)
			require.NoError(t, dst.handler.Restore(t.Context(), snap))
			tt.check(t, dst.get(t, "rg", "feat"))
		})
	}
}

func TestACIUDPPorts(t *testing.T) {
	t.Parallel()

	udp, tcp := armcontainerinstance.ContainerNetworkProtocolUDP, armcontainerinstance.ContainerNetworkProtocolTCP
	gUDP := armcontainerinstance.ContainerGroupNetworkProtocolUDP
	gTCP := armcontainerinstance.ContainerGroupNetworkProtocolTCP

	tests := []struct {
		name      string
		wantCode  string
		cports    []*armcontainerinstance.ContainerPort
		ports     []*armcontainerinstance.Port
		wantProto []string
		wantUDP   int
		wantTCP   int
	}{
		{
			name:      "udp",
			cports:    []*armcontainerinstance.ContainerPort{{Port: new(int32(53)), Protocol: &udp}},
			ports:     []*armcontainerinstance.Port{{Port: new(int32(53)), Protocol: &gUDP}},
			wantUDP:   1,
			wantProto: []string{"UDP"},
		},
		{
			name: "tcp and udp on one port",
			cports: []*armcontainerinstance.ContainerPort{
				{Port: new(int32(53)), Protocol: &udp}, {Port: new(int32(53)), Protocol: &tcp},
			},
			ports: []*armcontainerinstance.Port{
				{Port: new(int32(53)), Protocol: &gUDP}, {Port: new(int32(53)), Protocol: &gTCP},
			},
			wantUDP: 1, wantTCP: 1, wantProto: []string{"UDP", "TCP"},
		},
		{
			name:     "udp address port without a udp container port",
			cports:   []*armcontainerinstance.ContainerPort{{Port: new(int32(53)), Protocol: &tcp}},
			ports:    []*armcontainerinstance.Port{{Port: new(int32(53)), Protocol: &gUDP}},
			wantCode: "InvalidRequestContent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			g := aciOpts{}.group()
			g.Properties.Containers[0].Properties.Ports = tt.cports
			g.Properties.IPAddress = &armcontainerinstance.IPAddress{
				Type: new(armcontainerinstance.ContainerGroupIPAddressTypePrivate), Ports: tt.ports,
			}

			err := s.put(t, g)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			specs := s.rt.started()
			require.Len(t, specs, 1)

			var udpN, tcpN int

			for _, p := range specs[0].Ports {
				if strings.HasSuffix(p, ":53/udp") {
					udpN++
				} else if strings.HasSuffix(p, ":53") {
					tcpN++
				}
			}

			assert.Equal(t, tt.wantUDP, udpN)
			assert.Equal(t, tt.wantTCP, tcpN)

			var protos []string
			for _, p := range s.get(t, "rg", "feat").Properties.IPAddress.Ports {
				protos = append(protos, string(*p.Protocol))
			}

			assert.Equal(t, tt.wantProto, protos)
		})
	}
}

func TestACIRequestsMapToSoftLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		limits     *armcontainerinstance.ResourceLimits
		name       string
		wantMemRes int64
		wantMem    int64
		wantShares int64
	}{
		{name: "requests only", wantMemRes: 3 << 29, wantShares: 1024},
		{
			name:       "limits above requests",
			limits:     &armcontainerinstance.ResourceLimits{CPU: new(2.0), MemoryInGB: new(4.0)},
			wantMemRes: 3 << 29, wantMem: 4 << 30, wantShares: 1024,
		},
		{
			name:       "reservation never exceeds the limit",
			limits:     &armcontainerinstance.ResourceLimits{CPU: new(1.0), MemoryInGB: new(1.0)},
			wantMemRes: 1 << 30, wantMem: 1 << 30, wantShares: 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			_, err := s.create(t, "rg", "req", aciOpts{limits: tt.limits})
			require.NoError(t, err)

			specs := s.rt.started()
			require.Len(t, specs, 1)
			assert.Equal(t, tt.wantMemRes, specs[0].MemoryReservationBytes)
			assert.Equal(t, tt.wantMem, specs[0].MemoryBytes)
			assert.Equal(t, tt.wantShares, specs[0].CPUShares)
		})
	}
}

func TestACISpotRejectsConfidential(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	g := aciOpts{}.group()
	g.Properties.Priority = new(armcontainerinstance.ContainerGroupPrioritySpot)
	g.Properties.SKU = new(armcontainerinstance.ContainerGroupSKUConfidential)

	err := s.put(t, g)
	require.Error(t, err)
	assert.Equal(t, "InvalidRequestContent", sdkErrCode(err))
}

func TestACIOperationsList(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	pager := s.factory.NewOperationsClient().NewListPager(nil)

	byName := map[string]string{}

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, op := range page.Value {
			assert.Equal(t, "Microsoft Container Instance", *op.Display.Provider)
			byName[*op.Name] = *op.Display.Description
		}
	}

	assert.Equal(t, "Get all container groups.", byName["Microsoft.ContainerInstance/containerGroups/read"])
	assert.Equal(t, "Attach to the output stream of a container.",
		byName["Microsoft.ContainerInstance/containerGroups/containers/attach/action"])
	assert.Greater(t, len(byName), 30)
}

func TestACIGroupsGetDistinctAddresses(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	opts := aciOpts{
		ip: &armcontainerinstance.IPAddress{
			Type:  new(armcontainerinstance.ContainerGroupIPAddressTypePrivate),
			Ports: []*armcontainerinstance.Port{{Port: new(int32(8080))}},
		},
		ports: []*armcontainerinstance.ContainerPort{{Port: new(int32(8080))}},
	}

	addrs := map[string]bool{}

	for _, name := range []string{"one", "two", "three"} {
		g, err := s.create(t, "rg", name, opts)
		require.NoError(t, err)

		addrs[*g.Properties.IPAddress.IP] = true
	}

	assert.Len(t, addrs, 3, "each group has its own address")

	ports := map[string]bool{}
	for _, sp := range s.rt.started() {
		require.Len(t, sp.Ports, 1)
		assert.True(t, strings.HasSuffix(sp.Ports[0], ":8080:8080"), sp.Ports[0])

		ports[sp.Ports[0]] = true
	}

	assert.Len(t, ports, 3, "the same declared port is published on three addresses")
}
