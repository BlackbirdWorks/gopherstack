package azurearm_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const extSecret = "s3cr3t-protected-value"

func customScript() armcompute.VirtualMachineExtension {
	return armcompute.VirtualMachineExtension{
		Location: new("westus"),
		Properties: &armcompute.VirtualMachineExtensionProperties{
			Publisher:               new("Microsoft.Azure.Extensions"),
			Type:                    new("CustomScript"),
			TypeHandlerVersion:      new("2.1"),
			AutoUpgradeMinorVersion: new(true),
			Settings:                map[string]any{"fileUris": []any{"https://example.test/a.sh"}},
			ProtectedSettings:       map[string]any{"commandToExecute": extSecret},
		},
	}
}

func (s *cmpStack) putExt(t *testing.T, vm string, ext armcompute.VirtualMachineExtension) error {
	t.Helper()

	pl, err := s.cf.NewVirtualMachineExtensionsClient().BeginCreateOrUpdate(t.Context(), netRG, vm, "cse", ext, nil)

	return waitPoller(t, pl, err)
}

func TestComputeSDK_VMExtensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(*armcompute.VirtualMachineExtension)
		name     string
		wantCode string
	}{
		{name: "valid", mutate: func(*armcompute.VirtualMachineExtension) {}},
		{name: "missing publisher", wantCode: "InvalidParameter",
			mutate: func(e *armcompute.VirtualMachineExtension) { e.Properties.Publisher = nil }},
		{name: "missing type", wantCode: "InvalidParameter",
			mutate: func(e *armcompute.VirtualMachineExtension) { e.Properties.Type = nil }},
		{name: "malformed version", wantCode: "InvalidParameter",
			mutate: func(e *armcompute.VirtualMachineExtension) { e.Properties.TypeHandlerVersion = new("two") }},
		{name: "unknown catalog major", wantCode: "InvalidParameter",
			mutate: func(e *armcompute.VirtualMachineExtension) { e.Properties.TypeHandlerVersion = new("9.0") }},
		{name: "unknown publisher accepted",
			mutate: func(e *armcompute.VirtualMachineExtension) {
				e.Properties.Publisher, e.Properties.Type = new("Contoso.Ext"), new("Thing")
			}},
		{name: "after unknown extension", wantCode: "InvalidParameter",
			mutate: func(e *armcompute.VirtualMachineExtension) {
				e.Properties.ProvisionAfterExtensions = []*string{new("ghost")}
			}},
		{name: "no location", wantCode: "LocationRequired",
			mutate: func(e *armcompute.VirtualMachineExtension) { e.Location = nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))

			ext := customScript()
			tt.mutate(&ext)

			err := s.putExt(t, "vm1", ext)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			got, err := s.cf.NewVirtualMachineExtensionsClient().Get(t.Context(), netRG, "vm1", "cse", nil)
			require.NoError(t, err)
			assert.Nil(t, got.Properties.ProtectedSettings, "protectedSettings is write-only")
			assert.Equal(t, "Succeeded", *got.Properties.ProvisioningState)
			assert.Equal(t, "Microsoft.Compute/virtualMachines/extensions", *got.Type)
			assert.Contains(t, got.Properties.Settings, "fileUris")
		})
	}
}

func TestComputeSDK_ExtensionLifecycle(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))
	require.NoError(t, s.putExt(t, "vm1", customScript()))

	ec := s.cf.NewVirtualMachineExtensionsClient()

	got, err := ec.Get(
		t.Context(),
		netRG,
		"vm1",
		"cse",
		&armcompute.VirtualMachineExtensionsClientGetOptions{Expand: new("instanceView")},
	)
	require.NoError(t, err)
	require.NotNil(t, got.Properties.InstanceView)
	assert.Equal(t, "CustomScript", *got.Properties.InstanceView.Type)
	assert.Equal(t, "ProvisioningState/succeeded", *got.Properties.InstanceView.Statuses[0].Code)

	list, err := ec.List(t.Context(), netRG, "vm1", nil)
	require.NoError(t, err)
	assert.Len(t, list.Value, 1)

	vm, err := s.cf.NewVirtualMachinesClient().Get(t.Context(), netRG, "vm1", nil)
	require.NoError(t, err)
	require.Len(t, vm.Resources, 1, "VM GET reports its child extensions")

	view, err := s.cf.NewVirtualMachinesClient().InstanceView(t.Context(), netRG, "vm1", nil)
	require.NoError(t, err)
	require.Len(t, view.Extensions, 1)
	assert.Equal(t, "2.1", *view.Extensions[0].TypeHandlerVersion)

	upd := armcompute.VirtualMachineExtensionUpdate{Tags: map[string]*string{"k": new("v")}}
	pl, err := ec.BeginUpdate(t.Context(), netRG, "vm1", "cse", upd, nil)
	require.NoError(t, waitPoller(t, pl, err))

	got, err = ec.Get(t.Context(), netRG, "vm1", "cse", nil)
	require.NoError(t, err)
	assert.Equal(t, "v", *got.Tags["k"])
	assert.Equal(t, "2.1", *got.Properties.TypeHandlerVersion, "PATCH keeps untouched properties")

	dl, err := ec.BeginDelete(t.Context(), netRG, "vm1", "cse", nil)
	require.NoError(t, waitPoller(t, dl, err))

	_, err = ec.Get(t.Context(), netRG, "vm1", "cse", nil)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	err = s.putExt(t, "ghost", customScript())
	require.Error(t, err)
	assert.Equal(t, "ParentResourceNotFound", sdkErrCode(err))
}

func TestComputeSDK_ExtensionSecretsNotPersisted(t *testing.T) {
	t.Parallel()

	src := newCmpStack(t)
	src.mustVM(t, "vm1", linuxVM(src.newNIC(t, "nic1")))
	require.NoError(t, src.putExt(t, "vm1", customScript()))

	ss := src.scaleSet(t, 1)
	ss.Properties.VirtualMachineProfile.ExtensionProfile = &armcompute.VirtualMachineScaleSetExtensionProfile{
		Extensions: []*armcompute.VirtualMachineScaleSetExtension{{
			Name: new("cse"),
			Properties: &armcompute.VirtualMachineScaleSetExtensionProperties{
				Publisher: new("Microsoft.Azure.Extensions"), Type: new("CustomScript"), TypeHandlerVersion: new("2.1"),
				ProtectedSettings: map[string]any{"commandToExecute": extSecret},
			},
		}},
	}
	_, err := src.putScaleSet(t, "vmss1", ss)
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())
	raw, err := json.Marshal(snap)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), extSecret)

	dst := newCmpStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.cf.NewVirtualMachineExtensionsClient().Get(t.Context(), netRG, "vm1", "cse", nil)
	require.NoError(t, err)
	assert.Equal(t, "CustomScript", *got.Properties.Type)
	assert.Nil(t, got.Properties.ProtectedSettings)

	set, err := dst.cf.NewVirtualMachineScaleSetsClient().Get(t.Context(), netRG, "vmss1", nil)
	require.NoError(t, err)

	prof := set.Properties.VirtualMachineProfile.ExtensionProfile.Extensions[0].Properties
	assert.Nil(t, prof.ProtectedSettings)
}

func TestComputeSDK_ScaleSetExtensions(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	_, err := s.putScaleSet(t, "vmss1", s.scaleSet(t, 1))
	require.NoError(t, err)

	c := s.cf.NewVirtualMachineScaleSetExtensionsClient()
	ext := armcompute.VirtualMachineScaleSetExtension{
		Properties: &armcompute.VirtualMachineScaleSetExtensionProperties{
			Publisher: new("Microsoft.Azure.Extensions"), Type: new("CustomScript"), TypeHandlerVersion: new("2.1"),
			ProtectedSettings: map[string]any{"commandToExecute": extSecret}, Settings: map[string]any{"a": "b"},
		},
	}
	pl, err := c.BeginCreateOrUpdate(t.Context(), netRG, "vmss1", "cse", ext, nil)
	require.NoError(t, waitPoller(t, pl, err))

	got, err := c.Get(t.Context(), netRG, "vmss1", "cse", nil)
	require.NoError(t, err)
	assert.Nil(t, got.Properties.ProtectedSettings)
	assert.Equal(t, "Microsoft.Compute/virtualMachineScaleSets/extensions", *got.Type)

	pager := c.NewListPager(netRG, "vmss1", nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, page.Value, 1)
}

func TestComputeSDK_ExtensionImages(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	c := s.cf.NewVirtualMachineExtensionImagesClient()

	types, err := c.ListTypes(t.Context(), "westus", "Microsoft.Azure.Extensions", nil)
	require.NoError(t, err)
	require.Len(t, types.VirtualMachineExtensionImageArray, 1)
	assert.Equal(t, "CustomScript", *types.VirtualMachineExtensionImageArray[0].Name)

	vers, err := c.ListVersions(t.Context(), "westus", "Microsoft.Azure.Extensions", "customscript", nil)
	require.NoError(t, err)
	assert.Len(t, vers.VirtualMachineExtensionImageArray, 2)

	img, err := c.Get(t.Context(), "westus", "Microsoft.Azure.Extensions", "CustomScript", "2.1", nil)
	require.NoError(t, err)
	assert.Equal(t, "Linux", *img.Properties.OperatingSystem)
	assert.Equal(t, "IaaS", *img.Properties.ComputeRole)
	assert.True(t, strings.HasSuffix(*img.ID, "/types/CustomScript/versions/2.1"))

	_, err = c.Get(t.Context(), "westus", "Microsoft.Azure.Extensions", "CustomScript", "7.7", nil)
	require.Error(t, err)

	_, err = c.ListVersions(t.Context(), "westus", "Microsoft.Azure.Extensions", "Nope", nil)
	require.Error(t, err)

	empty, err := c.ListTypes(t.Context(), "westus", "Nobody", nil)
	require.NoError(t, err)
	assert.Empty(t, empty.VirtualMachineExtensionImageArray)
}

func TestComputeSDK_RunCommandAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cmd      string
		wantCode string
		script   []*string
		stop     bool
	}{
		{name: "unknown command", cmd: "Bogus", wantCode: "ResourceNotFound"},
		{
			name:     "windows command on linux",
			cmd:      "RunPowerShellScript",
			script:   []*string{new("dir")},
			wantCode: "ResourceNotFound",
		},
		{name: "missing script", cmd: "RunShellScript", wantCode: "InvalidParameter"},
		{name: "no agent", cmd: "RunShellScript", script: []*string{new("echo hi")}, wantCode: "OperationNotAllowed"},
		{name: "builtin without script", cmd: "ifconfig", wantCode: "OperationNotAllowed"},
		{
			name:     "stopped vm",
			cmd:      "RunShellScript",
			script:   []*string{new("true")},
			wantCode: "OperationNotAllowed",
			stop:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))

			if tt.stop {
				pl, err := s.cf.NewVirtualMachinesClient().BeginDeallocate(t.Context(), netRG, "vm1", nil)
				require.NoError(t, waitPoller(t, pl, err))
			}

			in := armcompute.RunCommandInput{CommandID: new(tt.cmd), Script: tt.script}
			pl, err := s.cf.NewVirtualMachinesClient().BeginRunCommand(t.Context(), netRG, "vm1", in, nil)
			err = waitPoller(t, pl, err)
			require.Error(t, err)
			assert.Equal(t, tt.wantCode, sdkErrCode(err))
		})
	}
}

func TestComputeSDK_RunCommandCatalog(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	pager := s.cf.NewVirtualMachineRunCommandsClient().NewListPager("westus", nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)

	ids := map[string]string{}
	for _, d := range page.Value {
		ids[*d.ID] = string(*d.OSType)
	}

	assert.Equal(t, "Linux", ids["RunShellScript"])
	assert.Equal(t, "Linux", ids["ifconfig"])
	assert.Equal(t, "Windows", ids["RunPowerShellScript"])
	assert.Equal(t, "Windows", ids["IPConfig"])
}

func TestComputeSDK_ManagedRunCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src      *armcompute.VirtualMachineRunCommandScriptSource
		name     string
		wantCode string
	}{
		{name: "script", src: &armcompute.VirtualMachineRunCommandScriptSource{Script: new("echo hi")}},
		{name: "command id", src: &armcompute.VirtualMachineRunCommandScriptSource{CommandID: new("ifconfig")}},
		{name: "windows command id", wantCode: "ResourceNotFound",
			src: &armcompute.VirtualMachineRunCommandScriptSource{CommandID: new("IPConfig")}},
		{name: "two sources", wantCode: "InvalidParameter",
			src: &armcompute.VirtualMachineRunCommandScriptSource{Script: new("x"), CommandID: new("ifconfig")}},
		{name: "no source", wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newCmpStack(t)
			s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))

			c := s.cf.NewVirtualMachineRunCommandsClient()
			rc := armcompute.VirtualMachineRunCommand{
				Location: new("westus"),
				Properties: &armcompute.VirtualMachineRunCommandProperties{
					Source:        tt.src,
					RunAsUser:     new("bob"),
					RunAsPassword: new(extSecret),
					ProtectedParameters: []*armcompute.RunCommandInputParameter{
						{Name: new("tok"), Value: new(extSecret)},
					},
					Parameters: []*armcompute.RunCommandInputParameter{{Name: new("p"), Value: new("v")}},
				},
			}
			pl, err := c.BeginCreateOrUpdate(t.Context(), netRG, "vm1", "rc1", rc, nil)
			err = waitPoller(t, pl, err)

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			got, err := c.GetByVirtualMachine(t.Context(), netRG, "vm1", "rc1", nil)
			require.NoError(t, err)
			assert.Nil(t, got.Properties.RunAsPassword)
			assert.Empty(t, got.Properties.ProtectedParameters)
			assert.Equal(t, "bob", *got.Properties.RunAsUser)
			assert.Equal(t, "Succeeded", *got.Properties.ProvisioningState)
			assert.Equal(t, "Unknown", string(*got.Properties.InstanceView.ExecutionState))

			raw, err := json.Marshal(s.handler.Snapshot(t.Context()))
			require.NoError(t, err)
			assert.NotContains(t, string(raw), extSecret)

			lp := c.NewListByVirtualMachinePager(netRG, "vm1", nil)
			page, err := lp.NextPage(t.Context())
			require.NoError(t, err)
			assert.Len(t, page.Value, 1)

			dl, err := c.BeginDelete(t.Context(), netRG, "vm1", "rc1", nil)
			require.NoError(t, waitPoller(t, dl, err))
		})
	}
}

func TestComputeSDK_ManagedRunCommandLimit(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))

	c := s.cf.NewVirtualMachineRunCommandsClient()

	var lastErr error

	for i := range 26 {
		rc := armcompute.VirtualMachineRunCommand{
			Location: new("westus"),
			Properties: &armcompute.VirtualMachineRunCommandProperties{
				Source: &armcompute.VirtualMachineRunCommandScriptSource{Script: new("true")},
			},
		}
		pl, err := c.BeginCreateOrUpdate(t.Context(), netRG, "vm1", "rc"+strings.Repeat("x", i+1), rc, nil)
		lastErr = waitPoller(t, pl, err)
	}

	require.Error(t, lastErr)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(lastErr))
}
