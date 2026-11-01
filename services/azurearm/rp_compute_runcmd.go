package azurearm

import (
	"net/http"
	"strings"
)

const (
	rcMaxManaged   = 25
	rcFieldSource  = "source"
	rcFieldCmdID   = "commandId"
	rcFieldScript  = "script"
	rcFieldURI     = "scriptUri"
	rcActionRun    = "runCommand"
	rcStateUnknown = "Unknown"
)

type runCommandDoc struct {
	id, os, description string
}

// runCommandDocs lists the action run commands on the Learn linux and windows run-command pages.
//
//nolint:gochecknoglobals // static read-only catalog
var runCommandDocs = []runCommandDoc{
	{"RunShellScript", osLinux, "Runs a Linux shell script."},
	{"ifconfig", osLinux, "Gets the configuration of all network interfaces."},
	{"RemoveRunCommandLinuxExtension", osLinux, "Removes the action run command extension."},
	{"RunPowerShellScript", osWindows, "Runs a PowerShell script"},
	{"DisableNLA", osWindows, "Disables Network Level Authentication (NLA)."},
	{"DisableWindowsUpdate", osWindows, "Disable Automatic Updates through Windows Update."},
	{"EnableAdminAccount", osWindows, "Checks if the local Administrator account is disabled, and if so enables it."},
	{"EnableEMS", osWindows, "Enable Emergency Management Services (EMS) to allow for serial console connection."},
	{"EnableRemotePS", osWindows, "Enable remote PowerShell."},
	{"EnableWindowsUpdate", osWindows, "Enable Automatic Updates through Windows Update."},
	{"IMDSCertCheck", osWindows, "Checks IMDS Health and then analyzes currently installed certificates."},
	{"IPConfig", osWindows, "Shows detailed information for the IP address, subnet mask and default gateway."},
	{"RDPSettings", osWindows, "Checks registry settings and domain policy settings."},
	{
		"ResetRDPCert",
		osWindows,
		"Removes the SSL certificate tied to the RDP listener and restores its security to default.",
	},
	{"SetRDPPort", osWindows, "Sets the default or user specified port number for Remote Desktop connections."},
	{"WindowsActivationValidation", osWindows, "Checks the current Windows license status."},
	{"WindowsGhostedNicValidationScript", osWindows, "Scans the Windows registry for ghosted network adapters."},
	{"WindowsUpgradeAssessmentValidation", osWindows, "Assesses readiness for an in-place OS upgrade."},
	{"RemoveRunCommandWindowsExtension", osWindows, "Removes the action run command extension."},
}

func findRunCommand(id string) (runCommandDoc, bool) {
	for _, d := range runCommandDocs {
		if strings.EqualFold(d.id, id) {
			return d, true
		}
	}

	return runCommandDoc{}, false
}

// RunCommandDocs returns the location-scoped list of action run commands.
func RunCommandDocs() map[string]any {
	out := make([]any, 0, len(runCommandDocs))

	for _, d := range runCommandDocs {
		out = append(out, map[string]any{
			cmpFieldID: d.id, cmpKeyOSType: d.os, "label": d.id, netPropDescription: d.description,
		})
	}

	return map[string]any{fieldValue: out}
}

func commandNotFound() error {
	return netErr(http.StatusNotFound, netCodeNotFound, "The entity was not found in this Azure location")
}

// RunCommand validates an action run command. No VM agent runs in the emulator, so a valid command on a running
// machine is refused instead of being given a fabricated result.
func (p *ComputeProvider) RunCommand(id ResourceID, body map[string]any) error {
	p.mu.RLock("RunCommand")
	defer p.mu.RUnlock()

	r, ok := p.top(cmpVMs, id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrResourceNotFound
	}

	cmdID := strOf(body, rcFieldCmdID)
	if cmdID == "" {
		return extReq(rcFieldCmdID)
	}

	doc, found := findRunCommand(cmdID)
	if !found || doc.os != vmOSType(r.Props) {
		return commandNotFound()
	}

	if (strings.EqualFold(cmdID, "RunShellScript") || strings.EqualFold(cmdID, "RunPowerShellScript")) &&
		len(strList(body[rcFieldScript])) == 0 {
		return extReq(rcFieldScript)
	}

	if power := r.Aux[auxPower]; power != powerRunning {
		return vmNotAllowed(rcActionRun, r.Name, "not running")
	}

	return cmpNotAllowed("Run Command cannot execute on VM '%s': the VM agent status is NOT READY (virtual machines "+
		"in the emulator do not run).", r.Name)
}

func putRunCommand(_ *ComputeProvider, parent *cmpRes, k, old *cmpKid, body map[string]any) error {
	props := cloneProps(propsOf(body))

	for _, f := range []string{"protectedParameters", "runAsPassword", fieldProvisioningState, cmpFieldInstView} {
		delete(props, f)
	}

	src := mapOf(props[rcFieldSource])
	if len(src) == 0 {
		return extReq("properties.source")
	}

	set := 0

	for _, f := range []string{rcFieldScript, rcFieldURI, rcFieldCmdID} {
		if strOf(src, f) != "" {
			set++
		}
	}

	if set != 1 {
		return cmpInvalid("Exactly one of properties.source.script, scriptUri or commandId must be specified.")
	}

	if id := strOf(src, rcFieldCmdID); id != "" {
		if doc, ok := findRunCommand(id); !ok || doc.os != vmOSType(parent.Props) {
			return commandNotFound()
		}
	}

	if t := intOf(props["timeoutInSeconds"]); t < 0 {
		return cmpInvalid("The timeoutInSeconds %d is not valid; it cannot be negative.", t)
	}

	if old == nil && len(kidsOf(parent, kidRunCommands)) >= rcMaxManaged {
		return cmpNotAllowed("The maximum number of managed run commands per virtual machine is %d.", rcMaxManaged)
	}

	setDefault(props, "asyncExecution", false)
	k.Props = props

	return nil
}

func renderRunCommand(p *ComputeProvider, parent *cmpRes, k *cmpKid, _ string) map[string]any {
	props := cloneProps(k.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded
	props[cmpFieldInstView] = map[string]any{
		"executionState":   rcStateUnknown,
		"executionMessage": "No VM agent runs in the emulator; the script is stored but never executed.",
	}

	return p.kidBase(parent, k, props)
}
