package azurearm

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

const (
	osLinux   = "Linux"
	osWindows = "Windows"

	cmpFieldOSProfile = "osProfile"
	cmpFieldAdminUser = "adminUsername"
	cmpFieldAdminPass = "adminPassword"
	cmpFieldComputer  = "computerName"
	cmpFieldLinuxCfg  = "linuxConfiguration"
	cmpFieldWinCfg    = "windowsConfiguration"

	winUserMax  = 20
	linUserMax  = 64
	winCompMax  = 15
	linCompMax  = 64
	winPassMin  = 8
	winPassMax  = 123
	linPassMin  = 6
	linPassMax  = 72
	passClasses = 3
)

//nolint:gochecknoglobals // static read-only lists documented on Microsoft Learn
var (
	cmpReservedUsers = strings.Fields("1 123 a actuser adm admin admin1 admin2 administrator aspnet backup console " +
		"david guest john owner root server sql support_388945a0 support sys test test1 test2 test3 user user1 user2")
	cmpBannedPasswords = strings.Fields("abc@123 iloveyou! p@$$w0rd p@ssw0rd p@ssword123 pa$$word pass@word1 " +
		"password! password1 password22")
	winCompBadChars = regexp.MustCompile("[`~!@#$%^&*()=+_\\[\\]{}\\\\|;:.'\",<>/?]")
	linCompRe       = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$`)
	allDigitsRe     = regexp.MustCompile(`^[0-9]+$`)
	sshKeyTypes     = strings.Fields("ssh-rsa ssh-ed25519 ecdsa-sha2-nistp256 ecdsa-sha2-nistp384 ecdsa-sha2-nistp521")
)

func isWindowsPublisher(pub string) bool {
	l := strings.ToLower(pub)

	return strings.HasPrefix(l, "microsoftwindows") || l == "microsoftsqlserver" || l == "microsoftvisualstudio"
}

// vmOSType decides a VM's operating system from its OS disk, OS profile and image.
func vmOSType(props map[string]any) string {
	sp := mapOf(props["storageProfile"])
	osp := mapOf(props[cmpFieldOSProfile])

	switch {
	case osp[cmpFieldLinuxCfg] != nil:
		return osLinux
	case osp[cmpFieldWinCfg] != nil:
		return osWindows
	}

	if t := strOf(mapOf(sp["osDisk"]), cmpKeyOSType); t != "" {
		if strings.EqualFold(t, osWindows) {
			return osWindows
		}

		return osLinux
	}

	if isWindowsPublisher(strOf(mapOf(sp["imageReference"]), "publisher")) {
		return osWindows
	}

	return osLinux
}

func validateAdminUser(osType, user string) error {
	maxLen := linUserMax
	if osType == osWindows {
		maxLen = winUserMax
	}

	switch {
	case user == "":
		return cmpInvalid("Required parameter 'adminUsername' is missing (null).")
	case len(user) > maxLen:
		return cmpInvalid(
			"The Admin Username specified is not valid. For %s VMs it must be at most %d characters long.",
			osType,
			maxLen,
		)
	case osType == osWindows && strings.HasSuffix(user, "."):
		return cmpInvalid("The Admin Username specified is not valid. It cannot end in a period (\".\").")
	case containsFold(cmpReservedUsers, user):
		return cmpInvalid("The Admin Username specified is not allowed.")
	}

	return nil
}

func validateAdminPassword(osType, pass string) error {
	lo, hi := linPassMin, linPassMax
	if osType == osWindows {
		lo, hi = winPassMin, winPassMax
	}

	if len(pass) < lo || len(pass) > hi {
		return cmpInvalid(
			"The supplied password must be between %d-%d characters long and must satisfy at least %d of "+
				"password complexity requirements from the following: 1) Contains an uppercase character 2) Contains a "+
				"lowercase character 3) Contains a numeric digit 4) Contains a special character 5) Control characters "+
				"are not allowed",
			lo,
			hi,
			passClasses,
		)
	}

	var upper, lower, digit, special bool

	for _, c := range pass {
		switch {
		case unicode.IsControl(c):
			return cmpInvalid("The supplied password must not contain control characters.")
		case unicode.IsUpper(c):
			upper = true
		case unicode.IsLower(c):
			lower = true
		case unicode.IsDigit(c):
			digit = true
		default:
			special = true
		}
	}

	classes := 0
	for _, ok := range []bool{upper, lower, digit, special} {
		if ok {
			classes++
		}
	}

	if classes < passClasses || containsFold(cmpBannedPasswords, pass) {
		return cmpInvalid(
			"The supplied password must be between %d-%d characters long and must satisfy at least %d of "+
				"password complexity requirements from the following: 1) Contains an uppercase character 2) Contains a "+
				"lowercase character 3) Contains a numeric digit 4) Contains a special character 5) Control characters "+
				"are not allowed",
			lo,
			hi,
			passClasses,
		)
	}

	return nil
}

func validateComputerName(osType, name string) error {
	if osType == osWindows {
		if len(name) > winCompMax || allDigitsRe.MatchString(name) || winCompBadChars.MatchString(name) || name == "" {
			return cmpInvalid(
				"The Windows computer name cannot be more than 15 characters long, be entirely numeric, " +
					"or contain the following characters: ` ~ ! @ # $ %% ^ & * ( ) = + _ [ ] { } \\ | ; : . ' \" , < > / ?.",
			)
		}

		return nil
	}

	if len(name) > linCompMax || !linCompRe.MatchString(name) {
		return cmpInvalid("The Linux computer name '%s' is not valid. It must be 1-%d characters of letters, digits, "+
			"'-' and '.'.", name, linCompMax)
	}

	return nil
}

func validateSSHKeys(user string, lin map[string]any) (bool, error) {
	keys := anyList(mapOf(lin["ssh"])["publicKeys"])

	for _, k := range keys {
		km := mapOf(k)
		if want := "/home/" + user + "/.ssh/authorized_keys"; strOf(km, "path") != want {
			return false, cmpInvalid(
				"Destination path for SSH public keys is currently limited to its default value %s "+
					"due to a known issue in Linux provisioning agent.",
				want,
			)
		}

		if err := validateSSHKeyData(strOf(km, "keyData")); err != nil {
			return false, err
		}
	}

	return len(keys) > 0, nil
}

func validateSSHKeyData(data string) error {
	fields := strings.Fields(data)
	if len(fields) < 2 || !containsFold(sshKeyTypes, fields[0]) {
		return cmpInvalid("Invalid value for linuxConfiguration.ssh.publicKeys.keyData: the key must be an OpenSSH " +
			"public key such as 'ssh-rsa AAAA...'.")
	}

	if _, err := base64.StdEncoding.DecodeString(fields[1]); err != nil {
		return cmpInvalid(
			"Invalid value for linuxConfiguration.ssh.publicKeys.keyData: the key data is not valid base64.",
		)
	}

	return nil
}

// buildOSProfile validates a VM's osProfile and returns the form that is stored: secrets are dropped.
func buildOSProfile(vmName, osType string, osp map[string]any, existing map[string]any) (map[string]any, error) {
	comp := strOf(osp, cmpFieldComputer)
	if comp == "" {
		comp = strOf(existing, cmpFieldComputer)
	}

	if comp == "" {
		comp = vmName
	}

	if err := validateComputerName(osType, comp); err != nil {
		return nil, err
	}

	user := strOf(osp, cmpFieldAdminUser)
	if user == "" {
		user = strOf(existing, cmpFieldAdminUser)
	}

	if err := validateAdminUser(osType, user); err != nil {
		return nil, err
	}

	if err := checkOSAuth(osType, user, osp, existing != nil); err != nil {
		return nil, err
	}

	out := map[string]any{cmpFieldComputer: comp, cmpFieldAdminUser: user}

	keep := []string{cmpFieldLinuxCfg, cmpFieldWinCfg, "allowExtensionOperations", "requireGuestProvisionSignal"}

	for _, k := range keep {
		if v, ok := osp[k]; ok {
			out[k] = deepClone(v)
		}
	}

	if _, ok := out["allowExtensionOperations"]; !ok {
		out["allowExtensionOperations"] = true
	}

	if _, ok := out["requireGuestProvisionSignal"]; !ok {
		out["requireGuestProvisionSignal"] = true
	}

	return out, nil
}

func checkOSAuth(osType, user string, osp map[string]any, update bool) error {
	pass := strOf(osp, cmpFieldAdminPass)

	if osType == osWindows {
		return checkWindowsAuth(pass, update)
	}

	return checkLinuxAuth(user, pass, osp, update)
}

func checkWindowsAuth(pass string, update bool) error {
	switch {
	case pass != "":
		return validateAdminPassword(osWindows, pass)
	case update:
		return nil
	}

	return cmpInvalid("Required parameter 'adminPassword' is missing (null).")
}

func checkLinuxAuth(user, pass string, osp map[string]any, update bool) error {
	lin := mapOf(osp[cmpFieldLinuxCfg])
	disabled, _ := lin["disablePasswordAuthentication"].(bool)

	hasKeys, err := validateSSHKeys(user, lin)
	if err != nil {
		return err
	}

	switch {
	case disabled && pass != "":
		return cmpInvalid("Parameter 'osProfile.adminPassword' is not allowed.")
	case disabled && !hasKeys && !update:
		return cmpInvalid("Required parameter 'linuxConfiguration.ssh.publicKeys' is missing: SSH public keys are " +
			"required when password authentication is disabled.")
	case pass != "":
		return validateAdminPassword(osLinux, pass)
	case !disabled && !hasKeys && !update:
		return cmpInvalid("Required parameter 'adminPassword' is missing (null).")
	}

	return nil
}

func imageRefOK(ref map[string]any) error {
	if strOf(ref, cmpFieldID) != "" {
		return nil
	}

	for k := range strings.FieldsSeq("publisher offer sku version") {
		if strOf(ref, k) == "" {
			return cmpInvalid("Required parameter 'storageProfile.imageReference.%s' is missing (null).", k)
		}
	}

	if v := strOf(ref, "version"); !strings.EqualFold(v, "latest") && !imageVersionRe.MatchString(v) {
		return cmpInvalid("The image version '%s' is not valid; use 'latest' or a Major.Minor.Build version.", v)
	}

	return nil
}

var imageVersionRe = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

func platformImageID(sub, loc string, ref map[string]any) string {
	if id := strOf(ref, cmpFieldID); id != "" {
		return id
	}

	return fmt.Sprintf(
		"/Subscriptions/%s/Providers/Microsoft.Compute/Locations/%s/Publishers/%s/ArtifactTypes/VMImage/"+
			"Offers/%s/Skus/%s/Versions/%s",
		sub,
		loc,
		strOf(ref, "publisher"),
		strOf(ref, "offer"),
		strOf(ref, "sku"),
		strOf(ref, "version"),
	)
}
