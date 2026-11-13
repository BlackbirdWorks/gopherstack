package ocidatabase

import (
	"fmt"
	"math"
	"net/netip"
	"slices"
	"strings"
	"unicode"
)

// Limits. Password, dbName and wallet-password rules are from the SDK CreateAutonomousDatabaseDetails and
// GenerateAutonomousDatabaseWalletDetails docs; the compute and storage ranges are recalled, not in the SDK.
const (
	passwordMin   = 12
	passwordMax   = 30
	dbNameMax     = 30
	walletPwMin   = 8
	ecpuMin       = 2
	ecpuMax       = 512
	ocpuMin       = 1
	ocpuMax       = 128
	storageTBsMin = 1
	storageTBsMax = 384
	storageGBsMin = 20
	gbPerTB       = 1024
	freeStorage   = 20
	freeECPU      = 2
	freeOCPU      = 1
	freeTierMax   = 2
	defaultECPU   = 2
	paidECPU      = 4
	maxACLEntries = 1024
	versionOldest = "19c"
	versionNew    = "26ai"
	versionMid    = "23ai"
	licenseIncl   = "LICENSE_INCLUDED"
	licenseByol   = "BRING_YOUR_OWN_LICENSE"
	charsetDef    = "AL32UTF8"
	ncharsetDef   = "AL16UTF16"
)

func validDBName(s string) bool {
	if s == "" || len(s) > dbNameMax || !isASCIILetter(rune(s[0])) {
		return false
	}

	for _, r := range s {
		if !isASCIILetter(r) && (r < '0' || r > '9') {
			return false
		}
	}

	return true
}

func isASCIILetter(r rune) bool { return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') }

func checkAdminPassword(pw string) *APIError {
	if len(pw) < passwordMin || len(pw) > passwordMax {
		return errParam("adminPassword must be between 12 and 30 characters long")
	}

	var upper, lower, digit bool

	for _, r := range pw {
		switch {
		case unicode.IsUpper(r):
			upper = true
		case unicode.IsLower(r):
			lower = true
		case unicode.IsDigit(r):
			digit = true
		}
	}

	if !upper || !lower || !digit {
		return errParam("adminPassword must contain at least 1 uppercase, 1 lowercase and 1 numeric character")
	}

	if strings.Contains(pw, `"`) {
		return errParam(`adminPassword cannot contain the double quote symbol (")`)
	}

	if strings.Contains(strings.ToLower(pw), "admin") {
		return errParam(`adminPassword cannot contain the username "admin", regardless of casing`)
	}

	return nil
}

func checkWalletPassword(pw string) *APIError {
	var letter, other bool

	for _, r := range pw {
		if unicode.IsLetter(r) {
			letter = true
		} else {
			other = true
		}
	}

	if len(pw) < walletPwMin || !letter || !other {
		return errParam("password must be at least 8 characters long and include at least 1 letter and either " +
			"1 numeric character or 1 special character")
	}

	return nil
}

func oneOf(field, v string, allowed ...string) *APIError {
	if !slices.Contains(allowed, v) {
		return errParam(fmt.Sprintf("%s must be one of %s", field, strings.Join(allowed, ", ")))
	}

	return nil
}

func checkWorkload(v string) *APIError {
	return oneOf("dbWorkload", v, workloadOLTP, workloadDW, workloadAJD, workloadAPEX, workloadLH)
}

func checkVersion(v string, engine bool) *APIError {
	if engine {
		return oneOf("dbVersion", v, versionNew, versionMid)
	}

	return oneOf("dbVersion", v, versionOldest, versionMid, versionNew)
}

func checkACL(entries []string) *APIError {
	if len(entries) > maxACLEntries {
		return errParam("whitelistedIps has too many entries")
	}

	for _, e := range entries {
		head, tail, hasTail := strings.Cut(e, ";")
		if strings.HasPrefix(head, "ocid1.vcn.") {
			if hasTail && !validIPRule(tail) {
				return errParam("whitelistedIps entry " + e + " is not a VCN OCID with an optional IP or CIDR")
			}

			continue
		}

		if hasTail || !validIPRule(e) {
			return errParam("whitelistedIps entry " + e + " is not an IP address, a CIDR block or a VCN OCID")
		}
	}

	return nil
}

func validIPRule(s string) bool {
	if _, err := netip.ParsePrefix(s); err == nil {
		return true
	}

	_, err := netip.ParseAddr(s)

	return err == nil
}

type computeSpec struct {
	model string
	count float32
}

func wholeNumber(f float64) bool { return f == math.Trunc(f) }

// resolveCompute applies the computeModel, computeCount, cpuCoreCount and ocpuCount rules of the SDK docs.
func resolveCompute(model *string, count, ocpu *float64, cores *int) (computeSpec, *APIError) {
	if cores != nil && (count != nil || ocpu != nil) {
		return computeSpec{}, errParam("cpuCoreCount cannot be used with computeCount or ocpuCount")
	}

	if model != nil && count == nil && ocpu == nil && cores == nil {
		return computeSpec{}, errMissing("computeCount is required when computeModel is set")
	}

	out := computeSpec{model: modelECPU, count: defaultECPU}

	if cores != nil || ocpu != nil {
		out.model = modelOCPU
	}

	if model != nil {
		if aerr := oneOf("computeModel", *model, modelECPU, modelOCPU); aerr != nil {
			return computeSpec{}, aerr
		}

		out.model = *model
	}

	out.count = pickCount(out, count, ocpu, cores)

	return out, checkComputeRange(out)
}

func pickCount(c computeSpec, count, ocpu *float64, cores *int) float32 {
	switch {
	case cores != nil:
		return float32(*cores)
	case ocpu != nil:
		return float32(*ocpu)
	case count != nil:
		return float32(*count)
	case c.model == modelOCPU:
		return ocpuMin
	}

	return c.count
}

func checkComputeRange(c computeSpec) *APIError {
	if !wholeNumber(float64(c.count)) {
		return errParam("fractional compute counts are not supported for Autonomous Database Serverless")
	}

	lo, hi := float32(ecpuMin), float32(ecpuMax)
	if c.model == modelOCPU {
		lo, hi = ocpuMin, ocpuMax
	}

	if c.count < lo || c.count > hi {
		return errParam(fmt.Sprintf("computeCount for the %s compute model must be between %v and %v", c.model, lo, hi))
	}

	return nil
}

// resolveStorage turns dataStorageSizeInTBs or dataStorageSizeInGBs into gigabytes.
func resolveStorage(tbs, gbs *int, def int) (int, *APIError) {
	switch {
	case tbs != nil && gbs != nil:
		return 0, errParam("dataStorageSizeInTBs cannot be used with dataStorageSizeInGBs")
	case tbs != nil:
		if *tbs < storageTBsMin || *tbs > storageTBsMax {
			return 0, errParam("dataStorageSizeInTBs must be between 1 and 384")
		}

		return *tbs * gbPerTB, nil
	case gbs != nil:
		if *gbs < storageGBsMin || *gbs > storageTBsMax*gbPerTB {
			return 0, errParam("dataStorageSizeInGBs must be between 20 and 393216")
		}

		return *gbs, nil
	}

	return def, nil
}

func storageTBs(gbs int) int { return max(storageTBsMin, (gbs+gbPerTB-1)/gbPerTB) }

func deref[T any](p *T, def T) T {
	if p == nil {
		return def
	}

	return *p
}
