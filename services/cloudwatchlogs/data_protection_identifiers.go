package cloudwatchlogs

import (
	"regexp"
	"strings"
)

const (
	cpfLen          = 11
	cnpjLen         = 14
	nifLetters      = "TRWAGMYFPDXBNJZSQVHLCKE"
	nifLetterModulo = 23
	deaDigits       = 7
	mod11           = 11
	decimalBase     = 10
)

// formatManagedMaskRules are identifiers with a checksum or a distinctive national format.
// Identifiers AWS keys on nearby keywords (Name, Address, PhoneNumber, ZipCode, ...) are not modeled.
func formatManagedMaskRules() map[string]maskRule {
	return map[string]maskRule{
		"CpfCode-BR": {re: regexp.MustCompile(`\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b`), valid: cpfValid},
		"Cnpj-BR":    {re: regexp.MustCompile(`\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b`), valid: cnpjValid},
		"CepCode-BR": {re: regexp.MustCompile(`\b\d{5}-\d{3}\b`)},
		"NationalInsuranceNumber-GB": {
			re: regexp.MustCompile(`\b[A-CEGHJ-PR-TW-Z][A-CEGHJ-NPR-TW-Z] ?\d{2} ?\d{2} ?\d{2} ?[A-D]\b`),
			valid: func(s string) bool {
				switch strings.ReplaceAll(s, " ", "")[:2] {
				case "BG", "GB", "NK", "KN", "TN", "NT", "ZZ":
					return false
				}

				return true
			},
		},
		"PostalCode-CA": {
			re: regexp.MustCompile(`\b[ABCEGHJ-NPRSTVXY]\d[ABCEGHJ-NPRSTV-Z][ -]?\d[ABCEGHJ-NPRSTV-Z]\d\b`),
		},
		"DrugEnforcementAgencyNumber-US": {
			re:    regexp.MustCompile(`\b[ABCDEFGHJKLMPRSTUX][A-Z]\d{7}\b`),
			valid: deaValid,
		},
		"IndividualTaxIdentificationNumber-US": {re: regexp.MustCompile(
			`\b9\d{2}-(?:5\d|6[0-5]|7\d|8[0-8]|9[0-24-9])-\d{4}\b`)},
		"NifNumber-ES": {re: regexp.MustCompile(`\b\d{8}[A-Z]\b`), valid: nifValid},
		"NieNumber-ES": {re: regexp.MustCompile(`\b[XYZ]\d{7}[A-Z]\b`), valid: nieValid},
	}
}

func digitsOf(s string) []int {
	out := make([]int, 0, len(s))

	for _, c := range s {
		if c >= '0' && c <= '9' {
			out = append(out, int(c-'0'))
		}
	}

	return out
}

func allSame(d []int) bool {
	for _, v := range d[1:] {
		if v != d[0] {
			return false
		}
	}

	return true
}

func weightedCheck(d []int, weights []int) int {
	sum := 0
	for i, w := range weights {
		sum += d[i] * w
	}

	return sum
}

func cpfValid(s string) bool {
	d := digitsOf(s)
	if len(d) != cpfLen || allSame(d) {
		return false
	}

	first := weightedCheck(d, []int{10, 9, 8, 7, 6, 5, 4, 3, 2}) * decimalBase % mod11 % decimalBase
	second := weightedCheck(d, []int{11, 10, 9, 8, 7, 6, 5, 4, 3, 2}) * decimalBase % mod11 % decimalBase

	return d[9] == first && d[10] == second
}

func cnpjValid(s string) bool {
	d := digitsOf(s)
	if len(d) != cnpjLen || allSame(d) {
		return false
	}

	check := func(w []int) int {
		if r := weightedCheck(d, w) % mod11; r > 1 {
			return mod11 - r
		}

		return 0
	}

	return d[12] == check([]int{5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2}) &&
		d[13] == check([]int{6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2})
}

func deaValid(s string) bool {
	d := digitsOf(s)
	if len(d) != deaDigits {
		return false
	}

	return (d[0]+d[2]+d[4]+2*(d[1]+d[3]+d[5]))%decimalBase == d[6]
}

func nifLetter(n int) byte { return nifLetters[n%nifLetterModulo] }

func nifValid(s string) bool {
	n := 0
	for _, v := range digitsOf(s) {
		n = n*decimalBase + v
	}

	return s[len(s)-1] == nifLetter(n)
}

func nieValid(s string) bool {
	n := strings.IndexByte("XYZ", s[0])
	for _, v := range digitsOf(s) {
		n = n*decimalBase + v
	}

	return s[len(s)-1] == nifLetter(n)
}
