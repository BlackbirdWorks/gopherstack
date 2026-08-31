package main

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

// Class is one of the seven bug classes this campaign has distinguished.
// See the package doc for what each one means and how it differs from its
// neighbours -- the boundary between requestFieldNeverRead and
// filterDefaultSemantics in particular is not obvious from the name alone.
type Class string

const (
	ClassRequestFieldNeverRead  Class = "request_field_never_read"
	ClassWrongWireKey           Class = "wrong_wire_key"
	ClassErrorEnvelopeShape     Class = "error_envelope_shape"
	ClassFabricatedErrorCode    Class = "fabricated_error_code"
	ClassWrongEnumValue         Class = "wrong_enum_value"
	ClassPaginationOrdering     Class = "pagination_ordering"
	ClassFilterDefaultSemantics Class = "filter_default_semantics"
)

// KnownClasses is the complete, closed set. A row naming any class not in
// this list fails validation rather than being silently accepted.
var KnownClasses = []Class{ //nolint:gochecknoglobals // immutable lookup table
	ClassRequestFieldNeverRead,
	ClassWrongWireKey,
	ClassErrorEnvelopeShape,
	ClassFabricatedErrorCode,
	ClassWrongEnumValue,
	ClassPaginationOrdering,
	ClassFilterDefaultSemantics,
}

// Verdict is the outcome of one (service, class) check.
type Verdict string

const (
	// VerdictFixed: a real bug of this class was found and corrected in
	// the named commit.
	VerdictFixed Verdict = "fixed"
	// VerdictClean: the service was checked against this class and no
	// bug was found.
	VerdictClean Verdict = "clean"
	// VerdictInapplicable: the service has no surface for this class at
	// all (e.g. no filter parameters exist to have filter-value bugs).
	// Recorded so the class is never re-dispatched at this service.
	VerdictInapplicable Verdict = "inapplicable"
)

var knownVerdicts = map[Verdict]bool{ //nolint:gochecknoglobals // immutable lookup table
	VerdictFixed:        true,
	VerdictClean:        true,
	VerdictInapplicable: true,
}

// Row is one line of evidence: this service was checked for this class,
// with this verdict, established by this commit on this date.
type Row struct {
	Service string `yaml:"service"`
	Class   string `yaml:"class"`
	Verdict string `yaml:"verdict"`
	Date    string `yaml:"date"`
	Commit  string `yaml:"commit"`
}

type ledgerFile struct {
	Rows []Row `yaml:"rows"`
}

// LoadLedger reads and parses the YAML ledger at path. It does not
// validate the rows against services/ or the known class set -- call
// Validate separately, since a caller may want to load and validate
// against a different service root (tests do exactly this).
func LoadLedger(path string) ([]Row, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var lf ledgerFile

	if unmarshalErr := yaml.Unmarshal(data, &lf); unmarshalErr != nil {
		return nil, fmt.Errorf("parse %s: %w", path, unmarshalErr)
	}

	return lf.Rows, nil
}

// RowsForService returns every row naming service, sorted by class.
func RowsForService(rows []Row, service string) []Row {
	var out []Row

	for _, r := range rows {
		if r.Service == service {
			out = append(out, r)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Class < out[j].Class })

	return out
}

// MissingForClass returns every service in allServices that has no row at
// all for class, sorted. This is the targeting output: services safe to
// dispatch a fresh pass at, because nothing here claims they were already
// checked.
func MissingForClass(rows []Row, class string, allServices []string) []string {
	covered := make(map[string]bool, len(rows))

	for _, r := range rows {
		if r.Class == class {
			covered[r.Service] = true
		}
	}

	var missing []string

	for _, s := range allServices {
		if !covered[s] {
			missing = append(missing, s)
		}
	}

	sort.Strings(missing)

	return missing
}
