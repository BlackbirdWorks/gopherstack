package gcpeventarc

import (
	"regexp"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpevents"
)

// eventType returns the value of the trigger's exact "type" filter.
func eventType(filters []*eventarcpb.EventFilter) string {
	for _, f := range filters {
		if f.GetAttribute() == attrType {
			return f.GetValue()
		}
	}

	return ""
}

func validateFilters(filters []*eventarcpb.EventFilter, hasChannel bool) error {
	if len(filters) == 0 {
		return errInvalid("event_filters must contain a filter for the type attribute.")
	}

	seen := map[string]bool{}

	for _, f := range filters {
		if f.GetAttribute() == "" || f.GetValue() == "" {
			return errInvalid("Every event filter needs an attribute and a value.")
		}

		if seen[f.GetAttribute()] {
			return errInvalid("Attribute %q is filtered more than once.", f.GetAttribute())
		}

		seen[f.GetAttribute()] = true

		switch f.GetOperator() {
		case "":
		case operatorPathPattern:
			if _, err := compilePathPattern(f.GetValue()); err != nil {
				return errInvalid("Invalid path pattern %q for attribute %q.", f.GetValue(), f.GetAttribute())
			}
		default:
			return errInvalid("Unsupported operator %q for attribute %q: want %q or none.",
				f.GetOperator(), f.GetAttribute(), operatorPathPattern)
		}
	}

	t := eventType(filters)
	if t == "" {
		return errInvalid("event_filters must contain a filter for the type attribute.")
	}

	if hasChannel {
		return nil
	}

	if !isSupportedType(t) {
		return errInvalid("Invalid event type %q: the supported types are listed by ListProviders.", t)
	}

	return validateTypedFilters(t, filters)
}

func validateTypedFilters(t string, filters []*eventarcpb.EventFilter) error {
	specs := filteringAttrs(t)

	for _, f := range filters {
		if f.GetAttribute() == attrType {
			if f.GetOperator() != "" {
				return errInvalid("The type filter must match exactly.")
			}

			continue
		}

		spec, ok := findSpec(specs, f.GetAttribute())
		if !ok {
			return errInvalid("Attribute %q cannot be used to filter events of type %q.", f.GetAttribute(), t)
		}

		if f.GetOperator() == operatorPathPattern && !spec.pathPattern {
			return errInvalid("Attribute %q does not support path patterns.", f.GetAttribute())
		}
	}

	for _, s := range specs {
		if s.required && !hasAttr(filters, s.name) {
			return errInvalid("Events of type %q require a filter for the %q attribute.", t, s.name)
		}
	}

	return nil
}

func findSpec(specs []attrSpec, name string) (attrSpec, bool) {
	for _, s := range specs {
		if s.name == name {
			return s, true
		}
	}

	return attrSpec{}, false
}

func hasAttr(filters []*eventarcpb.EventFilter, name string) bool {
	for _, f := range filters {
		if f.GetAttribute() == name {
			return true
		}
	}

	return false
}

// matches reports whether an event with the given attributes passes every filter.
func matches(filters []*eventarcpb.EventFilter, attrs map[string]string) bool {
	for _, f := range filters {
		got, ok := attrs[f.GetAttribute()]
		if !ok {
			return false
		}

		if f.GetOperator() == operatorPathPattern {
			re, err := compilePathPattern(f.GetValue())
			if err != nil || !re.MatchString(got) {
				return false
			}

			continue
		}

		if got != f.GetValue() {
			return false
		}
	}

	return true
}

func compilePathPattern(p string) (*regexp.Regexp, error) {
	re, err := gcpevents.CompilePathPattern(p)
	if err != nil {
		return nil, errInvalid("bad pattern %q", p)
	}

	return re, nil
}
