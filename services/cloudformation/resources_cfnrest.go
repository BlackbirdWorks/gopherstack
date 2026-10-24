package cloudformation

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

var errUnsupportedCFNValue = errors.New("unsupported property value")

// decodeCFNProps resolves intrinsics in props and JSON-decodes them into a wire-shaped input struct.
// A Tags array of {Key, Value} becomes the map the REST-JSON services expect; drop names keys to omit.
func decodeCFNProps(dst any, props map[string]any, params, physicalIDs map[string]string, drop ...string) error {
	resolved, _ := resolveDeep(props, params, physicalIDs).(map[string]any)
	if resolved == nil {
		resolved = map[string]any{}
	}

	for _, k := range drop {
		delete(resolved, k)
	}

	if _, isList := props["Tags"].([]any); isList {
		resolved["Tags"] = tagListProp(props, params, physicalIDs)
	}

	raw, err := json.Marshal(resolved)
	if err != nil {
		return fmt.Errorf("encode properties: %w", err)
	}

	if err = json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode properties: %w", err)
	}

	return nil
}

// ignoreRESTNotFound swallows a REST-JSON 404 so that deleting a vanished resource succeeds.
func ignoreRESTNotFound(err error) error {
	var re *restroute.Error
	if errors.As(err, &re) && re.Status == http.StatusNotFound {
		return nil
	}

	return err
}

// syncTagSet makes the current tags equal want through the supplied tag and untag calls.
func syncTagSet(current, want map[string]string, tag func(map[string]string) error, untag func([]string) error) error {
	var drop []string

	for k := range current {
		if _, keep := want[k]; !keep {
			drop = append(drop, k)
		}
	}

	slices.Sort(drop)

	if len(drop) > 0 {
		if err := untag(drop); err != nil {
			return err
		}
	}

	if len(want) > 0 {
		return tag(want)
	}

	return nil
}
