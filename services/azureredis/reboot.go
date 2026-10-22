package azureredis

import (
	"fmt"
	"strings"
)

// dataSurvivesReboot encodes Azure's behavior: a Basic cache is a single node, so any reboot loses its data; a
// Standard or Premium cache keeps a replica, so rebooting one node keeps the data, and rebooting both keeps it
// only when persistence (RDB or AOF backup) is on.
func dataSurvivesReboot(spec Spec, rebootType string) bool {
	if strings.EqualFold(spec.SKU, SKUBasic) {
		return false
	}

	if strings.EqualFold(rebootType, RebootAll) {
		return spec.Persistence
	}

	return true
}

func validRebootType(spec Spec, rebootType string) error {
	switch {
	case strings.EqualFold(rebootType, RebootAll), strings.EqualFold(rebootType, RebootPrimary):
		return nil
	case strings.EqualFold(rebootType, RebootSecondary) && !strings.EqualFold(spec.SKU, SKUBasic):
		return nil
	}

	return fmt.Errorf("%w: rebootType %q is not valid for a %s cache", ErrInvalid, rebootType, spec.SKU)
}

// Reboot restarts the selected nodes. Data is flushed when no surviving node or persisted copy holds it.
func (b *InMemoryBackend) Reboot(key, rebootType string) error {
	b.mu.Lock("Reboot")
	defer b.mu.Unlock()

	c, err := b.lookup(key)
	if err != nil {
		return err
	}

	if err = validRebootType(c.spec, rebootType); err != nil {
		return err
	}

	if !dataSurvivesReboot(c.spec, rebootType) {
		c.engine.Mini.FlushAll()
	}

	return nil
}
