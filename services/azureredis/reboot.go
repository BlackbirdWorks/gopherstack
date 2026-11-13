package azureredis

import (
	"fmt"
	"strings"
)

// dataSurvivesReboot: a Basic cache is a single node, so any reboot loses its data; on Standard and Premium a
// replica holds it, and rebooting both nodes does not flush (Learn: cache-administration, "Will I lose data from my
// cache if I do a reboot?").
func dataSurvivesReboot(spec Spec, _ string) bool {
	return !strings.EqualFold(spec.SKU, SKUBasic)
}

// dropsConnections: rebooting the primary fails over and rebooting the cache clears every client connection; a
// replica reboot has no client effect (Learn: cache-administration, "Reboot").
func dropsConnections(spec Spec, rebootType string) bool {
	return strings.EqualFold(spec.SKU, SKUBasic) || !strings.EqualFold(rebootType, RebootSecondary)
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

// Reboot restarts the selected nodes. Data is flushed when no surviving node holds it; client connections drop
// unless only a replica reboots.
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

	if dropsConnections(c.spec, rebootType) {
		c.tls.dropConnections()
		c.plain.dropConnections()
	}

	return nil
}
