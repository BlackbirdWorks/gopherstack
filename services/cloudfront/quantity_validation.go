package cloudfront

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
)

// quantityNode is a generic XML element used to walk an arbitrary CloudFront config
// document (DistributionConfig, CachePolicyConfig, StreamingDistributionConfig, ...)
// looking for the pervasive
//
//	<X><Quantity>N</Quantity><Items>...</Items></X>
//
// pattern, without needing a fully-typed Go struct for every nested list in the
// CloudFront schema. AWS validates every one of these pairings and rejects a
// mismatch with InconsistentQuantities; because Go slices carry their own length,
// it is easy for an emulator to accept a caller-supplied Quantity that disagrees
// with the actual Items and silently ignore the mismatch.
type quantityNode struct {
	XMLName xml.Name
	Content string         `xml:",chardata"`
	Nodes   []quantityNode `xml:",any"`
}

// validateQuantities parses rawConfig as generic XML and verifies every
// Quantity/Items pairing found anywhere in the document is internally consistent.
// It returns an error wrapping ErrInconsistentQuantities describing the first
// mismatch found, or nil if the document is consistent (or not parseable as XML --
// malformed XML is the caller's own strict typed-unmarshal's job to report, as
// MalformedXML, not this generic pass's).
func validateQuantities(rawConfig []byte) error {
	if len(rawConfig) == 0 {
		return nil
	}

	var root quantityNode
	//nolint:musttag,nilerr // deliberately generic: any element shape is accepted, and
	// malformed/unrecognized XML here is intentionally not an error for this pass --
	// the caller's own strict typed-unmarshal already reports MalformedXML.
	if err := xml.Unmarshal(rawConfig, &root); err != nil {
		return nil
	}

	return checkQuantityNode(&root)
}

// checkQuantityNode recursively checks n and its descendants for Quantity/Items
// mismatches, depth-first, returning the first one found.
func checkQuantityNode(n *quantityNode) error {
	var (
		quantity    int
		hasQuantity bool
		items       *quantityNode
	)

	for i := range n.Nodes {
		child := &n.Nodes[i]

		switch child.XMLName.Local {
		case "Quantity":
			if v, err := strconv.Atoi(strings.TrimSpace(child.Content)); err == nil {
				quantity, hasQuantity = v, true
			}
		case "Items":
			items = child
		}
	}

	if hasQuantity {
		actual := 0
		if items != nil {
			actual = len(items.Nodes)
		}

		if quantity != actual {
			return fmt.Errorf(
				"%w: the parameter Quantity (%d) for the %s list does not match "+
					"the number of Items provided (%d)",
				ErrInconsistentQuantities, quantity, n.XMLName.Local, actual,
			)
		}
	}

	for i := range n.Nodes {
		if err := checkQuantityNode(&n.Nodes[i]); err != nil {
			return err
		}
	}

	return nil
}
