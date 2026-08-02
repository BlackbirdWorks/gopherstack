package directconnect

import (
	"bytes"
	"fmt"
)

// PDF object numbers used by placeholderLoaContent's minimal single-page
// document, named rather than left as bare literals: objCatalog is the
// document root, objPages the (single-entry) page tree, objPage the one
// page itself, objContentStream its text-drawing stream, objFont the
// Helvetica font resource objPage references.
const (
	objCatalog        = 1
	objPages          = 2
	objPage           = 3
	objContentStream  = 4
	objFont           = 5
	pdfObjectCount    = objFont // highest object number == total object count
	xrefFreeEntry     = "0000000000 65535 f \n"
	xrefInUseEntryFmt = "%010d 00000 n \n"
)

// placeholderLoaContent returns a minimal, well-formed (correct xref
// offsets, computed at build time rather than hand-typed) single-page PDF
// stating plainly that it is a placeholder, for the three LOA-CFA
// (Letter of Authorization - Connecting Facility Assignment) ops. Real AWS
// generates an actual signed PDF authorizing physical cross-connect work at
// a colocation facility; this emulator has no such document to generate and
// must not fabricate one that reads as real -- see PARITY.md's honest-gap
// section. resourceID is embedded in the page text purely so two different
// LOA requests are byte-distinguishable, not because it carries any real
// meaning.
func placeholderLoaContent(resourceID string) []byte {
	stream := "BT /F1 12 Tf 72 720 Td (GOPHERSTACK PLACEHOLDER LOA-CFA - NOT A REAL AUTHORIZATION - resource: " +
		resourceID + ") Tj ET"

	var buf bytes.Buffer

	buf.WriteString("%PDF-1.4\n")

	// offsets[n] is the byte offset of object n; offsets[0] is unused (the
	// free-list head is never written as a real object).
	offsets := make([]int, 1, pdfObjectCount+1)

	writeObj := func(format string, args ...any) {
		offsets = append(offsets, buf.Len())
		fmt.Fprintf(&buf, format, args...)
	}

	writeObj("%d 0 obj\n<</Type/Catalog/Pages %d 0 R>>\nendobj\n", objCatalog, objPages)
	writeObj("%d 0 obj\n<</Type/Pages/Kids[%d 0 R]/Count 1>>\nendobj\n", objPages, objPage)
	writeObj(
		"%d 0 obj\n<</Type/Page/Parent %d 0 R/MediaBox[0 0 612 792]"+
			"/Resources<</Font<</F1 %d 0 R>>>>/Contents %d 0 R>>\nendobj\n",
		objPage, objPages, objFont, objContentStream,
	)
	writeObj("%d 0 obj\n<</Length %d>>\nstream\n%s\nendstream\nendobj\n", objContentStream, len(stream), stream)
	writeObj("%d 0 obj\n<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>\nendobj\n", objFont)

	xrefStart := buf.Len()
	objCount := len(offsets)
	fmt.Fprintf(&buf, "xref\n0 %d\n", objCount)
	buf.WriteString(xrefFreeEntry)

	for _, off := range offsets[1:] {
		fmt.Fprintf(&buf, xrefInUseEntryFmt, off)
	}

	fmt.Fprintf(&buf, "trailer\n<</Size %d/Root %d 0 R>>\n", objCount, objCatalog)
	fmt.Fprintf(&buf, "startxref\n%d\n%%%%EOF", xrefStart)

	return buf.Bytes()
}

// DescribeLoa returns the deprecated, flattened LOA-CFA shape for a
// Connection (PARITY.md wire-trap #7).
func (b *InMemoryBackend) DescribeLoa(connectionID string) ([]byte, error) {
	b.mu.RLock("DescribeLoa")
	defer b.mu.RUnlock()

	if _, ok := b.connections.Get(connectionID); !ok {
		return nil, notFoundError(resourceConnection, connectionID)
	}

	return placeholderLoaContent(connectionID), nil
}

// DescribeConnectionLoa returns the current, nested LOA-CFA shape for a
// Connection, and records LoaIssueTime.
func (b *InMemoryBackend) DescribeConnectionLoa(connectionID string) ([]byte, error) {
	b.mu.Lock("DescribeConnectionLoa")
	defer b.mu.Unlock()

	c, ok := b.connections.Get(connectionID)
	if !ok {
		return nil, notFoundError(resourceConnection, connectionID)
	}

	now := nowUTC()
	c.LoaIssueTime = &now

	return placeholderLoaContent(connectionID), nil
}

// DescribeInterconnectLoa returns the LOA-CFA for an Interconnect.
func (b *InMemoryBackend) DescribeInterconnectLoa(interconnectID string) ([]byte, error) {
	b.mu.RLock("DescribeInterconnectLoa")
	defer b.mu.RUnlock()

	if _, ok := b.interconnects.Get(interconnectID); !ok {
		return nil, notFoundError(resourceInterconnect, interconnectID)
	}

	return placeholderLoaContent(interconnectID), nil
}
