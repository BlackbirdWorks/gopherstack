package textract

import (
	"encoding/base64"
	"strconv"

	"github.com/google/uuid"
)

const (
	confidencePage  = 99.0
	confidenceLine  = 99.5
	confidenceWord1 = 99.8
	confidenceWord2 = 99.7
	confidenceWord3 = 99.9
	blockTypeWord   = "WORD"
	textTypePrinted = "PRINTED"

	// Block type string constants to avoid repeated literals.
	blockTypeKeyValueSet = "KEY_VALUE_SET"
	blockTypeCell        = "CELL"

	// Relationship type constants.
	relTypeChild  = "CHILD"
	relTypeValue  = "VALUE"
	relTypeAnswer = "ANSWER"

	// Confidence constants for generated blocks.
	confidenceKV          = 95.0
	confidenceKV2         = 93.0
	confidenceWordForm    = 99.0
	confidenceSelElem     = 97.0
	confidenceTable       = 99.0
	confidenceTableCell   = 98.0
	confidenceQuery       = 100.0
	confidenceQueryResult = 50.0
	confidenceSignature   = 80.0
	confidenceLayout      = 95.0

	// Geometry coordinate constants for generated blocks.
	geoLeft   = 0.05
	geoTop1   = 0.05
	geoTop2   = 0.12
	geoWidth  = 0.9
	geoHeight = 0.04
	geoWide   = 0.6

	geoWordLeft1  = 0.05
	geoWordLeft2  = 0.16
	geoWordLeft3  = 0.27
	geoWordWidth  = 0.10
	geoWordWidth3 = 0.06

	geoFormValLeft = 0.36
	geoFormWidth   = 0.30
	geoFormTop1    = 0.22
	geoFormTop2    = 0.28
	geoSelTop      = 0.34
	geoSelWidth    = 0.02
	geoSelHeight   = 0.02

	geoTableLeft   = 0.05
	geoTableTop    = 0.40
	geoTableWidth  = 0.80
	geoTableHeight = 0.20
	geoCell1Left   = 0.05
	geoCell1Width  = 0.40
	geoCell2Left   = 0.45
	geoCellTop1    = 0.40
	geoCellTop2    = 0.48
	geoCellHeight  = 0.08

	geoSigLeft   = 0.05
	geoSigTop    = 0.65
	geoSigWidth  = 0.20
	geoSigHeight = 0.06

	geoHdrLeft   = 0.05
	geoHdrTop    = 0.02
	geoHdrWidth  = 0.90
	geoHdrHeight = 0.04

	geoBodyLeft   = 0.05
	geoBodyTop    = 0.08
	geoBodyWidth  = 0.90
	geoBodyHeight = 0.50

	geoFtrLeft   = 0.05
	geoFtrTop    = 0.92
	geoFtrWidth  = 0.90
	geoFtrHeight = 0.04

	// Confidence for expense/ID/lending generated fields.
	confidenceExpenseHigh = 99.0
	confidenceExpenseMed  = 98.0
	confidenceExpenseLI   = 97.0
	confidenceExpenseLI2  = 96.0

	confidenceLending  = 95.0
	confidenceLending2 = 94.0
	confidenceLendSig  = 85.0

	// Geometry for lending signature.
	geoLendSigLeft   = 0.1
	geoLendSigTop    = 0.8
	geoLendSigWidth  = 0.3
	geoLendSigHeight = 0.06
)

// makeGeometry creates a simple bounding-box geometry with a polygon.
func makeGeometry(left, top, width, height float64) *Geometry {
	return &Geometry{
		BoundingBox: &BoundingBox{
			Left:   left,
			Top:    top,
			Width:  width,
			Height: height,
		},
		Polygon: []Point{
			{X: left, Y: top},
			{X: left + width, Y: top},
			{X: left + width, Y: top + height},
			{X: left, Y: top + height},
		},
	}
}

// syntheticBlocks returns a properly structured set of PAGE/LINE/WORD blocks
// with Geometry and Relationships wired per AWS spec.
func syntheticBlocks(documentURI string) []Block {
	pageID := uuid.NewString()
	line1ID := uuid.NewString()
	line2ID := uuid.NewString()
	word1ID := uuid.NewString()
	word2ID := uuid.NewString()
	word3ID := uuid.NewString()
	word4ID := uuid.NewString()
	word5ID := uuid.NewString()

	page1 := 1

	pageBlock := Block{
		BlockType:  "PAGE",
		Text:       "",
		Confidence: confidencePage,
		ID:         pageID,
		Page:       &page1,
		Geometry:   makeGeometry(0, 0, 1, 1),
		Relationships: []Relationship{
			{Type: relTypeChild, Ids: []string{line1ID, line2ID}},
		},
	}

	line1Text := "Synthetic extracted text from " + documentURI
	line1Block := Block{
		BlockType:  "LINE",
		Text:       line1Text,
		Confidence: confidenceLine,
		ID:         line1ID,
		Page:       &page1,
		Geometry:   makeGeometry(geoLeft, geoTop1, geoWidth, geoHeight),
		Relationships: []Relationship{
			{Type: relTypeChild, Ids: []string{word1ID, word2ID, word3ID}},
		},
	}

	line2Block := Block{
		BlockType:  "LINE",
		Text:       "Document processed successfully",
		Confidence: confidenceLine,
		ID:         line2ID,
		Page:       &page1,
		Geometry:   makeGeometry(geoLeft, geoTop2, geoWide, geoHeight),
		Relationships: []Relationship{
			{Type: relTypeChild, Ids: []string{word4ID, word5ID}},
		},
	}

	return []Block{
		pageBlock,
		line1Block,
		line2Block,
		{
			BlockType:  blockTypeWord,
			Text:       "Synthetic",
			TextType:   textTypePrinted,
			Confidence: confidenceWord1,
			ID:         word1ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft1, geoTop1, geoWordWidth, geoHeight),
		},
		{
			BlockType:  blockTypeWord,
			Text:       "extracted",
			TextType:   textTypePrinted,
			Confidence: confidenceWord2,
			ID:         word2ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft2, geoTop1, geoWordWidth, geoHeight),
		},
		{
			BlockType:  blockTypeWord,
			Text:       "text",
			TextType:   textTypePrinted,
			Confidence: confidenceWord3,
			ID:         word3ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft3, geoTop1, geoWordWidth3, geoHeight),
		},
		{
			BlockType:  blockTypeWord,
			Text:       "Document",
			TextType:   textTypePrinted,
			Confidence: confidenceWord1,
			ID:         word4ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft1, geoTop2, geoWordWidth, geoHeight),
		},
		{
			BlockType:  blockTypeWord,
			Text:       "processed",
			TextType:   textTypePrinted,
			Confidence: confidenceWord2,
			ID:         word5ID,
			Page:       &page1,
			Geometry:   makeGeometry(geoWordLeft2, geoTop2, geoWordWidth, geoHeight),
		},
	}
}

// analyzeDocumentBlocks generates blocks for AnalyzeDocument based on feature types
// and queries config.
func analyzeDocumentBlocks(documentURI string, featureTypes []string, queries *QueriesConfig) []Block {
	blocks := syntheticBlocks(documentURI)

	featureSet := make(map[string]bool, len(featureTypes))
	for _, ft := range featureTypes {
		featureSet[ft] = true
	}

	page1 := 1

	if featureSet[featureTypeForms] {
		blocks = append(blocks, buildFormsBlocks(page1)...)
	}

	if featureSet[featureTypeTables] {
		blocks = append(blocks, buildTablesBlocks(page1)...)
	}

	if featureSet[featureTypeQueries] && queries != nil {
		blocks = append(blocks, buildQueryBlocks(queries, page1)...)
	}

	if featureSet[featureTypeSignatures] {
		blocks = append(blocks, buildSignatureBlocks(page1)...)
	}

	if featureSet[featureTypeLayout] {
		blocks = append(blocks, buildLayoutBlocks(page1)...)
	}

	return blocks
}

// buildFormsBlocks builds KEY_VALUE_SET and SELECTION_ELEMENT blocks.
func buildFormsBlocks(page int) []Block {
	selID := uuid.NewString()
	blocks := buildFormKVBlocks(page)
	blocks = append(blocks, Block{
		BlockType:       "SELECTION_ELEMENT",
		ID:              selID,
		Confidence:      confidenceSelElem,
		SelectionStatus: "SELECTED",
		Page:            &page,
		Geometry:        makeGeometry(geoLeft, geoSelTop, geoSelWidth, geoSelHeight),
	})

	return blocks
}

// buildFormKVBlocks builds the KEY_VALUE_SET and WORD blocks for form fields.
func buildFormKVBlocks(page int) []Block {
	keyWord1ID := uuid.NewString()
	keyWord2ID := uuid.NewString()
	valueWord1ID := uuid.NewString()
	valueWord2ID := uuid.NewString()
	key1ID := uuid.NewString()
	val1ID := uuid.NewString()
	key2ID := uuid.NewString()
	val2ID := uuid.NewString()

	return []Block{
		// Key 1
		{
			BlockType:   blockTypeKeyValueSet,
			ID:          key1ID,
			Confidence:  confidenceKV,
			EntityTypes: []string{"KEY"},
			Page:        &page,
			Geometry:    makeGeometry(geoLeft, geoFormTop1, geoFormWidth, geoHeight),
			Relationships: []Relationship{
				{Type: relTypeValue, Ids: []string{val1ID}},
				{Type: relTypeChild, Ids: []string{keyWord1ID}},
			},
		},
		// Value 1
		{
			BlockType:   blockTypeKeyValueSet,
			ID:          val1ID,
			Confidence:  confidenceKV,
			EntityTypes: []string{"VALUE"},
			Page:        &page,
			Geometry:    makeGeometry(geoFormValLeft, geoFormTop1, geoFormWidth, geoHeight),
			Relationships: []Relationship{
				{Type: relTypeChild, Ids: []string{valueWord1ID}},
			},
		},
		// Word for key1
		{
			BlockType:  blockTypeWord,
			ID:         keyWord1ID,
			Text:       "Name:",
			TextType:   textTypePrinted,
			Confidence: confidenceWordForm,
			Page:       &page,
			Geometry:   makeGeometry(geoLeft, geoFormTop1, geoWordWidth, geoHeight),
		},
		// Word for value1
		{
			BlockType:  blockTypeWord,
			ID:         valueWord1ID,
			Text:       "John",
			TextType:   textTypePrinted,
			Confidence: confidenceWordForm,
			Page:       &page,
			Geometry:   makeGeometry(geoFormValLeft, geoFormTop1, geoWordWidth, geoHeight),
		},
		// Key 2
		{
			BlockType:   blockTypeKeyValueSet,
			ID:          key2ID,
			Confidence:  confidenceKV2,
			EntityTypes: []string{"KEY"},
			Page:        &page,
			Geometry:    makeGeometry(geoLeft, geoFormTop2, geoFormWidth, geoHeight),
			Relationships: []Relationship{
				{Type: relTypeValue, Ids: []string{val2ID}},
				{Type: relTypeChild, Ids: []string{keyWord2ID}},
			},
		},
		// Value 2
		{
			BlockType:   blockTypeKeyValueSet,
			ID:          val2ID,
			Confidence:  confidenceKV2,
			EntityTypes: []string{"VALUE"},
			Page:        &page,
			Geometry:    makeGeometry(geoFormValLeft, geoFormTop2, geoFormWidth, geoHeight),
			Relationships: []Relationship{
				{Type: relTypeChild, Ids: []string{valueWord2ID}},
			},
		},
		// Word for key2
		{
			BlockType:  blockTypeWord,
			ID:         keyWord2ID,
			Text:       "Status:",
			TextType:   textTypePrinted,
			Confidence: confidenceWordForm,
			Page:       &page,
			Geometry:   makeGeometry(geoLeft, geoFormTop2, geoWordWidth, geoHeight),
		},
		// Word for value2
		{
			BlockType:  blockTypeWord,
			ID:         valueWord2ID,
			Text:       "Active",
			TextType:   textTypePrinted,
			Confidence: confidenceWordForm,
			Page:       &page,
			Geometry:   makeGeometry(geoFormValLeft, geoFormTop2, geoWordWidth, geoHeight),
		},
	}
}

// buildTablesBlocks builds TABLE and CELL blocks.
func buildTablesBlocks(page int) []Block {
	tableID := uuid.NewString()
	cell11 := uuid.NewString()
	cell12 := uuid.NewString()
	cell21 := uuid.NewString()
	cell22 := uuid.NewString()

	row1 := 1
	row2 := 2
	col1 := 1
	col2 := 2
	span1 := 1

	return []Block{
		{
			BlockType:  "TABLE",
			ID:         tableID,
			Confidence: confidenceTable,
			Page:       &page,
			Geometry:   makeGeometry(geoTableLeft, geoTableTop, geoTableWidth, geoTableHeight),
			Relationships: []Relationship{
				{Type: relTypeChild, Ids: []string{cell11, cell12, cell21, cell22}},
			},
		},
		{
			BlockType:   blockTypeCell,
			ID:          cell11,
			Text:        "Header 1",
			Confidence:  confidenceTable,
			Page:        &page,
			Geometry:    makeGeometry(geoCell1Left, geoCellTop1, geoCell1Width, geoCellHeight),
			RowIndex:    &row1,
			ColumnIndex: &col1,
			RowSpan:     &span1,
			ColumnSpan:  &span1,
			EntityTypes: []string{"COLUMN_HEADER"},
		},
		{
			BlockType:   blockTypeCell,
			ID:          cell12,
			Text:        "Header 2",
			Confidence:  confidenceTable,
			Page:        &page,
			Geometry:    makeGeometry(geoCell2Left, geoCellTop1, geoCell1Width, geoCellHeight),
			RowIndex:    &row1,
			ColumnIndex: &col2,
			RowSpan:     &span1,
			ColumnSpan:  &span1,
			EntityTypes: []string{"COLUMN_HEADER"},
		},
		{
			BlockType:   blockTypeCell,
			ID:          cell21,
			Text:        "Value 1",
			Confidence:  confidenceTableCell,
			Page:        &page,
			Geometry:    makeGeometry(geoCell1Left, geoCellTop2, geoCell1Width, geoCellHeight),
			RowIndex:    &row2,
			ColumnIndex: &col1,
			RowSpan:     &span1,
			ColumnSpan:  &span1,
		},
		{
			BlockType:   blockTypeCell,
			ID:          cell22,
			Text:        "Value 2",
			Confidence:  confidenceTableCell,
			Page:        &page,
			Geometry:    makeGeometry(geoCell2Left, geoCellTop2, geoCell1Width, geoCellHeight),
			RowIndex:    &row2,
			ColumnIndex: &col2,
			RowSpan:     &span1,
			ColumnSpan:  &span1,
		},
	}
}

// buildQueryBlocks builds QUERY and QUERY_RESULT blocks for each query.
func buildQueryBlocks(queries *QueriesConfig, page int) []Block {
	const blocksPerQuery = 2 // one QUERY + one QUERY_RESULT per query entry
	blocks := make([]Block, 0, len(queries.Queries)*blocksPerQuery)

	for _, q := range queries.Queries {
		queryID := uuid.NewString()
		resultID := uuid.NewString()

		pages := q.Pages
		if len(pages) == 0 {
			pages = []string{"1"}
		}

		blocks = append(
			blocks,
			Block{
				BlockType:  "QUERY",
				ID:         queryID,
				Confidence: confidenceQuery,
				Page:       &page,
				Query: &QueryBlock{
					Text:  q.Text,
					Alias: q.Alias,
					Pages: pages,
				},
				Relationships: []Relationship{
					{Type: relTypeAnswer, Ids: []string{resultID}},
				},
			},
			Block{
				BlockType:  "QUERY_RESULT",
				ID:         resultID,
				Text:       "(no answer)",
				Confidence: confidenceQueryResult,
				Page:       &page,
			},
		)
	}

	return blocks
}

// buildSignatureBlocks builds SIGNATURE blocks.
func buildSignatureBlocks(page int) []Block {
	return []Block{
		{
			BlockType:  "SIGNATURE",
			ID:         uuid.NewString(),
			Confidence: confidenceSignature,
			Page:       &page,
			Geometry:   makeGeometry(geoSigLeft, geoSigTop, geoSigWidth, geoSigHeight),
		},
	}
}

// buildLayoutBlocks builds LAYOUT_* blocks.
func buildLayoutBlocks(page int) []Block {
	return []Block{
		{
			BlockType:  "LAYOUT_HEADER",
			ID:         uuid.NewString(),
			Text:       "Document Header",
			Confidence: confidenceLayout,
			Page:       &page,
			Geometry:   makeGeometry(geoHdrLeft, geoHdrTop, geoHdrWidth, geoHdrHeight),
		},
		{
			BlockType:  "LAYOUT_TEXT",
			ID:         uuid.NewString(),
			Text:       "Body text content",
			Confidence: confidenceLayout,
			Page:       &page,
			Geometry:   makeGeometry(geoBodyLeft, geoBodyTop, geoBodyWidth, geoBodyHeight),
		},
		{
			BlockType:  "LAYOUT_FOOTER",
			ID:         uuid.NewString(),
			Text:       "Page 1",
			Confidence: confidenceLayout,
			Page:       &page,
			Geometry:   makeGeometry(geoFtrLeft, geoFtrTop, geoFtrWidth, geoFtrHeight),
		},
	}
}

// paginateBlocks applies MaxResults and NextToken pagination to a blocks slice.
// Returns the page of blocks and a new NextToken (empty string means no more pages).
func paginateBlocks(blocks []Block, maxResults int, nextToken string) ([]Block, string) {
	offset := 0

	if nextToken != "" {
		decoded, err := base64.StdEncoding.DecodeString(nextToken)
		if err == nil {
			if n, err2 := strconv.Atoi(string(decoded)); err2 == nil && n >= 0 && n < len(blocks) {
				offset = n
			}
		}
	}

	if offset >= len(blocks) {
		return []Block{}, ""
	}

	end := len(blocks)
	if maxResults > 0 && offset+maxResults < end {
		end = offset + maxResults
	}

	page := make([]Block, end-offset)
	copy(page, blocks[offset:end])

	newToken := ""
	if end < len(blocks) {
		newToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}

	return page, newToken
}

// PaginateBlocks applies pagination to blocks (exported for handler use).
func PaginateBlocks(blocks []Block, maxResults int, nextToken string) ([]Block, string) {
	return paginateBlocks(blocks, maxResults, nextToken)
}
