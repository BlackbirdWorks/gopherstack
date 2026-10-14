package route53recoveryreadiness

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// CellView is the wire form of a cell.
type CellView struct {
	Tags                  map[string]string `json:"tags,omitempty"`
	CellArn               string            `json:"cellArn"`
	CellName              string            `json:"cellName"`
	Cells                 []string          `json:"cells"`
	ParentReadinessScopes []string          `json:"parentReadinessScopes"`
}

func cellView(c *Cell) CellView {
	parents := []string{}
	if c.Parent != "" {
		parents = append(parents, c.Parent)
	}

	return CellView{
		CellArn:               c.ARN,
		CellName:              c.Name,
		Cells:                 slices.Clone(c.Cells),
		ParentReadinessScopes: parents,
		Tags:                  cloneTags(c.Tags),
	}
}

// attachChildren validates that children can sit under parentArn; children already owned by it are allowed.
func (b *InMemoryBackend) attachChildren(parentArn, parentCell string, children []string) error {
	seen := map[string]bool{}

	for _, ca := range children {
		child, ok := b.cellByARN(ca)
		if !ok {
			return restroute.NotFound("Cell %s was not found", ca)
		}

		if seen[ca] {
			return restroute.Validation("Cell %s is listed more than once", ca)
		}

		seen[ca] = true

		if parentCell != "" && b.isAncestorOrSelf(child, parentCell) {
			return restroute.Validation("Cell %s cannot contain %s: that would create a cycle", parentCell, ca)
		}

		if child.Parent != "" && child.Parent != parentArn {
			return restroute.Conflict("Cell %s already belongs to %s", ca, child.Parent)
		}
	}

	return nil
}

// isAncestorOrSelf reports whether candidate is cellName itself or one of its ancestors.
func (b *InMemoryBackend) isAncestorOrSelf(candidate *Cell, cellName string) bool {
	if candidate.Name == cellName {
		return true
	}

	cur, err := b.getCell(cellName)
	for err == nil && cur.Parent != "" {
		if cur.Parent == candidate.ARN {
			return true
		}

		next, ok := b.cellByARN(cur.Parent)
		if !ok {
			return false
		}

		cur = next
	}

	return false
}

func (b *InMemoryBackend) setParent(children []string, parentArn string) {
	for _, ca := range children {
		if c, ok := b.cellByARN(ca); ok {
			c.Parent = parentArn
		}
	}
}

// adopt links children under arn and returns the stored child list (never nil).
func (b *InMemoryBackend) adopt(arn string, children []string) ([]string, error) {
	if err := b.attachChildren(arn, "", children); err != nil {
		return nil, err
	}

	b.setParent(children, arn)

	return append([]string{}, children...), nil
}

// CreateCell creates a cell.
func (b *InMemoryBackend) CreateCell(name string, children []string, tags map[string]string) (CellView, error) {
	if err := validName("cell", name); err != nil {
		return CellView{}, err
	}

	b.mu.Lock("CreateCell")
	defer b.mu.Unlock()

	if b.cells.Has(name) {
		return CellView{}, restroute.Conflict("Cell %s already exists", name)
	}

	arn := b.arnFor("cell", name)

	kids, err := b.adopt(arn, children)
	if err != nil {
		return CellView{}, err
	}

	cell := &Cell{ARN: arn, Name: name, Cells: kids, Tags: cloneTags(tags)}
	b.cells.Put(cell)

	return cellView(cell), nil
}

// GetCell returns a cell.
func (b *InMemoryBackend) GetCell(name string) (CellView, error) {
	b.mu.RLock("GetCell")
	defer b.mu.RUnlock()

	c, err := b.getCell(name)
	if err != nil {
		return CellView{}, err
	}

	return cellView(c), nil
}

// ListCells lists cells.
func (b *InMemoryBackend) ListCells(token string, maxResults int) ([]CellView, string, error) {
	b.mu.RLock("ListCells")
	defer b.mu.RUnlock()

	pg, err := restroute.Paginate(sorted(b.cells.All(), func(c *Cell) string { return c.Name }),
		token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	out := make([]CellView, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, cellView(c))
	}

	return out, pg.Next, nil
}

// UpdateCell replaces a cell's child list.
func (b *InMemoryBackend) UpdateCell(name string, children []string) (CellView, error) {
	b.mu.Lock("UpdateCell")
	defer b.mu.Unlock()

	c, err := b.getCell(name)
	if err != nil {
		return CellView{}, err
	}

	if err = b.attachChildren(c.ARN, c.Name, children); err != nil {
		return CellView{}, err
	}

	b.setParent(c.Cells, "")
	b.setParent(children, c.ARN)
	c.Cells = slices.Clone(children)

	if c.Cells == nil {
		c.Cells = []string{}
	}

	return cellView(c), nil
}

// DeleteCell deletes a cell that is not part of a recovery group or another cell.
func (b *InMemoryBackend) DeleteCell(name string) error {
	b.mu.Lock("DeleteCell")
	defer b.mu.Unlock()

	c, err := b.getCell(name)
	if err != nil {
		return err
	}

	if c.Parent != "" {
		return restroute.Conflict("Cell %s still belongs to %s; remove it from there first", name, c.Parent)
	}

	b.setParent(c.Cells, "")
	b.cells.Delete(name)

	return nil
}
