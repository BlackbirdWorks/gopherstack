package cosmosdb

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Spatial index types (azcosmos SpatialType, azcosmos v1.5.0 spatial_type.go; Learn "Indexing policies").
func spatialIndexTypes() []string { return []string{"Point", "Polygon", "MultiPolygon", "LineString"} }

// spatialIndex is one spatialIndexes entry. BoundingBox is required for a Geometry container (the .NET SpatialPath
// example carries it) and meaningless for Geography.
type spatialIndex struct {
	BoundingBox *boundingBox `json:"boundingBox"`
	Path        string       `json:"path"`
	Types       []string     `json:"types"`
}

type boundingBox struct {
	XMin *float64 `json:"xmin"`
	YMin *float64 `json:"ymin"`
	XMax *float64 `json:"xmax"`
	YMax *float64 `json:"ymax"`
}

func (b boundingBox) valid() bool {
	return b.XMin != nil && b.YMin != nil && b.XMax != nil && b.YMax != nil && *b.XMin < *b.XMax && *b.YMin < *b.YMax
}

// geospatialType is the container's geospatial data type; Geography unless the config says Geometry.
func geospatialType(raw json.RawMessage) string {
	var cfg struct {
		Type string `json:"type"`
	}

	if json.Unmarshal(raw, &cfg) == nil && cfg.Type == geoTypeGeometry {
		return geoTypeGeometry
	}

	return geoTypeGeography
}

func spatialPathOK(p string) bool {
	return strings.HasPrefix(p, "/") && (strings.HasSuffix(p, "/?") || strings.HasSuffix(p, "/*"))
}

// validateSpatialIndexes checks the policy's spatial indexes: a path ending in /? or /*, one or more known types,
// and a well-formed bounding box on every index of a Geometry container.
func validateSpatialIndexes(spatial []spatialIndex, geoType string) error {
	for _, idx := range spatial {
		if !spatialPathOK(idx.Path) {
			return fmt.Errorf("%w: spatial index path %q must start with '/' and end with '/?' or '/*'",
				ErrInvalidContainerPolicy, idx.Path)
		}

		if len(idx.Types) == 0 {
			return fmt.Errorf("%w: spatial index %q needs at least one type", ErrInvalidContainerPolicy, idx.Path)
		}

		for _, t := range idx.Types {
			if !slices.Contains(spatialIndexTypes(), t) {
				return fmt.Errorf("%w: spatial index %q has the unknown type %q (use %s)",
					ErrInvalidContainerPolicy, idx.Path, t, strings.Join(spatialIndexTypes(), ", "))
			}
		}

		if geoType == geoTypeGeometry && (idx.BoundingBox == nil || !idx.BoundingBox.valid()) {
			return fmt.Errorf("%w: spatial index %q on a Geometry container needs a boundingBox with "+
				"xmin < xmax and ymin < ymax", ErrInvalidContainerPolicy, idx.Path)
		}
	}

	return nil
}
