package iot

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func resolveThingTypeOps(path, method string) string {
	switch {
	case path == "/thing-types" && method == http.MethodGet:

		return opListThingTypes
	case strings.HasPrefix(path, "/thing-types/") && strings.HasSuffix(path, "/deprecate") && method == http.MethodPost:

		return opDeprecateThingType
	case strings.HasPrefix(path, "/thing-types/") && method == http.MethodPost:

		return opCreateThingType
	case strings.HasPrefix(path, "/thing-types/") && method == http.MethodGet:

		return opDescribeThingType
	case strings.HasPrefix(path, "/thing-types/") && method == http.MethodDelete:

		return opDeleteThingType
	case strings.HasPrefix(path, "/thing-types/") && method == http.MethodPatch:

		return opUpdateThingType
	}

	return unknownOperation
}

func (h *Handler) dispatchThingTypeOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateThingType:

		return true, h.handleCreateThingType(c)
	case opDescribeThingType:

		return true, h.handleDescribeThingType(c)
	case opListThingTypes:

		return true, h.handleListThingTypes(c)
	case opDeprecateThingType:

		return true, h.handleDeprecateThingType(c)
	case opDeleteThingType:

		return true, h.handleDeleteThingType(c)
	}

	return false, nil
}

func (h *Handler) handleCreateThingType(c *echo.Context) error {
	thingTypeName := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")

	var body struct {
		ThingTypeProperties *struct {
			ThingTypeDescription string   `json:"thingTypeDescription"`
			SearchableAttributes []string `json:"searchableAttributes"`
		} `json:"thingTypeProperties"`
		// []types.Tag on the wire, not a map (serializers.go:4974, aws-sdk-go-v2/service/iot@v1.77.4).
		Tags []tags.KV `json:"tags,omitempty"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	desc := ""

	var searchable []string

	if body.ThingTypeProperties != nil {
		desc = body.ThingTypeProperties.ThingTypeDescription
		searchable = body.ThingTypeProperties.SearchableAttributes
	}

	tt, err := h.Backend.CreateThingType(&CreateThingTypeInput{
		ThingTypeName:        thingTypeName,
		Description:          desc,
		SearchableAttributes: searchable,
		Tags:                 tags.MapFromKV(body.Tags),
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyThingTypeName: tt.ThingTypeName,
		keyThingTypeArn:  tt.ThingTypeARN,
		"thingTypeId":    tt.ThingTypeID,
	})
}

func (h *Handler) handleDescribeThingType(c *echo.Context) error {
	thingTypeName := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")

	tt, err := h.Backend.DescribeThingType(thingTypeName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyThingTypeName:    tt.ThingTypeName,
		keyThingTypeArn:     tt.ThingTypeARN,
		"thingTypeId":       tt.ThingTypeID,
		"thingTypeMetadata": thingTypeMetadataFields(tt),
		"thingTypeProperties": map[string]any{
			"thingTypeDescription": tt.Description,
			"searchableAttributes": tt.SearchableAttributes,
		},
	})
}

// thingTypeMetadataFields builds the ThingTypeMetadata wire object
// (deprecated/creationDate/deprecationDate), converting timestamps to epoch
// seconds as the restjson1 DateType wire format requires (json.Marshal'ing a
// raw time.Time renders an RFC3339 string instead, which the real SDK
// deserializer rejects).
func thingTypeMetadataFields(tt *ThingType) map[string]any {
	var deprecationDate any
	if tt.Deprecated && !tt.DeprecationDate.IsZero() {
		deprecationDate = awstime.Epoch(tt.DeprecationDate)
	}

	return map[string]any{
		"deprecated":      tt.Deprecated,
		keyCreationDate:   awstime.Epoch(tt.CreatedAt),
		"deprecationDate": deprecationDate,
	}
}

func (h *Handler) handleListThingTypes(c *echo.Context) error {
	types := h.Backend.ListThingTypes()
	out := make([]map[string]any, 0, len(types))

	for _, tt := range types {
		out = append(out, map[string]any{
			keyThingTypeName:    tt.ThingTypeName,
			keyThingTypeArn:     tt.ThingTypeARN,
			"thingTypeMetadata": thingTypeMetadataFields(tt),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"thingTypes": out})
}

func (h *Handler) handleDeprecateThingType(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")
	thingTypeName := strings.TrimSuffix(after, "/deprecate")

	var body struct {
		UndoDeprecate bool `json:"undoDeprecate"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	if err := h.Backend.DeprecateThingType(&DeprecateThingTypeInput{
		ThingTypeName: thingTypeName,
		UndoDeprecate: body.UndoDeprecate,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteThingType(c *echo.Context) error {
	thingTypeName := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")
	if err := h.Backend.DeleteThingType(thingTypeName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// resolveThingTypeUpdateOp resolves the PATCH /thing-types/{name} route.
func resolveThingTypeUpdateOp(path, method string) string {
	if strings.HasPrefix(path, "/thing-types/") && method == http.MethodPatch {
		return opUpdateThingType
	}

	return unknownOperation
}

func (h *Handler) handleUpdateThingType(c *echo.Context) error {
	thingTypeName := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")

	var body struct {
		ThingTypeProperties *struct {
			ThingTypeDescription string   `json:"thingTypeDescription"`
			SearchableAttributes []string `json:"searchableAttributes"`
		} `json:"thingTypeProperties"`
	}
	if err := readBody(c, &body); err != nil {
		return err
	}

	var desc string

	var searchable []string

	if body.ThingTypeProperties != nil {
		desc = body.ThingTypeProperties.ThingTypeDescription
		searchable = body.ThingTypeProperties.SearchableAttributes
	}

	if err := h.Backend.UpdateThingType(&UpdateThingTypeInput{
		ThingTypeName:        thingTypeName,
		Description:          desc,
		SearchableAttributes: searchable,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
