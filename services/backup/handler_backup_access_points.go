package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createBackupAccessPointBody struct {
	AccessPointMetadata map[string]string `json:"AccessPointMetadata"`
	Tags                map[string]string `json:"Tags"`
	Name                string            `json:"Name"`
	RecoveryPointArn    string            `json:"RecoveryPointArn"`
	AccessPointPolicy   string            `json:"AccessPointPolicy"`
}

func (h *Handler) handleCreateBackupAccessPoint(c *echo.Context, body []byte) error {
	var in createBackupAccessPointBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}
	}

	bap, err := h.Backend.CreateBackupAccessPoint(
		in.Name, in.RecoveryPointArn, in.AccessPointPolicy, in.AccessPointMetadata, in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAccessPointArn: bap.AccessPointArn,
		keyStatus:         bap.Status,
	})
}

// describeBackupAccessPointBody renders DescribeBackupAccessPointOutput.
// AccessPointMetadata is optional here (unlike the List item shape below --
// backup@v1.64.0 types.go: DescribeBackupAccessPointOutput does not mark it
// required), so it is only emitted when non-empty.
func describeBackupAccessPointBody(bap *AccessPoint) map[string]any {
	resp := map[string]any{
		keyAccessPointArn:   bap.AccessPointArn,
		keyBackupVaultName:  bap.BackupVaultName,
		keyCreationTime:     epochSeconds(bap.CreationTime),
		"Name":              bap.Name,
		keyRecoveryPointArn: bap.RecoveryPointArn,
		keyResourceArn:      bap.ResourceArn,
		keyResourceType:     bap.ResourceType,
		keyStatus:           bap.Status,
	}
	setOptionalStr(resp, keyBackupVaultArn, bap.BackupVaultArn)
	setOptionalStr(resp, "StatusMessage", bap.StatusMessage)

	if len(bap.AccessPointMetadata) > 0 {
		resp["AccessPointMetadata"] = bap.AccessPointMetadata
	}

	return resp
}

func (h *Handler) handleDescribeBackupAccessPoint(c *echo.Context, accessPointArn string) error {
	bap, err := h.Backend.DescribeBackupAccessPoint(accessPointArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeBackupAccessPointBody(bap))
}

func (h *Handler) handleDeleteBackupAccessPoint(c *echo.Context, accessPointArn string) error {
	if err := h.Backend.DeleteBackupAccessPoint(accessPointArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// backupAccessPointListItem renders one types.ListAccessPointsMember.
// AccessPointMetadata IS required here (unlike Describe above), so it is
// always emitted, as {} when empty.
func backupAccessPointListItem(bap *AccessPoint) map[string]any {
	metadata := bap.AccessPointMetadata
	if metadata == nil {
		metadata = map[string]string{}
	}

	item := map[string]any{
		keyAccessPointArn:     bap.AccessPointArn,
		"AccessPointMetadata": metadata,
		keyBackupVaultName:    bap.BackupVaultName,
		keyCreationTime:       epochSeconds(bap.CreationTime),
		"Name":                bap.Name,
		keyRecoveryPointArn:   bap.RecoveryPointArn,
		keyResourceArn:        bap.ResourceArn,
		keyResourceType:       bap.ResourceType,
		keyStatus:             bap.Status,
	}
	setOptionalStr(item, keyBackupVaultArn, bap.BackupVaultArn)
	setOptionalStr(item, "StatusMessage", bap.StatusMessage)

	return item
}

func backupAccessPointsPage(list []*AccessPoint, nextToken string) map[string]any {
	items := make([]map[string]any, 0, len(list))
	for _, bap := range list {
		items = append(items, backupAccessPointListItem(bap))
	}

	resp := map[string]any{"BackupAccessPoints": items}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return resp
}

func (h *Handler) handleListBackupAccessPoints(c *echo.Context) error {
	q := c.Request().URL.Query()
	list, nextToken := h.Backend.ListBackupAccessPoints(
		parseInt(q.Get("MaxResults")), q.Get("NextToken"),
	)

	return c.JSON(http.StatusOK, backupAccessPointsPage(list, nextToken))
}

func (h *Handler) handleListBackupAccessPointsByRecoveryPoint(
	c *echo.Context, recoveryPointArn string,
) error {
	q := c.Request().URL.Query()
	list, nextToken := h.Backend.ListBackupAccessPointsByRecoveryPoint(
		recoveryPointArn, parseInt(q.Get("MaxResults")), q.Get("NextToken"),
	)

	return c.JSON(http.StatusOK, backupAccessPointsPage(list, nextToken))
}

func (h *Handler) handleListBackupAccessPointsByResource(
	c *echo.Context, resourceArn string,
) error {
	q := c.Request().URL.Query()
	list, nextToken := h.Backend.ListBackupAccessPointsByResource(
		resourceArn, parseInt(q.Get("MaxResults")), q.Get("NextToken"),
	)

	return c.JSON(http.StatusOK, backupAccessPointsPage(list, nextToken))
}
