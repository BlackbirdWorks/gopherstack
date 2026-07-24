package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreateDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		Name        string `json:"Name"`
		ShortName   string `json:"ShortName"`
		Description string `json:"Description"`
		Password    string `json:"Password"`
		Size        string `json:"Size"`
		VpcSettings *struct {
			VpcID     string   `json:"VpcId"`
			SubnetIDs []string `json:"SubnetIds"`
		} `json:"VpcSettings"`
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "Name is required"))
	}
	if req.Password == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "Password is required"))
	}
	if req.Size != string(DirectorySizeSmall) && req.Size != string(DirectorySizeLarge) {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "Size must be Small or Large"))
	}

	tags := reqTagsToTags(req.Tags)

	var vpcSettings *DirectoryVpcSettings
	if req.VpcSettings != nil {
		vpcSettings = &DirectoryVpcSettings{
			VpcID:     req.VpcSettings.VpcID,
			SubnetIDs: req.VpcSettings.SubnetIDs,
		}
	}

	d, createErr := h.Backend.CreateDirectory(
		h.contextWithRegion(c),
		req.Name,
		req.ShortName,
		req.Description,
		req.Password,
		DirectorySize(req.Size),
		vpcSettings,
		tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: d.DirectoryID,
	})
}

func (h *Handler) handleCreateMicrosoftAD(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		Name        string `json:"Name"`
		ShortName   string `json:"ShortName"`
		Description string `json:"Description"`
		Password    string `json:"Password"`
		Edition     string `json:"Edition"`
		VpcSettings *struct {
			VpcID     string   `json:"VpcId"`
			SubnetIDs []string `json:"SubnetIds"`
		} `json:"VpcSettings"`
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "Name is required"))
	}
	if req.Password == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "Password is required"))
	}

	edition := DirectoryEdition(req.Edition)
	if edition == "" {
		edition = DirectoryEditionEnterprise
	}
	if edition != DirectoryEditionEnterprise && edition != DirectoryEditionStandard {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "Edition must be Enterprise or Standard"),
		)
	}

	tags := reqTagsToTags(req.Tags)

	var vpcSettings *DirectoryVpcSettings
	if req.VpcSettings != nil {
		vpcSettings = &DirectoryVpcSettings{
			VpcID:     req.VpcSettings.VpcID,
			SubnetIDs: req.VpcSettings.SubnetIDs,
		}
	}

	d, createErr := h.Backend.CreateMicrosoftAD(
		h.contextWithRegion(c),
		req.Name,
		req.ShortName,
		req.Description,
		req.Password,
		edition,
		vpcSettings,
		tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: d.DirectoryID,
	})
}

func (h *Handler) handleDeleteDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId is required"))
	}

	if delErr := h.Backend.DeleteDirectory(h.contextWithRegion(c), req.DirectoryID); delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: req.DirectoryID,
	})
}

func (h *Handler) handleDescribeDirectories(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		NextToken    string   `json:"NextToken"`
		DirectoryIDs []string `json:"DirectoryIds"`
		Limit        int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	dirs, nextToken, listErr := h.Backend.DescribeDirectories(
		h.contextWithRegion(c),
		req.DirectoryIDs,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	dirList := make([]map[string]any, 0, len(dirs))
	for _, d := range dirs {
		dirList = append(dirList, directoryToJSON(d))
	}

	resp := map[string]any{
		"DirectoryDescriptions": dirList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleCreateAlias(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Alias       string `json:"Alias"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.Alias == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "DirectoryId and Alias are required"))
	}

	if aliasErr := h.Backend.CreateAlias(h.contextWithRegion(c), req.DirectoryID, req.Alias); aliasErr != nil {
		return h.mapError(c, aliasErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDirectoryID: req.DirectoryID,
		"Alias":        req.Alias,
	})
}

func (h *Handler) handleGetDirectoryLimits(c *echo.Context) error {
	limits := h.Backend.GetDirectoryLimits(h.contextWithRegion(c))

	return c.JSON(http.StatusOK, map[string]any{
		"DirectoryLimits": map[string]any{
			"CloudOnlyDirectoriesCurrentCount": limits.CloudOnlyDirectoriesCurrentCount,
			"CloudOnlyDirectoriesLimit":        limits.CloudOnlyDirectoriesLimit,
			"CloudOnlyDirectoriesLimitReached": limits.CloudOnlyDirectoriesLimitReached,
			"CloudOnlyMicrosoftADCurrentCount": limits.CloudOnlyMicrosoftADCurrentCount,
			"CloudOnlyMicrosoftADLimit":        limits.CloudOnlyMicrosoftADLimit,
			"CloudOnlyMicrosoftADLimitReached": limits.CloudOnlyMicrosoftADLimitReached,
			"ConnectedDirectoriesCurrentCount": limits.ConnectedDirectoriesCurrentCount,
			"ConnectedDirectoriesLimit":        limits.ConnectedDirectoriesLimit,
			"ConnectedDirectoriesLimitReached": limits.ConnectedDirectoriesLimitReached,
		},
	})
}

func (h *Handler) handleCreateComputer(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID  string `json:"DirectoryId"`
		ComputerName string `json:"ComputerName"`
		Password     string `json:"Password"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.ComputerName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "DirectoryId and ComputerName are required"),
		)
	}

	computer, createErr := h.Backend.CreateComputer(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.ComputerName,
		req.Password,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Computer": map[string]any{
			"ComputerId":   computer.ComputerID,
			"ComputerName": computer.ComputerName,
		},
	})
}

func (h *Handler) handleResetUserPassword(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		UserName    string `json:"UserName"`
		NewPassword string `json:"NewPassword"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" || req.UserName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "DirectoryId and UserName are required"),
		)
	}

	resetErr := h.Backend.ResetUserPassword(h.contextWithRegion(c), req.DirectoryID, req.UserName, req.NewPassword)
	if resetErr != nil {
		return h.mapError(c, resetErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleConnectDirectory(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		Name        string `json:"Name"`
		ShortName   string `json:"ShortName"`
		Description string `json:"Description"`
		Password    string `json:"Password"`
		Size        string `json:"Size"`
		Tags        []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "Name is required"))
	}
	if req.Password == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "Password is required"))
	}
	if req.Size != string(DirectorySizeSmall) && req.Size != string(DirectorySizeLarge) {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "Size must be Small or Large"))
	}

	tags := reqTagsToTags(req.Tags)
	d, createErr := h.Backend.ConnectDirectory(
		h.contextWithRegion(c),
		req.Name,
		req.ShortName,
		req.Description,
		req.Password,
		DirectorySize(req.Size),
		tags,
	)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDirectoryID: d.DirectoryID})
}
