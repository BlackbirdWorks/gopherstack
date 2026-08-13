package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

// importDataSourceInput mirrors types.ImportDataSource. Both members are
// required on the real type; gopherstack has no S3 fetcher, so DataFormat/
// S3Url are validated and stored for echo-back but the file they name is
// never actually read.
type importDataSourceInput struct {
	DataFormat string `json:"DataFormat"`
	S3Url      string `json:"S3Url"`
}

// contactListDestinationInput mirrors types.ContactListDestination.
type contactListDestinationInput struct {
	ContactListImportAction string `json:"ContactListImportAction"`
	ContactListName         string `json:"ContactListName"`
}

// suppressionListDestinationInput mirrors types.SuppressionListDestination.
type suppressionListDestinationInput struct {
	SuppressionListImportAction string `json:"SuppressionListImportAction"`
}

// importDestinationInput mirrors types.ImportDestination: exactly one of the
// two branches must be set (real SES v2 requires "either
// ContactListDestination or SuppressionListDestination").
type importDestinationInput struct {
	ContactListDestination     *contactListDestinationInput     `json:"ContactListDestination,omitempty"`
	SuppressionListDestination *suppressionListDestinationInput `json:"SuppressionListDestination,omitempty"`
}

// toImportDestination validates the oneof and its branch's own required
// members, then converts to the backend shape.
func (d importDestinationInput) toImportDestination() (ImportDestination, error) {
	hasContactList := d.ContactListDestination != nil
	hasSuppressionList := d.SuppressionListDestination != nil

	switch {
	case hasContactList == hasSuppressionList:
		return ImportDestination{}, fmt.Errorf(
			"%w: ImportDestination must set exactly one of ContactListDestination or SuppressionListDestination",
			ErrInvalidInput,
		)
	case hasContactList:
		if d.ContactListDestination.ContactListImportAction == "" {
			return ImportDestination{}, fmt.Errorf(
				"%w: ContactListDestination.ContactListImportAction is required", ErrInvalidInput,
			)
		}

		if d.ContactListDestination.ContactListName == "" {
			return ImportDestination{}, fmt.Errorf(
				"%w: ContactListDestination.ContactListName is required", ErrInvalidInput,
			)
		}

		return ImportDestination{
			ContactListName:         d.ContactListDestination.ContactListName,
			ContactListImportAction: d.ContactListDestination.ContactListImportAction,
		}, nil
	default:
		if d.SuppressionListDestination.SuppressionListImportAction == "" {
			return ImportDestination{}, fmt.Errorf(
				"%w: SuppressionListDestination.SuppressionListImportAction is required", ErrInvalidInput,
			)
		}

		return ImportDestination{
			SuppressionListImportAction: d.SuppressionListDestination.SuppressionListImportAction,
		}, nil
	}
}

type createImportJobInput struct {
	ImportDataSource  *importDataSourceInput  `json:"ImportDataSource"`
	ImportDestination *importDestinationInput `json:"ImportDestination"`
}

func (h *Handler) handleCreateImportJob(c *echo.Context) (any, error) {
	var in createImportJobInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if in.ImportDataSource == nil {
		return nil, fmt.Errorf("%w: ImportDataSource is required", ErrInvalidInput)
	}

	if in.ImportDataSource.DataFormat == "" {
		return nil, fmt.Errorf("%w: ImportDataSource.DataFormat is required", ErrInvalidInput)
	}

	if in.ImportDataSource.S3Url == "" {
		return nil, fmt.Errorf("%w: ImportDataSource.S3Url is required", ErrInvalidInput)
	}

	if in.ImportDestination == nil {
		return nil, fmt.Errorf("%w: ImportDestination is required", ErrInvalidInput)
	}

	dest, err := in.ImportDestination.toImportDestination()
	if err != nil {
		return nil, err
	}

	job, err := h.Backend.CreateImportJob(dest)
	if err != nil {
		return nil, err
	}

	return map[string]any{"JobId": job.JobID}, nil
}

func (h *Handler) handleGetImportJob(jobID string) (any, error) {
	job, err := h.Backend.GetImportJob(jobID)
	if err != nil {
		return nil, err
	}

	return toImportJobOutput(job), nil
}

// listImportJobsInput mirrors ListImportJobsInput -- real SES v2 serves
// ListImportJobs as POST /v2/email/import-jobs/list with filter/pagination
// in the JSON body, not query params (ImportDestinationType filter isn't
// modelled by the backend yet, so only pagination is honored).
type listImportJobsInput struct {
	NextToken string `json:"NextToken"`
	PageSize  int32  `json:"PageSize"`
}

func (h *Handler) handleListImportJobs(c *echo.Context) (any, error) {
	var in listImportJobsInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	pg := h.Backend.ListImportJobs(in.NextToken, int(in.PageSize))

	items := make([]*importJobOutput, 0, len(pg.Data))
	for _, j := range pg.Data {
		items = append(items, toImportJobOutput(j))
	}

	return map[string]any{
		"ImportJobs": items,
		keyNextToken: pg.Next,
	}, nil
}
