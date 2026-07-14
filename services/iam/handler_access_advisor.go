package iam

import (
	"net/url"
	"time"
)

// iamAccessAdvisorDispatch wires real GenerateServiceLastAccessedDetails and GetServiceLastAccessedDetails.
func (h *Handler) iamAccessAdvisorDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		opGenerateServiceLastAccessed: func(vals url.Values, reqID string) (any, error) {
			entityARN := vals.Get("Arn")
			jobID := h.Backend.GenerateServiceLastAccessedDetailsForEntity(entityARN)

			return &generateServiceLastAccessedDetailsResponse{
				XMLName: xmlLocalName("GenerateServiceLastAccessedDetailsResponse"),
				Xmlns:   iamXMLNS,
				GenerateServiceLastAccessedDetailsResult: generateSLADResult{
					JobID: jobID,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opGetServiceLastAccessed: func(vals url.Values, reqID string) (any, error) {
			jobID := vals.Get("JobId")

			status, details, err := h.Backend.GetServiceLastAccessedDetails(jobID)
			if err != nil {
				return nil, err
			}

			now := isoTime(time.Now().UTC())
			xmlDetails := make([]ServiceLastAccessedDetailXML, 0, len(details))

			for _, d := range details {
				entry := ServiceLastAccessedDetailXML{
					ServiceName:                d.ServiceName,
					ServiceNamespace:           d.ServiceNamespace,
					TotalAuthenticatedEntities: d.TotalAuthenticatedEntities,
				}

				if !d.LastAuthenticated.IsZero() {
					entry.LastAuthenticated = isoTime(d.LastAuthenticated)
					entry.LastAuthenticatedArn = d.LastAuthenticatedArn
				}

				xmlDetails = append(xmlDetails, entry)
			}

			return &GetServiceLastAccessedDetailsResponse{
				Xmlns: iamXMLNS,
				GetServiceLastAccessedDetailsResult: GetServiceLastAccessedDetailsResult{
					JobStatus:            status,
					JobCreationDate:      now,
					JobCompletionDate:    now,
					ServicesLastAccessed: xmlDetails,
					IsTruncated:          false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// jobStatusCompleted is the status returned by async IAM job stubs.
const jobStatusCompleted = "COMPLETED"
