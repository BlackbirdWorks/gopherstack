package elasticache

import (
	"errors"
	"net/http"
)

// paramErrorCode maps a parameter-validation sentinel to its wire fault.
func paramErrorCode(err error) (int, string, bool) {
	switch {
	case errors.Is(err, ErrInvalidParameterValue):
		return http.StatusBadRequest, "InvalidParameterValue", true
	case errors.Is(err, ErrInvalidParameterCombination):
		return http.StatusBadRequest, "InvalidParameterCombination", true
	case errors.Is(err, ErrNodeGroupNotFound):
		return http.StatusNotFound, "NodeGroupNotFoundFault", true
	case errors.Is(err, ErrNoOperation):
		return http.StatusBadRequest, "NoOperationFault", true
	case errors.Is(err, ErrNodeGroupsQuotaExceeded):
		return http.StatusBadRequest, "NodeGroupsPerReplicationGroupQuotaExceeded", true
	case errors.Is(err, ErrNodeQuotaForClusterExceeded):
		return http.StatusBadRequest, "NodeQuotaForClusterExceeded", true
	}

	return 0, "", false
}
