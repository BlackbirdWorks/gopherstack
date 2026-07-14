package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_FailoverDBCluster verifies FailoverDBCluster performs a real writer
// promotion (not a disguised no-op): a cluster with fewer than two members
// has no reader to fail over to and must error, a two-member cluster must
// swap which member is the writer, and an explicit TargetDBInstanceIdentifier
// must be honored (or rejected when invalid).
func Test_FailoverDBCluster(t *testing.T) {
	t.Parallel()

	t.Run("no reader available errors", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "solo-cluster")
		createInstance(t, h, "solo-inst", "solo-cluster")

		rr := doRequest(t, h, url.Values{
			"Action":              {"FailoverDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"solo-cluster"},
		})
		require.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Contains(t, rr.Body.String(), "InvalidDBClusterStateFault")
	})

	t.Run("promotes a reader to writer", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "fo-cluster")
		createInstance(t, h, "fo-writer", "fo-cluster")
		createInstance(t, h, "fo-reader", "fo-cluster")

		rr := doRequest(t, h, url.Values{
			"Action":              {"FailoverDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"fo-cluster"},
		})
		require.Equal(t, http.StatusOK, rr.Code)
		body := rr.Body.String()
		assert.Contains(
			t, body,
			"<DBInstanceIdentifier>fo-reader</DBInstanceIdentifier><IsClusterWriter>true</IsClusterWriter>",
		)
		assert.Contains(
			t, body,
			"<DBInstanceIdentifier>fo-writer</DBInstanceIdentifier><IsClusterWriter>false</IsClusterWriter>",
		)
	})

	t.Run("honors explicit target instance", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "fo-target-cluster")
		createInstance(t, h, "fo-target-writer", "fo-target-cluster")
		createInstance(t, h, "fo-target-a", "fo-target-cluster")
		createInstance(t, h, "fo-target-b", "fo-target-cluster")

		rr := doRequest(t, h, url.Values{
			"Action":                     {"FailoverDBCluster"},
			"Version":                    {"2014-10-31"},
			"DBClusterIdentifier":        {"fo-target-cluster"},
			"TargetDBInstanceIdentifier": {"fo-target-b"},
		})
		require.Equal(t, http.StatusOK, rr.Code)
		body := rr.Body.String()
		assert.Contains(
			t, body,
			"<DBInstanceIdentifier>fo-target-b</DBInstanceIdentifier><IsClusterWriter>true</IsClusterWriter>",
		)
		assert.Contains(
			t, body,
			"<DBInstanceIdentifier>fo-target-a</DBInstanceIdentifier><IsClusterWriter>false</IsClusterWriter>",
		)
	})

	t.Run("invalid target instance errors", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "fo-badtarget-cluster")
		createInstance(t, h, "fo-badtarget-writer", "fo-badtarget-cluster")
		createInstance(t, h, "fo-badtarget-reader", "fo-badtarget-cluster")

		rr := doRequest(t, h, url.Values{
			"Action":                     {"FailoverDBCluster"},
			"Version":                    {"2014-10-31"},
			"DBClusterIdentifier":        {"fo-badtarget-cluster"},
			"TargetDBInstanceIdentifier": {"does-not-exist"},
		})
		require.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Contains(t, rr.Body.String(), "InvalidDBInstanceState")
	})

	t.Run("unknown cluster errors", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		rr := doRequest(t, h, url.Values{
			"Action":              {"FailoverDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"missing-cluster"},
		})
		require.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
	})
}
