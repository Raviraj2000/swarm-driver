package swarmDriver

import (
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
)

func newSwarmDriverConstructor() (storagedriver.StorageDriver, error) {
	// Replace these with actual arguments required by your New function
	addr := /* your swarm address */
	store := /* your storage implementation */
	encrypt := /* your encryption flag */

	return New(addr, store, encrypt), nil
}

func TestSwarmDriverSuite(t *testing.T) {
	testsuites.Driver(t, newSwarmDriverConstructor)
}

func BenchmarkSwarmDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, newSwarmDriverConstructor)
}
