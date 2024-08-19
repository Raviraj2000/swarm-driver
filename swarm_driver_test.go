package swarmDriver

import (
	"testing"

	"github.com/Raviraj2000/swarmDriver/testStore"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	"github.com/ethersphere/bee/pkg/swarm"
)

func newSwarmDriverConstructor() (storagedriver.StorageDriver, error) {
	addr := swarm.NewAddress([]byte("0000000000000000000000000000000000000000000000000000000000000000"))
	encrypt := false
	store := testStore.NewSwarmInMemoryStore()

	return New(addr, store, encrypt), nil
}

func TestSwarmDriverSuite(t *testing.T) {
	testsuites.Driver(t, newSwarmDriverConstructor)
}

func BenchmarkSwarmDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, newSwarmDriverConstructor)
}
