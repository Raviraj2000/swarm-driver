package swarmdriver

import (
	"testing"

	"github.com/Raviraj2000/swarmdriver/store/teststore"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	"github.com/ethereum/go-ethereum/common"
)

func newSwarmDriverConstructor() (storagedriver.StorageDriver, error) {
	addr := common.HexToAddress("0xabcd")
	encrypt := false
	store := teststore.NewSwarmInMemoryStore()

	return New(addr, store, encrypt), nil
}

func TestSwarmDriverSuite(t *testing.T) {
	testsuites.Driver(t, newSwarmDriverConstructor)
}

func BenchmarkSwarmDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, newSwarmDriverConstructor)
}
