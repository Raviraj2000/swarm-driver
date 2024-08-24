package swarmdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	"github.com/ethereum/go-ethereum/common"
	beecrypto "github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/file/joiner"
	"github.com/ethersphere/bee/pkg/file/splitter"
	"github.com/ethersphere/bee/pkg/swarm"

	"github.com/Raviraj2000/swarmdriver/lookuper"
	"github.com/Raviraj2000/swarmdriver/publisher"
	"github.com/Raviraj2000/swarmdriver/store"
)

const driverName = "swarm"

func init() {
	factory.Register(driverName, &swarmDriverFactory{})
}

// swarmDriverFactory implements the factory.StorageDriverFactory interface.
type swarmDriverFactory struct{}

func (factory *swarmDriverFactory) Create(ctx context.Context, parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	addr, ok := parameters["addr"].(common.Address)
	if !ok {
		return nil, fmt.Errorf("Create: missing or invalid 'addr' parameter")
	}

	store, ok := parameters["store"].(store.PutGetter)
	if !ok {
		return nil, fmt.Errorf("Create: missing or invalid 'store' parameter")
	}

	encrypt, ok := parameters["encrypt"].(bool)
	if !ok {
		return nil, fmt.Errorf("Create: missing or invalid 'encrypt' parameter")
	}

	return New(addr, store, encrypt), nil
}

type Publisher interface {
	Put(ctx context.Context, id string, version int64, ref swarm.Address) error
}

type Lookuper interface {
	Get(ctx context.Context, id string, version int64) (swarm.Address, error)
}

type swarmDriver struct {
	Mutex     sync.RWMutex
	Reference swarm.Address
	Synced    bool
	Store     store.PutGetter
	Encrypt   bool
	Publisher Publisher
	Lookuper  Lookuper
}

type metaData struct {
	IsDir    bool
	Path     string
	ModTime  int64
	Size     int
	Children []string
}

var _ storagedriver.StorageDriver = &swarmDriver{}

// Check if address is a zero address
func isZeroAddress(ref swarm.Address) bool {
	if ref.Equal(swarm.ZeroAddress) {
		return true
	}
	zeroAddr := make([]byte, 32)
	return swarm.NewAddress(zeroAddr).Equal(ref)
}

// New constructs a new Driver.
func New(addr common.Address, store store.PutGetter, encrypt bool) *swarmDriver {
	log.Printf("[INFO] Creating New Swarm Driver")
	pk, err := beecrypto.GenerateSecp256k1Key()
	if err != nil {
		panic(err)
	}
	signer := beecrypto.NewDefaultSigner(pk)
	ethAddress, err := signer.EthereumAddress()
	if err != nil {
		panic(err)
	}
	lk := lookuper.New(store, ethAddress)
	pb := publisher.New(store, signer, lookuper.Latest(store, addr))
	return &swarmDriver{
		Store:     store,
		Encrypt:   encrypt,
		Lookuper:  lk,
		Publisher: pb,
	}
}

// Implement the storagedriver.StorageDriver interface.
func (d *swarmDriver) Name() string {
	return driverName
}

func fromMetadata(reader io.Reader) (metaData, error) {
	md := metaData{}
	buf, err := io.ReadAll(reader)
	if err != nil {
		return metaData{}, fmt.Errorf("fromMetadata: failed reading metadata %w", err)
	}
	err = json.Unmarshal(buf, &md)
	if err != nil {
		return metaData{}, fmt.Errorf("fromMetadata: failed unmarshalling metadata %w", err)
	}
	return md, nil
}

func (d *swarmDriver) Delete(ctx context.Context, path string) error {
	log.Printf("[INFO] Deleting path %s", path)
	return nil
}

// Implement remaining StorageDriver methods

// GetContent retrieves the content stored at "path" as a []byte.
func (d *swarmDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	log.Printf("[INFO] GetContent: Hit for path %v\n", path)

	mtdtRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("Could not find parent path %s \n", path)
		}
		return nil, fmt.Errorf("GetContent: failed to lookup path metadata: %v", err)
	}
	mtdtJoiner, _, err := joiner.New(ctx, d.Store, mtdtRef)
	if err != nil {
		return nil, fmt.Errorf("GetContent: failed to create joiner for metadata: %v", err)
	}
	mtdt, err := fromMetadata(mtdtJoiner)
	if err != nil {
		return nil, fmt.Errorf("GetContent: failed to read metadata: %v", err)
	}
	//check if data is a directory
	if mtdt.IsDir {
		return nil, fmt.Errorf("GetContent: Path to directory")
	}

	dataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("GetContent: failed to lookup path data: %v", err)
	}

	dataJoiner, _, err := joiner.New(ctx, d.Store, dataRef)
	if err != nil {
		return nil, fmt.Errorf("GetContent:  failed to create joiner for data: %v", err)
	}
	data, err := io.ReadAll(dataJoiner)
	if err != nil {
		return nil, fmt.Errorf("GetContent: failed to read data %w", err)
	}

	return data, nil
}

func (d *swarmDriver) PutContent(ctx context.Context, path string, content []byte) error {
	log.Printf("[INFO] PutContent: hit for Path: %v \n", path)

	//Check if file path already exists in metadata of parent
	// Use lookuper to get metadata of parent
	parentPath := filepath.ToSlash(filepath.Dir(path))
	fmt.Printf("PutContent: DataPath = %s \n", filepath.Base(path))
	fmt.Printf("PutContent: ParentPath = %s \n", parentPath)

	parentMtdtRef, err := d.Lookuper.Get(ctx, filepath.Join(parentPath, "mtdt"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("Could not find parent path %s \n", parentPath)
		}
		return fmt.Errorf("PutContent: failed to lookup parent  metadata: %v", err)
	}
	parentMtdtRefJoiner, _, err := joiner.New(ctx, d.Store, parentMtdtRef)
	if err != nil {
		return fmt.Errorf("PutContent: failed to create joiner for metadata: %v", err)
	}
	parentMtdt, err := fromMetadata(parentMtdtRefJoiner)
	if err != nil {
		return fmt.Errorf("PutContent: failed to read parent metadata: %v", err)
	}

	// Get reference for new content
	splitter := splitter.NewSimpleSplitter(d.Store)
	dataRef, err := splitter.Split(ctx, io.NopCloser(bytes.NewReader(content)), int64(len(content)), d.Encrypt)
	if err != nil || isZeroAddress(dataRef) {
		return fmt.Errorf("PutContent: failed to create splitter for new content: %v", err)
	}

	// Publish ref for new content
	if err := d.Publisher.Put(ctx, filepath.Join(path, "data"), time.Now().Unix(), dataRef); err != nil {
		return fmt.Errorf("PutContent: failed to publish new data reference: %v", err)
	}

	// Create metadata for content
	mtdt := metaData{
		IsDir:   false,
		Path:    path,
		ModTime: time.Now().Unix(),
		Size:    len(content),
	}

	mtdtJson, err := json.Marshal(mtdt)
	if err != nil {
		return fmt.Errorf("PutContent: failed to marshal metadata: %v", err)
	}

	// Get reference for metadata
	mtdtRef, err := splitter.Split(ctx, io.NopCloser(bytes.NewReader(mtdtJson)), int64(len(mtdtJson)), d.Encrypt)
	if err != nil || isZeroAddress(mtdtRef) {
		return fmt.Errorf("PutContent: failed to create splitter for metadata: %v", err)
	}

	// Publish content metadata
	if err := d.Publisher.Put(ctx, filepath.Join(path, "mtdt"), time.Now().Unix(), mtdtRef); err != nil {
		return fmt.Errorf("PutContent: failed to publish metadata reference: %v", err)
	}

	// Check if file path in children of parent
	found := false
	for _, v := range parentMtdt.Children {
		if strings.Index(v, path) != -1 {
			found = true
			break
		}
	}
	// if file not in parent path then add
	if !found {
		parentMtdt.Children = append(parentMtdt.Children, path)
		parentMtdt.ModTime = time.Now().Unix()

		parentMtdtBuf, err := json.Marshal(parentMtdt)
		if err != nil {
			return fmt.Errorf("PutContent: failed to marshal parent metadata: %v", err)
		}

		parentMtdtBufRef, err := splitter.Split(ctx, io.NopCloser(bytes.NewReader(parentMtdtBuf)), int64(len(parentMtdtBuf)), d.Encrypt)
		if err != nil || isZeroAddress(parentMtdtBufRef) {
			return fmt.Errorf("PutContent: failed to create splitter for parent metadata: %v", err)
		}

		if err := d.Publisher.Put(ctx, filepath.Join(parentPath, "mtdt"), time.Now().Unix(), parentMtdtBufRef); err != nil {
			return fmt.Errorf("PutContent: failed to publish parent metadata reference: %v", err)
		}
	}

	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *swarmDriver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	log.Printf("[INFO] Reader: Hit for path:%s and offset:%d", path, offset)
	return d.reader(ctx, path, offset)
}

func (d *swarmDriver) reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	log.Printf("[INFO] reader: Hit for path %s \n", path)

	if offset < 0 {
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset}
	}

	// Lookup data reference for the given path
	dataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("Could not find path %s \n", path)
		}
		return nil, fmt.Errorf("reader: failed to lookup data: %v", err)
	}

	// Create a joiner to read the data
	dataJoiner, _, err := joiner.New(ctx, d.Store, dataRef)
	if err != nil {
		return nil, fmt.Errorf("reader: failed to create joiner: %v", err)
	}

	// Seek to the specified offset
	if _, err := dataJoiner.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("reader: failed to seek to offset %d: %v", offset, err)
	}

	return io.NopCloser(dataJoiner), nil
}

// Stat returns info about the provided path.
func (d *swarmDriver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	log.Printf("[INFO] Stat: Hit for path %s", path)
	mtdtRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("Could not find metadata path %s \n", path)
		}
		return nil, fmt.Errorf("Stat: failed to lookup parent metadata: %v", err)
	}
	mtdtJoiner, _, err := joiner.New(ctx, d.Store, mtdtRef)
	if err != nil {
		return nil, fmt.Errorf("Stat: failed to create joiner for metadata: %v", err)
	}
	mtdt, err := fromMetadata(mtdtJoiner)
	if err != nil {
		return nil, fmt.Errorf("Stat: failed to read metadata: %v", err)
	}

	fi := storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   mtdt.IsDir,
		ModTime: time.Unix(mtdt.ModTime, 0),
	}

	if !fi.IsDir {
		fi.Size = int64(mtdt.Size)
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *swarmDriver) List(ctx context.Context, path string) ([]string, error) {
	// Retrieve metadata reference
	mtdtRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("Could not find metadata path %s \n", path)
		}
		return nil, fmt.Errorf("List: failed to lookup parent metadata: %v", err)
	}

	// Create a joiner to read the metadata
	mtdtJoiner, _, err := joiner.New(ctx, d.Store, mtdtRef)
	if err != nil {
		return nil, fmt.Errorf("List: failed to create joiner for metadata: %v", err)
	}

	// Parse the metadata
	mtdt, err := fromMetadata(mtdtJoiner)
	if err != nil {
		return nil, fmt.Errorf("List: failed to read metadata: %v", err)
	}

	// Ensure it's a directory
	if !mtdt.IsDir {
		return nil, fmt.Errorf("List: not a directory")
	}

	// Ensure children are not nil
	if mtdt.Children == nil {
		return nil, fmt.Errorf("List: no children found")
	}

	return mtdt.Children, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
func (d *swarmDriver) Move(ctx context.Context, sourcePath string, destPath string) error {
	log.Printf("[INFO] Move: source=%s, destination=%s \n", sourcePath, destPath)
	// 1. Lookup and read source metadata
	sourceMetaRef, err := d.Lookuper.Get(ctx, filepath.Join(sourcePath, "mtdt"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("Could not find source metadata path %s \n", sourcePath)
		}
		return fmt.Errorf("Move: failed to lookup source metadata: %v", err)
	}
	sourceMetaJoiner, _, err := joiner.New(ctx, d.Store, sourceMetaRef)
	if err != nil {
		return fmt.Errorf("Move: failed to create joiner for source metadata: %v", err)
	}
	sourceMeta, err := fromMetadata(sourceMetaJoiner)
	if err != nil {
		return fmt.Errorf("Move: failed to read source metadata: %v", err)
	}

	// 2. Remove entry from the source parent
	sourceParentPath := filepath.Dir(sourcePath)
	sourceParentMetaRef, err := d.Lookuper.Get(ctx, filepath.Join(sourceParentPath, "mtdt"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("Could not find source parent metadata path %s \n", sourceParentPath)
		}
		return fmt.Errorf("Move: failed to lookup source parent metadata: %v", err)
	}
	sourceParentMetaJoiner, _, err := joiner.New(ctx, d.Store, sourceParentMetaRef)
	if err != nil {
		return fmt.Errorf("Move: failed to create joiner for source parent metadata: %v", err)
	}
	sourceParentMeta, err := fromMetadata(sourceParentMetaJoiner)
	if err != nil {
		return fmt.Errorf("Move: failed to read source parent metadata: %v", err)
	}
	sourceParentMeta.Children = removeFromSlice(sourceParentMeta.Children, sourcePath)

	// 3. Add entry to the destination parent
	destParentPath := filepath.Dir(destPath)
	destParentMetaRef, err := d.Lookuper.Get(ctx, filepath.Join(destParentPath, "mtdt"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("Could not find destination parent metadata path %s \n", destParentPath)
		}
		return fmt.Errorf("Move: failed to lookup destination parent metadata: %v", err)
	}
	destParentMetaJoiner, _, err := joiner.New(ctx, d.Store, destParentMetaRef)
	if err != nil {
		return fmt.Errorf("Move: failed to create reader for destination parent metadata: %v", err)
	}
	destParentMeta, err := fromMetadata(destParentMetaJoiner)
	if err != nil {
		return fmt.Errorf("Move: failed to read destination parent metadata: %v", err)
	}
	destParentMeta.Children = append(destParentMeta.Children, destPath)

	// 4. Update metadata to the new destination path
	sourceMeta.Path = destPath
	newMetaBuf, err := json.Marshal(sourceMeta)
	if err != nil {
		return fmt.Errorf("Move: failed to marshal destination metadata: %v", err)
	}
	newMetaRef, err := splitter.NewSimpleSplitter(d.Store).Split(ctx, io.NopCloser(bytes.NewReader(newMetaBuf)), int64(len(newMetaBuf)), d.Encrypt)
	if err != nil {
		return fmt.Errorf("Move: failed to split destination metadata: %v", err)
	}
	err = d.Publisher.Put(ctx, filepath.Join(destPath, "mtdt"), time.Now().Unix(), newMetaRef)
	if err != nil {
		return fmt.Errorf("Move: failed to publish destination metadata: %v", err)
	}

	// 5. Add data from sourcepath to destpath
	sourceDataRef, err := d.Lookuper.Get(ctx, filepath.Join(sourcePath, "data"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("Could not find source data path %s \n", sourcePath)
		}
		return fmt.Errorf("Move: failed to lookup source data: %v", err)
	}
	err = d.Publisher.Put(ctx, filepath.Join(destPath, "data"), time.Now().Unix(), sourceDataRef)
	if err != nil {
		return fmt.Errorf("Move: failed to publish data to destination: %v", err)
	}

	return nil

}

func removeFromSlice(slice []string, item string) []string {
	for i, v := range slice {
		if v == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// RedirectURL returns a URL which may be used to retrieve the content stored at the given path.
func (d *swarmDriver) RedirectURL(*http.Request, string) (string, error) {
	log.Printf("[INFO] Redirect hit")
	return "", nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file and directory
func (d *swarmDriver) Walk(ctx context.Context, path string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	log.Printf("[INFO] Walking path %s", path)
	fmt.Println(path)
	return nil
}

type swarmFile struct {
	d         *swarmDriver
	path      string
	buffer    *bytes.Buffer
	bufSize   int64
	closed    bool
	committed bool
	cancelled bool
	offset    int64
	//ref       swarm.Address
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *swarmDriver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	log.Printf("[INFO] Writer: Hit for Path: %v \n", path)
	var combinedData bytes.Buffer
	w := &swarmFile{
		d:         d,
		path:      path,
		bufSize:   0,
		closed:    false,
		committed: false,
		cancelled: false,
	}

	if append {
		// Lookup existing data at the specified path
		oldDataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
		if err != nil {
			if err.Error() == "invalid chunk lookup" {
				log.Fatalf("Could not find path %s \n", path)
			}
			return nil, fmt.Errorf("Writer: failed to lookup old data: %v", err)
		}

		// Create a joiner to read the existing data
		oldDataJoiner, _, err := joiner.New(ctx, d.Store, oldDataRef)
		if err != nil {
			return nil, fmt.Errorf("Writer: failed to create joiner for old data: %v", err)
		}

		// Copy existing data into the buffer
		if _, err := io.Copy(&combinedData, oldDataJoiner); err != nil {
			return nil, fmt.Errorf("Writer: failed to copy old data: %v", err)
		}

	}

	// Set the buffer and size in the writer
	w.buffer = &combinedData

	// Return the FileWriter
	return w, nil
}

// func (d *swarmDriver) newWriter(path string, buf bytes.Buffer, bufSize int64) storagedriver.FileWriter {
// 	return &swarmFile{
// 		d:       d,
// 		path:    path,
// 		bufSize: bufSize,
// 		buffer:  buf,
// 	}
// }

func (w *swarmFile) Read(buf []byte) (int, error) {
	log.Printf("[INFO] Read hit")
	return 0, nil
}

func (w *swarmFile) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("Write: already closed")
	} else if w.committed {
		return 0, fmt.Errorf("Write: already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("Write: already cancelled")
	}

	return w.buffer.Write(p)
}

func (w *swarmFile) Size() int64 {
	log.Printf("[INFO] Size hit")
	w.d.Mutex.RLock()
	defer w.d.Mutex.RUnlock()

	return int64(w.buffer.Len())
}

func (w *swarmFile) Close() error {
	log.Printf("[INFO] Close hit")
	if w.closed {
		return fmt.Errorf("Close: already closed")
	}
	w.closed = true

	return nil
}

func (w *swarmFile) Cancel(ctx context.Context) error {
	log.Printf("[INFO] Cancel hit")
	if w.closed {
		return fmt.Errorf("Cancel: already closed")
	} else if w.committed {
		return fmt.Errorf("Cancel: already committed")
	}
	w.cancelled = true

	w.d.Mutex.Lock()
	defer w.d.Mutex.Unlock()

	w = nil

	return nil
}

func (w *swarmFile) Commit(ctx context.Context) error {
	log.Printf("[INFO] Commit Hit")
	if w.closed {
		return fmt.Errorf("Commit: already closed")
	} else if w.committed {
		return fmt.Errorf("Commit: already committed")
	} else if w.cancelled {
		return fmt.Errorf("Commit: already cancelled")
	}

	// Mark the file as committed
	w.committed = true

	// Create a splitter to handle the data in the buffer
	splitter := splitter.NewSimpleSplitter(w.d.Store)
	newRef, err := splitter.Split(ctx, io.NopCloser(w.buffer), int64(w.buffer.Len()), w.d.Encrypt)
	if err != nil {
		return fmt.Errorf("Commit: failed to split buffer content: %v", err)
	}

	// Publish the new reference to the specified path
	err = w.d.Publisher.Put(ctx, filepath.Join(w.path, "data"), time.Now().Unix(), newRef)
	if err != nil {
		return fmt.Errorf("Commit: failed to publish data reference: %v", err)
	}

	w.buffer.Reset()

	return nil
}
