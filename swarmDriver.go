package swarmdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	"github.com/ethereum/go-ethereum/common"
	beecrypto "github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/file"
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
	Synced    bool
	Store     store.PutGetter
	Encrypt   bool
	Publisher Publisher
	Lookuper  Lookuper
	Splitter  file.Splitter
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
	splitter := splitter.NewSimpleSplitter(store)
	return &swarmDriver{
		Store:     store,
		Encrypt:   encrypt,
		Lookuper:  lk,
		Publisher: pb,
		Splitter:  splitter,
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

func (d *swarmDriver) getMetadata(ctx context.Context, path string) (metaData, error) {
	metaRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		return metaData{}, fmt.Errorf("getMetadata: failed to lookup metadata: %v", err)
	}

	metaJoiner, _, err := joiner.New(ctx, d.Store, metaRef)
	if err != nil {
		return metaData{}, fmt.Errorf("getMetadata: failed to create reader for metadata: %v", err)
	}

	meta, err := fromMetadata(metaJoiner)
	if err != nil {
		return metaData{}, fmt.Errorf("getMetadata: failed to read metadata: %v", err)
	}

	return meta, nil
}

func (d *swarmDriver) putMetadata(ctx context.Context, path string, meta metaData) error {
	metaBuf, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to marshal metadata: %v", err)
	}

	metaRef, err := d.Splitter.Split(ctx, io.NopCloser(bytes.NewReader(metaBuf)), int64(len(metaBuf)), d.Encrypt)
	if err != nil || isZeroAddress(metaRef) {
		return fmt.Errorf("putMetadata: failed to split metadata: %v", err)
	}

	err = d.Publisher.Put(ctx, filepath.Join(path, "mtdt"), time.Now().Unix(), metaRef)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to publish metadata: %v", err)
	}

	return nil
}

func (d *swarmDriver) getData(ctx context.Context, path string) ([]byte, error) {

	dataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Printf("[Error] getData: Could not find data path %s", path)
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, fmt.Errorf("getData: failed to lookup data reference: %v", err)
	}

	dataJoiner, _, err := joiner.New(ctx, d.Store, dataRef)
	if err != nil {
		return nil, fmt.Errorf("getData: failed to create joiner for data: %v", err)
	}

	data, err := io.ReadAll(dataJoiner)
	if err != nil {
		return nil, fmt.Errorf("getData: failed to read data: %w", err)
	}

	return data, nil
}

func (d *swarmDriver) putData(ctx context.Context, path string, data []byte) error {

	dataRef, err := d.Splitter.Split(ctx, io.NopCloser(bytes.NewReader(data)), int64(len(data)), d.Encrypt)
	if err != nil || isZeroAddress(dataRef) {
		return fmt.Errorf("putData: failed to split data: %v", err)
	}

	err = d.Publisher.Put(ctx, filepath.Join(path, "data"), time.Now().Unix(), dataRef)
	if err != nil {
		return fmt.Errorf("putData: failed to publish data reference: %v", err)
	}

	return nil
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *swarmDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	log.Printf("[INFO] GetContent: Hit for path %v", path)

	// Fetch metadata using the helper function
	mtdt, err := d.getMetadata(ctx, path)
	if err != nil {
		if errors.Is(err, storagedriver.PathNotFoundError{}) {
			log.Fatalf("[Error] GetContent: Could not find metadata path %s", path)
		}
		return nil, err
	}

	// Check if data is a directory
	if mtdt.IsDir {
		return nil, fmt.Errorf("GetContent: Path to directory")
	}

	// Fetch data using the helper function
	data, err := d.getData(ctx, path)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (d *swarmDriver) PutContent(ctx context.Context, path string, content []byte) error {
	log.Printf("[INFO] PutContent: hit for Path: %v \n", path)

	// Determine parent path
	parentPath := filepath.ToSlash(filepath.Dir(path))
	log.Printf("PutContent: DataPath = %s \n", filepath.Base(path))
	log.Printf("PutContent: ParentPath = %s \n", parentPath)

	// Fetch parent metadata using the helper function
	parentMtdt, err := d.getMetadata(ctx, parentPath)
	if err != nil {
		if errors.Is(err, storagedriver.PathNotFoundError{}) {
			log.Fatalf("[Error] PutContent: Could not find parent metadata path %s \n", parentPath)
		}
		return err
	}

	// Split the content to get data reference
	dataRef, err := d.Splitter.Split(ctx, io.NopCloser(bytes.NewReader(content)), int64(len(content)), d.Encrypt)
	if err != nil || isZeroAddress(dataRef) {
		return fmt.Errorf("PutContent: failed to create splitter for new content: %v", err)
	}

	// Publish data reference
	if err := d.Publisher.Put(ctx, filepath.Join(path, "data"), time.Now().Unix(), dataRef); err != nil {
		return fmt.Errorf("PutContent: failed to publish new data reference: %v", err)
	}

	// Create and store metadata for the new content
	mtdt := metaData{
		IsDir:   false,
		Path:    path,
		ModTime: time.Now().Unix(),
		Size:    len(content),
	}

	if err := d.putMetadata(ctx, path, mtdt); err != nil {
		return fmt.Errorf("PutContent: failed to store metadata: %v", err)
	}

	// Update parent metadata if the path is not already present
	found := false
	for _, v := range parentMtdt.Children {
		if v == path {
			found = true
			break
		}
	}

	if !found {
		parentMtdt.Children = append(parentMtdt.Children, path)
		parentMtdt.ModTime = time.Now().Unix()

		if err := d.putMetadata(ctx, parentPath, parentMtdt); err != nil {
			return fmt.Errorf("PutContent: failed to update parent metadata: %v", err)
		}
	}

	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *swarmDriver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	log.Printf("[INFO] Reader: Hit for path %s \n", path)

	if offset < 0 {
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset}
	}

	// Lookup data reference for the given path
	dataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("[Error] reader: Could not find path %s \n", path)
		}
		return nil, storagedriver.PathNotFoundError{Path: path}
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

	// Fetch metadata using the helper function
	mtdt, err := d.getMetadata(ctx, path)
	if err != nil {
		if errors.Is(err, storagedriver.PathNotFoundError{}) {
			log.Fatalf("[Error] Stat: Could not find metadata path %s \n", path)
		}
		return nil, err
	}

	// Construct FileInfoFields from metadata
	fi := storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   mtdt.IsDir,
		ModTime: time.Unix(mtdt.ModTime, 0),
	}

	// Set the size if it's not a directory
	if !fi.IsDir {
		fi.Size = int64(mtdt.Size)
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *swarmDriver) List(ctx context.Context, path string) ([]string, error) {
	log.Printf("[INFO] List: Hit for path %s", path)

	// Fetch metadata using the helper function
	mtdt, err := d.getMetadata(ctx, path)
	if err != nil {
		if errors.Is(err, storagedriver.PathNotFoundError{}) {
			log.Fatalf("[Error] List: Could not find metadata path %s \n", path)
		}
		return nil, err
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

func (d *swarmDriver) Delete(ctx context.Context, path string) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	log.Printf("[INFO] Deleting path %s", path)

	// Retrieve parent path
	parentPath := filepath.Dir(path)

	// Fetch parent metadata using helper function
	parentMeta, err := d.getMetadata(ctx, parentPath)
	if err != nil {
		if errors.Is(err, storagedriver.PathNotFoundError{}) {
			log.Fatalf("[Error] Delete: Could not find parent metadata path: %s", parentPath)
		}
		return err
	}

	// Remove the path from the parent metadata's children
	parentMeta.Children = removeFromSlice(parentMeta.Children, filepath.Base(path))

	// Update parent metadata using helper function
	err = d.putMetadata(ctx, parentPath, parentMeta)
	if err != nil {
		return fmt.Errorf("Delete: failed to update parent metadata: %v", err)
	}

	log.Printf("[INFO] Successfully removed path %s from parent metadata", path)
	return nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
func (d *swarmDriver) Move(ctx context.Context, sourcePath string, destPath string) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	log.Printf("[INFO] Move: source=%s, destination=%s \n", sourcePath, destPath)

	// 1. Lookup and read source metadata
	sourceMeta, err := d.getMetadata(ctx, sourcePath)
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("[Error] Move: Could not find source metadata path %s \n", sourcePath)
		}
		return storagedriver.PathNotFoundError{Path: sourcePath}
	}

	// 2. Remove entry from the source parent
	sourceParentPath := filepath.Dir(sourcePath)
	sourceParentMeta, err := d.getMetadata(ctx, sourceParentPath)
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("[Error] Move: Could not find source parent metadata path %s \n", sourceParentPath)
		}
		return storagedriver.PathNotFoundError{Path: sourceParentPath}
	}
	sourceParentMeta.Children = removeFromSlice(sourceParentMeta.Children, sourcePath)
	if err := d.putMetadata(ctx, sourceParentPath, sourceParentMeta); err != nil {
		return fmt.Errorf("Move: failed to update source parent metadata: %v", err)
	}

	// 3. Add entry to the destination parent
	destParentPath := filepath.Dir(destPath)
	destParentMeta, err := d.getMetadata(ctx, destParentPath)
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("[Error] Move: Could not find destination parent metadata path %s \n", destParentPath)
		}
		return storagedriver.PathNotFoundError{Path: destParentPath}
	}
	destParentMeta.Children = append(destParentMeta.Children, destPath)
	if err := d.putMetadata(ctx, destParentPath, destParentMeta); err != nil {
		return fmt.Errorf("Move: failed to update destination parent metadata: %v", err)
	}

	// 4. Update metadata to the new destination path
	sourceMeta.Path = destPath
	if err := d.putMetadata(ctx, destPath, sourceMeta); err != nil {
		return fmt.Errorf("Move: failed to update metadata at destination path: %v", err)
	}

	// 5. Add data from sourcepath to destpath
	sourceDataRef, err := d.getData(ctx, filepath.Join(sourcePath, "data"))
	if err != nil {
		if err.Error() == "invalid chunk lookup" {
			log.Fatalf("[Error] Move: Could not find source data path %s \n", sourcePath)
		}
		return storagedriver.PathNotFoundError{Path: sourcePath}
	}
	if err := d.putData(ctx, filepath.Join(destPath, "data"), sourceDataRef); err != nil {
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
				log.Fatalf("[Error] Writer: Could not find path %s \n", path)
			}
			return nil, storagedriver.PathNotFoundError{Path: path}
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
	w.d.Mutex.Lock()
	defer w.d.Mutex.Unlock()
	log.Printf("[INFO] Commit initiated for path: %s", w.path)

	if w.closed {
		return fmt.Errorf("Commit: already closed")
	} else if w.committed {
		return fmt.Errorf("Commit: already committed")
	} else if w.cancelled {
		return fmt.Errorf("Commit: already cancelled")
	}

	// Use the helper function to split and store data
	err := w.d.putData(ctx, w.path, w.buffer.Bytes())
	if err != nil {
		return fmt.Errorf("Commit: failed to publish data reference: %v", err)
	}
	log.Printf("[INFO] Commit: Data committed for path: %s", w.path)

	// Create metadata for the committed content
	meta := metaData{
		IsDir:   false,
		Path:    w.path,
		ModTime: time.Now().Unix(),
		Size:    w.buffer.Len(),
	}

	// Publish the metadata using helper function
	if err := w.d.putMetadata(ctx, w.path, meta); err != nil {
		return fmt.Errorf("Commit: failed to publish metadata reference: %v", err)
	}
	log.Printf("[INFO] Commit: Metadata committed for path: %s", w.path)

	// Update parent metadata
	parentPath := filepath.ToSlash(filepath.Dir(w.path))
	parentMtdt, err := w.d.getMetadata(ctx, parentPath)
	if err != nil {
		// If parent metadata does not exist, create a new directory metadata
		parentMtdt = metaData{
			IsDir:    true,
			Path:     parentPath,
			ModTime:  time.Now().Unix(),
			Children: []string{w.path},
		}
		if err := w.d.putMetadata(ctx, parentPath, parentMtdt); err != nil {
			return fmt.Errorf("Commit: failed to publish new parent metadata reference: %v", err)
		}

		log.Printf("[INFO] Successfully created new parent metadata for path: %s", parentPath)
	} else {
		// Update existing parent metadata
		found := false
		for _, childPath := range parentMtdt.Children {
			if childPath == w.path {
				found = true
				break
			}
		}

		if !found {
			parentMtdt.Children = append(parentMtdt.Children, w.path)
			parentMtdt.ModTime = time.Now().Unix()

			if err := w.d.putMetadata(ctx, parentPath, parentMtdt); err != nil {
				return fmt.Errorf("Commit: failed to publish updated parent metadata reference: %v", err)
			}

			log.Printf("[INFO] Successfully updated parent metadata for path: %s", parentPath)
		}
	}

	// Reset the buffer after committing data and metadata
	w.buffer.Reset()

	// Mark the file as committed
	w.committed = true

	log.Printf("[INFO] Successfully committed data and metadata for path: %s", w.path)
	return nil
}
