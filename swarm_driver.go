package swarmDriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	"github.com/ethersphere/bee/pkg/file/joiner"
	"github.com/ethersphere/bee/pkg/file/splitter"
	"github.com/ethersphere/bee/pkg/swarm"

	"github.com/Raviraj2000/swarmDriver/store"
)

const driverName = "swarm"

func init() {
	factory.Register(driverName, &swarmDriverFactory{})
}

// swarmDriverFactory implements the factory.StorageDriverFactory interface.
type swarmDriverFactory struct{}

func (factory *swarmDriverFactory) Create(ctx context.Context, parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	addr, ok := parameters["addr"].(swarm.Address)
	if !ok {
		return nil, fmt.Errorf("missing or invalid 'addr' parameter")
	}

	store, ok := parameters["store"].(store.PutGetter)
	if !ok {
		return nil, fmt.Errorf("missing or invalid 'store' parameter")
	}

	encrypt, ok := parameters["encrypt"].(bool)
	if !ok {
		return nil, fmt.Errorf("missing or invalid 'encrypt' parameter")
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
func New(addr swarm.Address, store store.PutGetter, encrypt bool) *swarmDriver {
	return &swarmDriver{
		Store:     store,
		Encrypt:   encrypt,
		Reference: addr,
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
		return metaData{}, fmt.Errorf("failed reading metadata %w", err)
	}
	err = json.Unmarshal(buf, &md)
	if err != nil {
		return metaData{}, fmt.Errorf("failed decoding metadata %w", err)
	}
	return md, nil
}

// Implement remaining StorageDriver methods

// GetContent retrieves the content stored at "path" as a []byte.
func (d *swarmDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	fmt.Printf("GetContent hit for path %v\n", path)

	mtdtRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup parent metadata: %v", err)
	}
	mtdtJoiner, _, err := joiner.New(ctx, d.Store, mtdtRef)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for metadata: %v", err)
	}
	mtdt, err := fromMetadata(mtdtJoiner)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}
	//check if data is a directory
	if mtdt.IsDir {
		return nil, fmt.Errorf("Path to directory")
	}

	dataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup parent metadata: %v", err)
	}

	dataJoiner, _, err := joiner.New(ctx, d.Store, dataRef)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for metadata: %v", err)
	}
	data, err := io.ReadAll(dataJoiner)
	if err != nil {
		return nil, fmt.Errorf("failed reading metadata %w", err)
	}

	return data, nil
}

func (d *swarmDriver) PutContent(ctx context.Context, path string, content []byte) error {
	fmt.Printf("PutContent hit for path %v", path)

	// Get reference for new content
	splitter := splitter.NewSimpleSplitter(d.Store)
	dataRef, err := splitter.Split(ctx, io.NopCloser(bytes.NewReader(content)), int64(len(content)), d.Encrypt)
	if err != nil || isZeroAddress(dataRef) {
		return fmt.Errorf("failed to split content: %v", err)
	}

	// Publish ref for new content
	if err := d.Publisher.Put(ctx, filepath.Join(path, "data"), time.Now().Unix(), dataRef); err != nil {
		return fmt.Errorf("failed to publish new data reference: %v", err)
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
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	// Get reference for metadata
	mtdtRef, err := splitter.Split(ctx, io.NopCloser(bytes.NewReader(mtdtJson)), int64(len(mtdtJson)), d.Encrypt)
	if err != nil || isZeroAddress(mtdtRef) {
		return fmt.Errorf("failed to split metadata: %v", err)
	}

	// Publish content metadata
	if err := d.Publisher.Put(ctx, filepath.Join(path, "mtdt"), time.Now().Unix(), mtdtRef); err != nil {
		return fmt.Errorf("failed to publish metadata reference: %v", err)
	}

	//Check if file path already exists in metadata of parent
	// Use lookuper to get metadata of parent
	parentPath := filepath.Dir(path)

	parentMtdtRef, err := d.Lookuper.Get(ctx, filepath.Join(parentPath, "mtdt"), time.Now().Unix())
	if err != nil {
		return fmt.Errorf("failed to lookup parent metadata: %v", err)
	}
	parentMtdtRefJoiner, _, err := joiner.New(ctx, d.Store, parentMtdtRef)
	if err != nil {
		return fmt.Errorf("failed to create reader for metadata: %v", err)
	}
	parentMtdt, err := fromMetadata(parentMtdtRefJoiner)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %v", err)
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
			return fmt.Errorf("failed to marshal parent metadata: %v", err)
		}

		parentMtdtBufRef, err := splitter.Split(ctx, io.NopCloser(bytes.NewReader(parentMtdtBuf)), int64(len(parentMtdtBuf)), d.Encrypt)
		if err != nil || isZeroAddress(parentMtdtBufRef) {
			return fmt.Errorf("failed to split parent metadata: %v", err)
		}

		if err := d.Publisher.Put(ctx, filepath.Join(parentPath, "mtdt"), time.Now().Unix(), parentMtdtBufRef); err != nil {
			return fmt.Errorf("failed to publish parent metadata reference: %v", err)
		}
	}

	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *swarmDriver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

	return d.reader(ctx, path, offset)
}

func (d *swarmDriver) reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	fmt.Println("reader:")
	fmt.Println(path)

	if offset < 0 {
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset}
	}

	// Lookup data reference for the given path
	dataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup data: %v", err)
	}

	// Create a joiner to read the data
	dataJoiner, _, err := joiner.New(ctx, d.Store, dataRef)
	if err != nil {
		return nil, fmt.Errorf("failed to create joiner: %v", err)
	}

	// Seek to the specified offset
	if _, err := dataJoiner.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d: %v", offset, err)
	}

	return io.NopCloser(dataJoiner), nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *swarmDriver) Writer(ctx context.Context, path string, app bool) (storagedriver.FileWriter, error) {
	// Lookup the new data reference at the specified path
	newDataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup new data: %v", err)
	}

	// Create a new FileWriter with the data reference and append flag
	return d.newWriter(newDataRef), nil
}

// Stat returns info about the provided path.
func (d *swarmDriver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {

	mtdtRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup parent metadata: %v", err)
	}
	mtdtJoiner, _, err := joiner.New(ctx, d.Store, mtdtRef)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for metadata: %v", err)
	}
	mtdt, err := fromMetadata(mtdtJoiner)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
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

	mtdtRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup parent metadata: %v", err)
	}
	mtdtJoiner, _, err := joiner.New(ctx, d.Store, mtdtRef)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for metadata: %v", err)
	}
	mtdt, err := fromMetadata(mtdtJoiner)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}

	if !mtdt.IsDir {
		return nil, fmt.Errorf("not a directory") // TODO(stevvooe): Need error type for this...
	}

	return mtdt.Children, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
func (d *swarmDriver) Move(ctx context.Context, sourcePath string, destPath string) error {

	// 1. Lookup and read source metadata
	sourceMetaRef, err := d.Lookuper.Get(ctx, filepath.Join(sourcePath, "mtdt"), time.Now().Unix())
	if err != nil {
		return fmt.Errorf("failed to lookup source metadata: %v", err)
	}
	sourceMetaJoiner, _, err := joiner.New(ctx, d.Store, sourceMetaRef)
	if err != nil {
		return fmt.Errorf("failed to create reader for source metadata: %v", err)
	}
	sourceMeta, err := fromMetadata(sourceMetaJoiner)
	if err != nil {
		return fmt.Errorf("failed to read source metadata: %v", err)
	}

	// 2. Remove entry from the source parent
	sourceParentPath := filepath.Dir(sourcePath)
	sourceParentMetaRef, err := d.Lookuper.Get(ctx, filepath.Join(sourceParentPath, "mtdt"), time.Now().Unix())
	if err != nil {
		return fmt.Errorf("failed to lookup parent metadata: %v", err)
	}
	sourceParentMetaJoiner, _, err := joiner.New(ctx, d.Store, sourceParentMetaRef)
	if err != nil {
		return fmt.Errorf("failed to create reader for parent metadata: %v", err)
	}
	sourceParentMeta, err := fromMetadata(sourceParentMetaJoiner)
	if err != nil {
		return fmt.Errorf("failed to read parent metadata: %v", err)
	}
	sourceParentMeta.Children = removeFromSlice(sourceParentMeta.Children, sourcePath)

	// 3. Add entry to the destination parent
	destParentPath := filepath.Dir(destPath)
	destParentMetaRef, err := d.Lookuper.Get(ctx, filepath.Join(destParentPath, "mtdt"), time.Now().Unix())
	if err != nil {
		return fmt.Errorf("failed to lookup destination parent metadata: %v", err)
	}
	destParentMetaJoiner, _, err := joiner.New(ctx, d.Store, destParentMetaRef)
	if err != nil {
		return fmt.Errorf("failed to create reader for destination parent metadata: %v", err)
	}
	destParentMeta, err := fromMetadata(destParentMetaJoiner)
	if err != nil {
		return fmt.Errorf("failed to read destination parent metadata: %v", err)
	}
	destParentMeta.Children = append(destParentMeta.Children, destPath)

	// 4. Update metadata to the new destination path
	sourceMeta.Path = destPath
	newMetaBuf, err := json.Marshal(sourceMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal destination metadata: %v", err)
	}
	newMetaRef, err := splitter.NewSimpleSplitter(d.Store).Split(ctx, io.NopCloser(bytes.NewReader(newMetaBuf)), int64(len(newMetaBuf)), d.Encrypt)
	if err != nil {
		return fmt.Errorf("failed to split destination metadata: %v", err)
	}
	err = d.Publisher.Put(ctx, filepath.Join(destPath, "mtdt"), time.Now().Unix(), newMetaRef)
	if err != nil {
		return fmt.Errorf("failed to publish destination metadata: %v", err)
	}

	// 5. Delete the old source metadata
	err = d.Delete(ctx, sourcePath)
	if err != nil {
		return fmt.Errorf("failed to delete source metadata: %v", err)
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

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *swarmDriver) Delete(ctx context.Context, path string) error {

	fmt.Println("Delete:")
	fmt.Println(path)
	normalized := normalize(path)

	err := d.root.delete(normalized)
	switch err {
	case errNotExists:
		return storagedriver.PathNotFoundError{Path: path}
	default:
		return err
	}
}

// RedirectURL returns a URL which may be used to retrieve the content stored at the given path.
func (d *swarmDriver) RedirectURL(*http.Request, string) (string, error) {
	return "", nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file and directory
func (d *swarmDriver) Walk(ctx context.Context, path string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	fmt.Println("Walk:")
	fmt.Println(path)
	return storagedriver.WalkFallback(ctx, d, path, f, options...)
}

type swarmFile struct {
	d         *swarmDriver
	Reference swarm.Address
	BufSize   int64
	closed    bool
	committed bool
	cancelled bool
}

func (d *swarmDriver) newWriter(ref swarm.Address, bufSize int64) storagedriver.FileWriter {
	return &swarmFile{
		d:         d,
		Reference: ref,
		BufSize:   bufSize,
	}
}

func (w *swarmFile) Read(buf []byte) (int, error) {

	return 0, nil
}

func (w *swarmFile) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	if cap(w.buffer) < len(p)+w.buffSize {
		data := make([]byte, len(w.buffer), len(p)+w.buffSize)
		copy(data, w.buffer)
		w.buffer = data
	}

	w.buffer = w.buffer[:w.buffSize+len(p)]
	n := copy(w.buffer[w.buffSize:w.buffSize+len(p)], p)
	w.buffSize += n

	return n, nil
}

func (w *swarmFile) Size() int64 {
	w.d.Mutex.RLock()
	defer w.d.Mutex.RUnlock()

	return w.BufSize
}

func (w *swarmFile) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true

	if err := w.flush(); err != nil {
		return err
	}

	return nil
}

func (w *swarmFile) Cancel(ctx context.Context) error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true

	w.d.Mutex.Lock()
	defer w.d.Mutex.Unlock()

	w = nil

	return nil
}

func (w *swarmFile) Commit(ctx context.Context) error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	w.committed = true

	if err := w.flush(); err != nil {
		return err
	}

	return nil
}

func (w *swarmFile) flush() error {
	w.d.Mutex.Lock()
	defer w.d.Mutex.Unlock()

	if _, err := w.f.WriteAt(w.buffer, int64(len(w.f.data))); err != nil {
		return err
	}
	w.buffer = []byte{}
	w.BufSize = 0

	return nil
}
