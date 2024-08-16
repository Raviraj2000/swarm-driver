package swarm

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

	"github.com/Raviraj2000/swarm-driver/lookuper"
	"github.com/Raviraj2000/swarm-driver/publisher"
	"github.com/Raviraj2000/swarm-driver/store"
)

const driverName = "swarm"

func init() {
	factory.Register(driverName, &swarmDriverFactory{})
}

// swarmDriverFactory implements the factory.StorageDriverFactory interface.
type swarmDriverFactory struct{}

func (factory *swarmDriverFactory) Create(ctx context.Context, parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return New(), nil
}

type Publisher interface {
	Put(ctx context.Context, id string, version int64, ref swarm.Address) error
}

type Lookuper interface {
	Get(ctx context.Context, id string, version int64) (swarm.Address, error)
}

type swarmDriver struct {
	mutex     sync.RWMutex
	reference swarm.Address
	synced    bool
	store     store.PutGetter
	encrypt   bool
	publisher publisher.Publisher
	lookuper  lookuper.Lookuper
}

type metaData struct {
	isDir    bool
	Path     string
	ModTime  int64
	Size     int
	children []string
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
		store:     store,
		encrypt:   encrypt,
		reference: addr,
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
	md.Size = len(buf)
	return md, nil
}

// Implement remaining StorageDriver methods

// GetContent retrieves the content stored at "path" as a []byte.
func (d *swarmDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	fmt.Printf("GetContent hit for path %v\n", path)

	mtdt_ref, err := d.lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup parent metadata: %v", err)
	}
	mtdt_joiner, _, err := joiner.New(ctx, d.store, mtdt_ref)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for metadata: %v", err)
	}
	md, err := fromMetadata(mtdt_joiner)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}
	//check if data is a directory
	if md.isDir {
		return nil, fmt.Errorf("Path to directory")
	}

	data_ref, err := d.lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup parent metadata: %v", err)
	}

	data_joiner, _, err := joiner.New(ctx, d.store, data_ref)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for metadata: %v", err)
	}

	buf, err := io.ReadAll(data_joiner)
	if err != nil {
		return nil, fmt.Errorf("failed reading metadata %w", err)
	}

	return buf, nil
}

func (d *swarmDriver) PutContent(ctx context.Context, path string, content []byte) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	fmt.Printf("PutContent hit for path %v", path)

	// Separating directory and filepath
	parentPath := filepath.Dir(path)
	contentPath := filepath.Base(path)

	// Get reference for new content
	splitter := splitter.NewSimpleSplitter(d.store)
	r2, err := splitter.Split(ctx, io.NopCloser(bytes.NewReader(content)), int64(len(content)), d.encrypt)
	if err != nil || isZeroAddress(r2) {
		return fmt.Errorf("failed to split content: %v", err)
	}

	// Publish ref for new content
	if err := d.publisher.Put(ctx, filepath.Join(contentPath, "data"), time.Now().Unix(), r2); err != nil {
		return fmt.Errorf("failed to publish new data reference: %v", err)
	}

	// Create metadata for content
	metaData := metaData{
		isDir:   false,
		Path:    path,
		ModTime: time.Now().Unix(),
	}
	// Convert metadata to Json
	mtdtBuf, err := json.Marshal(metaData)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}
	// Get reference for metadata

	r1, err := splitter.Split(ctx, io.NopCloser(bytes.NewReader(mtdtBuf)), int64(len(mtdtBuf)), d.encrypt)
	if err != nil || isZeroAddress(r1) {
		return fmt.Errorf("failed to split metadata: %v", err)
	}

	// Publish content metadata
	if err := d.publisher.Put(ctx, filepath.Join(contentPath, "mtdt"), time.Now().Unix(), r1); err != nil {
		return fmt.Errorf("failed to publish metadata reference: %v", err)
	}

	//Check if file path already exists in metadata of parent
	// Use lookuper to get metadata of parent
	r, err := d.lookuper.Get(ctx, filepath.Join(parentPath, "mtdt"), time.Now().Unix())
	if err != nil {
		return fmt.Errorf("failed to lookup parent metadata: %v", err)
	}
	joiner, _, err := joiner.New(ctx, d.store, r)
	if err != nil {
		return fmt.Errorf("failed to create reader for metadata: %v", err)
	}
	md, err := fromMetadata(joiner)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %v", err)
	}

	// check if file path in children of parent
	found := false
	for _, v := range md.children {
		if strings.Index(v, path) != -1 {
			found = true
			break
		}
	}
	// if file not in parent path then add
	if !found {
		md.children = append(md.children, path)
		md.ModTime = time.Now().Unix()

		mtdtParent, err := json.Marshal(md)
		if err != nil {
			return fmt.Errorf("failed to marshal parent metadata: %v", err)
		}

		r1, err = splitter.Split(ctx, io.NopCloser(bytes.NewReader(mtdtParent)), int64(len(mtdtParent)), d.encrypt)
		if err != nil || isZeroAddress(r1) {
			return fmt.Errorf("failed to split parent metadata: %v", err)
		}

		if err := d.publisher.Put(ctx, filepath.Join(parentPath, "mtdt"), time.Now().Unix(), r1); err != nil {
			return fmt.Errorf("failed to publish parent metadata reference: %v", err)
		}
	}

	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *swarmDriver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.reader(ctx, path, offset)
}

func (d *swarmDriver) reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	fmt.Println("reader:")
	fmt.Println(path)

	if offset < 0 {
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset}
	}

	// Lookup data reference for the given path
	dataRef, err := d.lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup data: %v", err)
	}

	// Create a joiner to read the data
	dataJoiner, _, err := joiner.New(ctx, d.store, dataRef)
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
	var buf []byte
	// Lookup new data at the specified path
	newDataRef, err := d.lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup new data: %v", err)
	}

	// Create a joiner to read the new data
	newDataJoiner, _, err := joiner.New(ctx, d.store, newDataRef)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for new data: %v", err)
	}

	// Read the new data into a buffer
	newData, err := io.ReadAll(newDataJoiner)
	if err != nil {
		return nil, fmt.Errorf("failed reading new data: %v", err)
	}

	if app {
		parentPath := filepath.Dir(path)
		// Lookup existing data at the specified path
		existingDataRef, err := d.lookuper.Get(ctx, filepath.Join(parentPath, "data"), time.Now().Unix())
		if err != nil {
			return nil, fmt.Errorf("failed to lookup existing data: %v", err)
		}

		// Create a joiner to read the existing data
		existingDataJoiner, _, err := joiner.New(ctx, d.store, existingDataRef)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader for existing data: %v", err)
		}

		// Read the existing data into a buffer
		existingData, err := io.ReadAll(existingDataJoiner)
		if err != nil {
			return nil, fmt.Errorf("failed reading existing data: %v", err)
		}

		// Combine the existing data with the new data
		buf = append(existingData, newData...)
	} else {
		// If not appending, just use the new data
		buf = newData

	}
	bufSize := int64(len(buf))
	// Split the combined or new data using the splitter
	splitter := splitter.NewSimpleSplitter(d.store)
	r2, err := splitter.Split(ctx, io.NopCloser(bytes.NewReader(buf)), bufSize, d.encrypt)
	if err != nil || isZeroAddress(r2) {
		return nil, fmt.Errorf("failed to split content: %v", err)
	}

	// Create a new FileWriter with the combined or new data
	return d.newWriter(r2, bufSize), nil
}

// Stat returns info about the provided path.
func (d *swarmDriver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	mtdt_ref, err := d.lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup parent metadata: %v", err)
	}
	mtdt_joiner, _, err := joiner.New(ctx, d.store, mtdt_ref)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for metadata: %v", err)
	}
	md, err := fromMetadata(mtdt_joiner)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}

	fi := storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   md.isDir,
		ModTime: time.Unix(md.ModTime, 0),
	}

	if !fi.IsDir {
		fi.Size = int64(md.Size)
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *swarmDriver) List(ctx context.Context, path string) ([]string, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	mtdt_ref, err := d.lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to lookup parent metadata: %v", err)
	}
	mtdt_joiner, _, err := joiner.New(ctx, d.store, mtdt_ref)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for metadata: %v", err)
	}
	md, err := fromMetadata(mtdt_joiner)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}

	if !md.isDir {
		return nil, fmt.Errorf("not a directory") // TODO(stevvooe): Need error type for this...
	}

	return md.children, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *swarmDriver) Move(ctx context.Context, sourcePath string, destPath string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	fmt.Println("Move:")
	fmt.Println(sourcePath)
	fmt.Println(destPath)
	normalizedSrc, normalizedDst := normalize(sourcePath), normalize(destPath)

	err := d.root.move(normalizedSrc, normalizedDst)
	switch err {
	case errNotExists:
		return storagedriver.PathNotFoundError{Path: destPath}
	default:
		return err
	}
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *swarmDriver) Delete(ctx context.Context, path string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
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
	reference swarm.Address
	bufSize   int64
	closed    bool
	committed bool
	cancelled bool
}

func (d *swarmDriver) newWriter(ref swarm.Address, bufSize int64) storagedriver.FileWriter {
	return &swarmFile{
		d:         d,
		reference: ref,
		bufSize:   bufSize,
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

	w.d.mutex.Lock()
	defer w.d.mutex.Unlock()
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
	w.d.mutex.RLock()
	defer w.d.mutex.RUnlock()

	return w.bufSize
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

	w.d.mutex.Lock()
	defer w.d.mutex.Unlock()

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
	w.d.mutex.Lock()
	defer w.d.mutex.Unlock()

	if _, err := w.f.WriteAt(w.buffer, int64(len(w.f.data))); err != nil {
		return err
	}
	w.buffer = []byte{}
	w.buffSize = 0

	return nil
}
