package swarmdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

var logger *slog.Logger

func init() {
	factory.Register(driverName, &swarmDriverFactory{})
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	logger = slog.New(handler)
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
	logger.Debug("Creating New Swarm Driver")
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
	d := &swarmDriver{
		Store:     store,
		Encrypt:   encrypt,
		Lookuper:  lk,
		Publisher: pb,
		Splitter:  splitter,
	}
	if err := d.addPathToRoot(context.Background(), ""); err != nil {
		logger.Error("New: Failed to create root path:")
	}
	logger.Debug("Swarm driver successfully created!")
	return d
}

// Implement the storagedriver.StorageDriver interface.
func (d *swarmDriver) Name() string {
	return driverName
}

func isValidPath(path string) bool {
	// A path is invalid if it's empty
	if path == "" {
		logger.Error("isValidPath: Invalid Path: Path should not be empty")
		return false
	}
	if path == "/" {
		logger.Error("isValidPath: Invalid Path: Path should not be /")
		return false
	}
	if !strings.HasPrefix(path, "/") {
		logger.Error("isValidPath: Invalid Path: Path should not contain / at the start")
		return false
	}
	if strings.HasSuffix(path, "/") {
		logger.Error("isValidPath: Invalid Path: Path should not contain / at the end")
		return false
	}
	// A path is invalid if it contains any invalid characters
	invalidChars := []string{"*", "?", "<", ">", "|", "\"", ":"}
	for _, char := range invalidChars {
		if strings.Contains(path, char) {
			logger.Error("isValidPath: Invalid Path: Path should not have contain invalid characters", slog.String("invalidChar", char))
			return false
		}
	}
	// A path is invalid if it contains double slashes
	if strings.Contains(path, "//") {
		logger.Error("isValidPath: Invalid Path: Path should not contain /")
		return false
	}
	logger.Info("isValidPath: Path Valid!", slog.String("path", path))
	return true
}

func (d *swarmDriver) addPathToRoot(ctx context.Context, path string) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	rootPath := "/"
	// Retrieve root metadata
	rootMeta, err := d.getMetadata(ctx, rootPath)
	if err != nil {
		// If root metadata does not exist, initialize it
		rootMeta = metaData{
			IsDir:    true,
			Path:     rootPath,
			ModTime:  time.Now().Unix(),
			Children: []string{},
		}
	}
	if path != "" {
		// Add the path to the root's children if it's not already present
		found := false
		for _, child := range rootMeta.Children {
			if child == path {
				found = true
				break
			}
		}
		if !found {
			rootMeta.Children = append(rootMeta.Children, path)
			rootMeta.ModTime = time.Now().Unix()
		}
	}
	metaBuf, err := json.Marshal(rootMeta)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to marshal metadata: %v", err)
	}
	metaRef, err := d.Splitter.Split(ctx, io.NopCloser(bytes.NewReader(metaBuf)), int64(len(metaBuf)), d.Encrypt)
	if err != nil || isZeroAddress(metaRef) {
		return fmt.Errorf("putMetadata: failed to split metadata: %v", err)
	}
	err = d.Publisher.Put(ctx, filepath.Join(rootPath, "mtdt"), time.Now().Unix(), metaRef)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to publish metadata: %v", err)
	}
	logger.Info("addPathToRoot: Success!", slog.String("path", path))
	return nil
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
	path = filepath.ToSlash(path)
	metaRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "mtdt"), time.Now().Unix())
	if err != nil {
		return metaData{}, storagedriver.InvalidPathError{Path: path, DriverName: d.Name()}
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
	logger.Info("PutMetadata Hit", slog.String("path", path))
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
	logger.Info("putMetadata: Success!", slog.String("path", path))
	logger.Info("putMetadata: Adding child paths to parents", slog.String("path", path))
	// Update metadata for each parent directory up to the root
	for currentPath := filepath.ToSlash(filepath.Dir(path)); ; currentPath = filepath.ToSlash(filepath.Dir(currentPath)) {
		parentMeta, err := d.getMetadata(ctx, currentPath)
		if err != nil {
			logger.Warn("putMetaData: Metadata not found. Creating new", slog.String("path", currentPath))
			parentMeta = metaData{
				IsDir:    true,
				Path:     currentPath,
				ModTime:  time.Now().Unix(),
				Children: []string{},
			}
		}
		found := false
		childPath := filepath.Base(path)
		for _, child := range parentMeta.Children {
			if child == childPath {
				found = true
				break
			}
		}
		if !found {
			parentMeta.Children = append(parentMeta.Children, childPath)
			parentMeta.ModTime = time.Now().Unix()
			parentMetaBuf, err := json.Marshal(parentMeta)
			if err != nil {
				return fmt.Errorf("putMetadata: failed to marshal parent metadata: %v", err)
			}
			parentMetaRef, err := d.Splitter.Split(ctx, io.NopCloser(bytes.NewReader(parentMetaBuf)), int64(len(parentMetaBuf)), d.Encrypt)
			if err != nil || isZeroAddress(parentMetaRef) {
				return fmt.Errorf("putMetadata: failed to split parent metadata: %v", err)
			}
			err = d.Publisher.Put(ctx, filepath.Join(currentPath, "mtdt"), time.Now().Unix(), parentMetaRef)
			if err != nil {
				return fmt.Errorf("putMetadata: failed to publish parent metadata: %v", err)
			}
			logger.Info("putMetaData: Updated parent metadata", slog.String("path", currentPath))
		}
		if currentPath == "/" {
			break
		}
		path = currentPath
	}

	return nil
}

func (d *swarmDriver) getData(ctx context.Context, path string) ([]byte, error) {
	dataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("getData: failed to lookup data: %v", err)
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
	logger.Info("putData Hit!", slog.String("path", path))
	if len(data) == 0 {
		emptyRef := swarm.NewAddress(nil)
		err := d.Publisher.Put(ctx, filepath.Join(path, "data"), time.Now().Unix(), emptyRef)
		if err != nil {
			return fmt.Errorf("putData: failed to publish empty data reference: %v", err)
		}
		return nil
	}
	dataRef, err := d.Splitter.Split(ctx, io.NopCloser(bytes.NewReader(data)), int64(len(data)), d.Encrypt)
	if err != nil || isZeroAddress(dataRef) {
		return fmt.Errorf("putData: failed to split data: %v", err)
	}
	err = d.Publisher.Put(ctx, filepath.Join(path, "data"), time.Now().Unix(), dataRef)
	if err != nil {
		return fmt.Errorf("putData: failed to publish data reference: %v", err)
	}
	logger.Info("putData: Success!", slog.String("path", path))
	return nil
}

func (d *swarmDriver) deleteData(ctx context.Context, path string) error {
	dataRefPath := filepath.Join(path, "data")
	err := d.Publisher.Put(ctx, dataRefPath, time.Now().Unix(), swarm.ZeroAddress) // Using a ZeroAddress to represent deletion
	if err != nil {
		return fmt.Errorf("deleteData: failed to nullify data reference for path %s: %v", path, err)
	}
	logger.Info("deleteData: Successfully nullified data reference", slog.String("path", path))
	return nil
}

func (d *swarmDriver) deleteMetadata(ctx context.Context, path string) error {
	metadataRefPath := filepath.Join(path, "mtdt")
	err := d.Publisher.Put(ctx, metadataRefPath, time.Now().Unix(), swarm.ZeroAddress) // Using a ZeroAddress to represent deletion
	if err != nil {
		return fmt.Errorf("deleteMetadata: failed to nullify metadata for path %s: %v", path, err)
	}
	logger.Info("deleteMetadata: Successfully nullified metadata reference", slog.String("path", path))
	return nil
}

func (d *swarmDriver) childExists(ctx context.Context, path string) bool {
	logger.Info("childExists Hit", slog.String("path", path))
	if path == "/" {
		return true
	}
	parentPath := filepath.ToSlash(filepath.Dir(path))
	childPath := filepath.Base(path)
	parentMtdt, err := d.getMetadata(ctx, parentPath)
	if err != nil {
		return false
	}
	found := false
	for _, child := range parentMtdt.Children {
		if child == childPath {
			found = true
			break
		}
	}
	if !found {
		return false
	}
	return true
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *swarmDriver) GetContent(ctx context.Context, path string) ([]byte, error) {

	d.Mutex.RLock()
	defer d.Mutex.RUnlock()
	logger.Info("GetContent Hit", slog.String("path", path))
	if !(isValidPath(path)) {
		return nil, storagedriver.InvalidPathError{DriverName: d.Name()}
	}
	if !(d.childExists(ctx, path)) {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Fetch metadata using the helper function
	mtdt, err := d.getMetadata(ctx, path)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Check if data is a directory
	if mtdt.IsDir {
		return nil, storagedriver.InvalidPathError{DriverName: d.Name()}
	}
	// Fetch data using the helper function
	data, err := d.getData(ctx, path)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}

	logger.Info("GetContent: Success!", slog.String("path", path))

	return data, nil
}

func (d *swarmDriver) PutContent(ctx context.Context, path string, content []byte) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	logger.Info("PutContent Hit", slog.String("path", path))
	if !(isValidPath(path)) {
		return storagedriver.InvalidPathError{Path: path, DriverName: d.Name()}
	}
	// Split the content to get a data reference
	if err := d.putData(ctx, path, content); err != nil {
		logger.Debug("PutContent: putData Failed!", slog.String("path", path))
		return storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Create and store metadata for the new content
	mtdt := metaData{
		IsDir:   false,
		Path:    path,
		ModTime: time.Now().Unix(),
		Size:    len(content),
	}
	logger.Info("PutContent: Initiating Put for MetaData", slog.String("path", path))
	if err := d.putMetadata(ctx, path, mtdt); err != nil {
		logger.Info("PutContent: putMetaData Failed!", slog.String("path", path))
		return storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	logger.Info("PutContent: Success!", slog.String("path", path))
	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *swarmDriver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	d.Mutex.RLock()
	defer d.Mutex.RUnlock()
	logger.Info("Reader Hit", slog.String("path", path))
	if offset < 0 {
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset, DriverName: d.Name()}
	}
	if !(d.childExists(ctx, path)) {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Lookup data reference for the given path
	dataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Create a joiner to read the data
	dataJoiner, _, err := joiner.New(ctx, d.Store, dataRef)
	if err != nil {
		return nil, fmt.Errorf("Reader: failed to create joiner: %v", err)
	}
	// Seek to the specified offset
	if _, err := dataJoiner.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("Reader: failed to seek to offset %d: %v", offset, err)
	}
	logger.Info("Reader: Success", slog.String("path", path))
	return io.NopCloser(dataJoiner), nil
}

// Stat returns info about the provided path.
func (d *swarmDriver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	d.Mutex.RLock()
	defer d.Mutex.RUnlock()
	logger.Info("Stat Hit", slog.String("path", path))
	// Fetch metadata using the helper function
	mtdt, err := d.getMetadata(ctx, path)
	if err != nil {
		logger.Info("Stat: Failed to lookup Metadata path", slog.String("path", path))
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
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
	fmt.Println(fi)
	logger.Info("Stat: Success!", slog.String("path", path))
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *swarmDriver) List(ctx context.Context, path string) ([]string, error) {
	d.Mutex.RLock()
	defer d.Mutex.RUnlock()
	logger.Info("List Hit", slog.String("path", path))
	if !(d.childExists(ctx, path)) {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
	}
	// Fetch metadata using the helper function
	mtdt, err := d.getMetadata(ctx, path)
	if err != nil {
		logger.Error("List: Failed to lookup Metadata path", slog.String("path", path))
		return nil, storagedriver.PathNotFoundError{Path: filepath.ToSlash(path), DriverName: d.Name()}
	}
	// Ensure it's a directory
	if !mtdt.IsDir {
		logger.Info("List: Not a directory", slog.String("path", path))
		return nil, storagedriver.InvalidPathError{Path: filepath.ToSlash(path), DriverName: d.Name()}
	}
	// Ensure children are not nil
	if len(mtdt.Children) == 0 {
		logger.Info("List: This path has no children", slog.String("path", path))
	}
	children := []string{}
	for _, child := range mtdt.Children {
		children = append(children, filepath.Join(path, child))
	}
	return children, nil
}

func (d *swarmDriver) Delete(ctx context.Context, path string) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	logger.Debug("Delete Hit", slog.String("path", path))

	if path != "/" {
		// Remove the path from the parent's children
		parentPath := filepath.ToSlash(filepath.Dir(path))
		childPath := filepath.Base(path)
		parentMeta, err := d.getMetadata(ctx, parentPath)
		if err != nil {
			logger.Error("Delete: Failed to get parent Metadata", slog.String("childPath", parentPath))
			return storagedriver.PathNotFoundError{DriverName: d.Name(), Path: parentPath}
		}
		parentMeta.Children = removeFromSlice(parentMeta.Children, childPath)
		if err := d.putMetadata(ctx, parentPath, parentMeta); err != nil {
			return fmt.Errorf("failed to update parent metadata: %v", err)
		}
	}
	
	// Delete data and metadata
	if err := d.deleteData(ctx, path); err != nil {
		return fmt.Errorf("failed to delete data: %v", err)
	}
	if err := d.deleteMetadata(ctx, path); err != nil {
		return fmt.Errorf("failed to delete metadata: %v", err)
	}
	logger.Info("Successfully deleted path", slog.String("path", path))
	return nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
func (d *swarmDriver) Move(ctx context.Context, sourcePath string, destPath string) error {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	logger.Info("Move Hit", slog.String("sourcePath", sourcePath), slog.String("destPath", destPath))
	// 1. Lookup and read source metadata
	sourceMeta, err := d.getMetadata(ctx, sourcePath)
	if err != nil {
		logger.Error("Move: Failed to lookup source Metadata path", slog.String("path", sourcePath))
		return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: d.Name()}
	}
	// 2. Remove entry from the source parent
	sourceParentPath := filepath.ToSlash(filepath.Dir(sourcePath))
	sourceParentMeta, err := d.getMetadata(ctx, sourceParentPath)
	if err != nil {
		logger.Error("Move: Failed to get source parent Metadata", slog.String("path", sourcePath))
		return storagedriver.PathNotFoundError{Path: sourceParentPath, DriverName: d.Name()}
	}
	sourceParentMeta.Children = removeFromSlice(sourceParentMeta.Children, filepath.Base(sourcePath))
	logger.Info("Move: Initiating Put for MetaData", slog.String("path", sourceParentPath))
	if err := d.putMetadata(ctx, sourceParentPath, sourceParentMeta); err != nil {
		return fmt.Errorf("Move: failed to update source parent metadata: %v", err)
	}
	// 3. Add entry to the destination parent
	destParentPath := filepath.ToSlash(filepath.Dir(destPath))
	destParentMeta, err := d.getMetadata(ctx, destParentPath)
	if err != nil {
		logger.Info("Move: Destination parent not found, creating new metadata", slog.String("path", destParentPath))
		destParentMeta = metaData{
			IsDir:    true,
			Path:     destParentPath,
			ModTime:  time.Now().Unix(),
			Children: []string{},
		}
	}
	destParentMeta.Children = append(destParentMeta.Children, filepath.Base(destPath))
	logger.Info("Move: Initiating Put for MetaData at destination", slog.String("path", destParentPath))
	if err := d.putMetadata(ctx, destParentPath, destParentMeta); err != nil {
		return fmt.Errorf("Move: failed to update destination parent metadata: %v", err)
	}
	// 4. Update metadata to the new destination path
	sourceMeta.Path = destPath
	logger.Info("Move: Initiating Put for MetaData", slog.String("path", destPath))
	if err := d.putMetadata(ctx, destPath, sourceMeta); err != nil {
		return fmt.Errorf("Move: failed to update metadata at destination path: %v", err)
	}
	err = d.moveDataRecursively(ctx, sourcePath, destPath)
	if err != nil {
		return storagedriver.PathNotFoundError{Path: destParentPath, DriverName: d.Name()}
	}

	return nil
}

func (d *swarmDriver) moveDataRecursively(ctx context.Context, sourcePath, destPath string) error {
	// Get metadata of the source path
	sourceMetadata, err := d.getMetadata(ctx, sourcePath)
	if err != nil {
		logger.Error("Move: Failed to lookup source metadata", slog.String("path", sourcePath))
		return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: d.Name()}
	}

	// Update the metadata's path field to reflect the new destination
	sourceMetadata.Path = destPath

	// Move the data reference for the current path
	dataRef, err := d.Lookuper.Get(ctx, filepath.Join(sourcePath, "data"), time.Now().Unix())
	if err != nil {
		logger.Error("Move: Failed to get data reference", slog.String("path", sourcePath))
		return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: d.Name()}
	}

	// Publish data reference to destination
	err = d.Publisher.Put(ctx, filepath.Join(destPath, "data"), time.Now().Unix(), dataRef)
	if err != nil {
		return fmt.Errorf("Move: failed to publish data reference to destination: %v", err)
	}
	logger.Info("Move: Data reference moved", slog.String("source", sourcePath), slog.String("destination", destPath))

	metaBuf, err := json.Marshal(sourceMetadata)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to marshal metadata: %v", err)
	}
	// Publish the updated metadata to the destination
	metaRef, err := d.Splitter.Split(ctx, io.NopCloser(bytes.NewReader(metaBuf)), int64(len(metaBuf)), d.Encrypt)
	if err != nil || isZeroAddress(metaRef) {
		return fmt.Errorf("putMetadata: failed to split metadata: %v", err)
	}
	err = d.Publisher.Put(ctx, filepath.Join(destPath, "mtdt"), time.Now().Unix(), metaRef)
	if err != nil {
		return fmt.Errorf("putMetadata: failed to publish metadata: %v", err)
	}

	// Recursively handle children
	for _, child := range sourceMetadata.Children {
		sourceChildPath := filepath.Join(sourcePath, child)
		destChildPath := filepath.Join(destPath, child)

		// Recursively move each child
		err := d.moveDataRecursively(ctx, sourceChildPath, destChildPath)
		if err != nil {
			return fmt.Errorf("Move: failed to move child data from %s to %s: %v", sourceChildPath, destChildPath, err)
		}
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
	logger.Info("RedirectURL Hit")
	return "", nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file and directory
func (d *swarmDriver) Walk(ctx context.Context, path string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	logger.Info("Walk Hit", slog.String("path", path))
	return nil
}

type swarmFile struct {
	d         *swarmDriver
	path      string
	buffer    *bytes.Buffer
	closed    bool
	committed bool
	cancelled bool
	offset    int64
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *swarmDriver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	logger.Info("Writer Hit", slog.String("path", path))

	var combinedData bytes.Buffer
	w := &swarmFile{
		d:         d,
		path:      path,
		closed:    false,
		committed: false,
		cancelled: false,
	}

	if append {
		logger.Info("Writer: Append True", slog.String("path", path))
		// Lookup existing data at the specified path
		oldDataRef, err := d.Lookuper.Get(ctx, filepath.Join(path, "data"), time.Now().Unix())
		if err != nil {
			logger.Error("Writer: Append: Failed to fetch data", slog.String("path", path))
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
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

func (w *swarmFile) Write(p []byte) (int, error) {

	w.d.Mutex.Lock()
	defer w.d.Mutex.Unlock()

	// logger.Info("Write Hit", slog.String("path", w.path))

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

	logger.Info("Size Hit", slog.String("path", w.path))

	return int64(w.buffer.Len())
}

func (w *swarmFile) Close() error {

	w.d.Mutex.Lock()
	defer w.d.Mutex.Unlock()

	logger.Info("Close Hit", slog.String("path", w.path))

	if w.closed {
		return fmt.Errorf("Close: already closed")
	}
	w.closed = true

	return nil
}

func (w *swarmFile) Cancel(ctx context.Context) error {

	logger.Info("Cancel Hit", slog.String("path", w.path))

	if w.closed {
		return fmt.Errorf("Cancel: already closed")
	} else if w.committed {
		return fmt.Errorf("Cancel: already committed")
	}
	w.cancelled = true

	w = nil

	return nil
}

func (w *swarmFile) Commit(ctx context.Context) error {
	w.d.Mutex.Lock()
	defer w.d.Mutex.Unlock()

	logger.Info("Commit Hit", slog.String("path", w.path))

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
	logger.Info("Commit: Data committed", slog.String("path", w.path))

	// Create metadata for the committed content
	meta := metaData{
		IsDir:   false,
		Path:    w.path,
		ModTime: time.Now().Unix(),
		Size:    w.buffer.Len(),
	}

	// Publish the metadata using helper function
	logger.Info("Commit: Initiating Put for MetaData", slog.String("path", w.path))
	if err := w.d.putMetadata(ctx, w.path, meta); err != nil {
		return fmt.Errorf("Commit: failed to publish metadata reference: %v", err)
	}
	logger.Info("Commit: Metadata committed", slog.String("path", w.path))

	// Reset the buffer after committing data and metadata
	w.buffer.Reset()

	// Mark the file as committed
	w.committed = true

	logger.Info("Commit: Successfully committed data and metadata", slog.String("path", w.path))
	return nil
}
