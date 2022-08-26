package bitcask

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// Maximum file size 10KB.
const maxFileSize = 10 * 1024

const (
    // ReadOnly constant give the bitcask process read only permission.
    ReadOnly     ConfigOpt = 0
    // ReadWrite constant allow the bitcask process to both read and write.
    ReadWrite    ConfigOpt = 1
    // SyncOnPut makes the bitcask sync the writes to the files after each put.
    SyncOnPut    ConfigOpt = 2
    // SyncOnDemand makes the bitcask sync when user calls the sync function.
    SyncOnDemand ConfigOpt = 3

    // Error message when key not found when deleting or getting it.
    KeyDoesNotExist = "key does not exist"
    // Error message when the directory cannot be openned for any reason.
    CannotOpenThisDir = "cannot open this directory"
    // Error message when a read only process try to write.
    WriteDenied = "write permission denied"
    // Error message when a read only process try to create a new bitcask datastore.
    CannotCreateBitcask = "read only cannot create new bitcask datastore"
    // Error message when a process try to access a bitcask with writer process holding it.
    WriterExist = "another writer exists in this bitcask"
)

const (
    // Default file mode.
    dirMode = os.FileMode(0777)
    // Default directory mode.
    fileMode = os.FileMode(0666)

    // Prefix used in keydir file name.
    keyDirFilePrefix = "keydir"
    // Prefix used in hintfile names.
    hintFilePrefix = "hintfile"

    // Number and size of fields of file line with constant size.
    staticFields = 3
    numberFieldSize = 19

    // Constant to determine the process in the bitcask is a reader.
    reader      processAccess = 0
    // Constant to determine the process in the bitcask is a writer.
    writer      processAccess = 1
    // Constant to determine there is no process in the bitcask.
    noProcess   processAccess = 2

    // Prefix to read only process lock.
    readLock = ".readlock"
    // Prefix to read and write process lock.
    writeLock = ".writelock"

    // Value to distinguish the deleted values.
    tompStone = "DELETE THIS VALUE"
)

// ConfigOpt is a type to the config option constants the user pass to open.
type ConfigOpt int

// processAccess is a type for determining the access permission of existing process.
type processAccess int

// BitcaskError represents the type or errors can occur while process is running on a bitcask.
type BitcaskError string

// Bitcask contains the data needed to manipulate the bitcask datastore.
// user creates an object of it to use the bitcask.
type Bitcask struct {
    datastorePath string
    lock string
    keyDirFile string
    keyDir map[string]record
    config options
    activeFile datastoreFile
}

// datastoreFile represents the current active file that is used to append values.
type datastoreFile struct {
    file *os.File
    fileName string
    currentPos int
    currentSize int
}

// record represents the value of keydir map.
type record struct {
    fileId string
    valueSize int
    valuePos int
    tstamp int
}

// options groups the config options passed to Open.
type options struct {
    writePermission ConfigOpt
    syncOption ConfigOpt
}

// Implement error interface.
func (e BitcaskError) Error() string {
    return string(e)
}

// Open creates a new process to manipulate the given bitcask datastore path.
// It takes options ReadWrite, ReadOnly, SyncOnPut and SyncOnDemand.
// Only one ReadWrite process can open a bitcask at a time.
// Only ReadWrite permission can create a new bitcask datastore.
// If there is no bitcask datastore in the given path a new datastore is created when ReadWrite permission is given.
func Open(dirPath string, opts ...ConfigOpt) (*Bitcask, error) {
    var openErr error

    bitcask := Bitcask{
        keyDir: make(map[string]record),
        datastorePath: dirPath,
        config: options{writePermission: ReadOnly, syncOption: SyncOnDemand},
    }

    for _, opt := range opts {
        switch opt {
        case ReadWrite:
            bitcask.config.writePermission = ReadWrite
        case SyncOnPut:
            bitcask.config.syncOption = SyncOnPut
        }
    }

    bitcaskDir, pathErr := os.Open(dirPath)
    defer bitcaskDir.Close()

    if pathErr == nil {
        openErr = bitcask.openExistingDatastore()
    } else if os.IsNotExist(pathErr) {
        openErr = bitcask.createNewDatastore()
    } else {
        openErr = BitcaskError(fmt.Sprintf("%s: %s", dirPath, CannotOpenThisDir))
    }

    if openErr == nil {
        return &bitcask, nil
    } else {
        return nil, openErr
    }
}

// Get retrieves the value by key from a bitcask datastore.
// returns an error if key does not exist in the bitcask datastore.
func (b *Bitcask) Get(key string) (string, error) {
    rec, isExist := b.keyDir[key]

    if !isExist {
        return "", BitcaskError(fmt.Sprintf("%s: %s", string(key), KeyDoesNotExist))
    }

    buf := make([]byte, rec.valueSize)
    file, _ := os.Open(path.Join(b.datastorePath, rec.fileId))
    defer file.Close()
    file.ReadAt(buf, int64(rec.valuePos))
    return string(buf), nil
}

// Put stores a value by key in a bitcask datastore.
// Sync on each put if SyncOnPut option is set.
func (b *Bitcask) Put(key string, value string) error {
    if b.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    tstamp := int(time.Now().UnixMicro())
    n, err := b.writeToActiveFile(string(compressFileLine(key, value, tstamp)))
    if err != nil {
        return err
    }

    b.keyDir[key] = record{
        fileId:    b.activeFile.fileName,
        valueSize: len(value),
        valuePos:  b.activeFile.currentPos + staticFields * numberFieldSize + len(key),
        tstamp:    int(tstamp),
    }

    b.activeFile.currentPos += n
    b.activeFile.currentSize += n

    if b.config.syncOption == SyncOnPut {
        b.Sync()
    }

    return nil
}

// Delete removes a key from a bitcask datastore 
// by appending a special TompStone value that will be deleted in the next merge.
// returns an error if key does not exist in the bitcask datastore.
func (b *Bitcask) Delete(key string) error {
    if b.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    _, err := b.Get(key)
    if err != nil {
        return err
    }

    delete(b.keyDir, key)

    return nil
}

// ListKeys list all keys in a bitcask datastore.
func (b *Bitcask) ListKeys() []string {
    var list []string

    for key := range b.keyDir {
        list = append(list, key)
    }

    return list
}

// Fold folds over all key/value pairs in a bitcask datastore.
// fun is expected to be in the form: F(K, V, Acc) -> Acc
func (b *Bitcask) Fold(fun func(string, string, any) any, acc any) any {
    for key := range b.keyDir {
        value, _ := b.Get(key)
        acc = fun(key, value, acc)
    }
    return acc
}

// Merge rearrange the bitcask datastore in a more compact form.
// Also produces hintfiles to provide a faster startup.
// returns an error if ReadWrite permission is not set.
func (b *Bitcask) Merge() error {
    if b.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    var currentPos int = 0
    var currentSize int = 0
    newKeyDir := make(map[string]record)

    b.Sync()

    bitcaskDir, _ := os.Open(b.datastorePath)
    defer bitcaskDir.Close()
    bitcaskDirContent, _ := bitcaskDir.Readdir(0)

    mergeFileName := strconv.FormatInt(time.Now().UnixMicro(), 10)
    hintFileName := hintFilePrefix + mergeFileName

    mergeFile, _ := os.OpenFile(path.Join(b.datastorePath, mergeFileName),
    os.O_CREATE | os.O_RDWR, fileMode)

    hintFile, _ := os.OpenFile(path.Join(b.datastorePath, hintFileName),
    os.O_CREATE | os.O_RDWR, fileMode)

    for key, recValue := range b.keyDir {
        if recValue.fileId != b.activeFile.fileName {

            tstamp := time.Now().UnixMicro()
            value, _ := b.Get(key)
            fileLine := string(compressFileLine(key, value, int(tstamp)))

            if len(fileLine) + currentSize > maxFileSize {
                mergeFile.Close()
                hintFile.Close()

                mergeFileName = strconv.FormatInt(time.Now().UnixMicro(), 10)
                mergeFile, _ = os.OpenFile(path.Join(b.datastorePath, mergeFileName),
                os.O_CREATE | os.O_RDWR, fileMode)

                hintFileName = hintFilePrefix + mergeFileName
                hintFile, _ = os.OpenFile(path.Join(b.datastorePath, hintFileName),
                os.O_CREATE | os.O_RDWR, fileMode)

                currentPos = 0
                currentSize = 0
            }

            newKeyDir[key] = record{
                fileId:    mergeFileName,
                valueSize: len(value),
                valuePos:  currentPos + staticFields * numberFieldSize + len(key),
                tstamp:    int(tstamp),
            }

            hintFileLine := buildHintFileLine(newKeyDir[key], key)
            n, _ := fmt.Fprintln(mergeFile, fileLine)
            fmt.Fprintln(hintFile, hintFileLine)
            currentPos += n
            currentSize += n
        } else {
            newKeyDir[key] = b.keyDir[key]
        }
    }

    b.keyDir = newKeyDir
    mergeFile.Close()
    hintFile.Close()

    for _, file := range bitcaskDirContent {
        fileName := file.Name()
        // Skip lock and active files
        if !strings.HasPrefix(fileName, ".") && b.activeFile.fileName != fileName {
            os.Remove(path.Join(b.datastorePath, fileName))
        }
    }

    return nil
}

// Sync forces all pending writes to be written into disk.
// returns an error if ReadWrite permission is not set.
func (b *Bitcask) Sync() error {
    if b.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    err := b.activeFile.file.Sync()
    if err != nil {
        return err
    }

    return nil
}

// Close flushes all pending writes into disk and closes the bitcask datastore.
func (b *Bitcask) Close() {
    if b.config.writePermission == ReadWrite {
        b.Sync()
        b.activeFile.file.Close()
        os.Remove(path.Join(b.datastorePath, b.lock))
    } else {
        os.Remove(path.Join(b.datastorePath, b.keyDirFile))
        os.Remove(path.Join(b.datastorePath, b.lock))
    }
    b = nil
}
