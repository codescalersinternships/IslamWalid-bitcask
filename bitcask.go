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

    // Maximum allowed pending writes.
    maxPendingWrites = 100

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
    directoryPath string
    lock string
    keyDirFile string
    keyDir map[string]record
    config options
    currentActive activeFile
    pendingWrites map[string]string
}

// activeFile represents the current active file that is used to append values.
type activeFile struct {
    file *os.File
    fileName string
    currentPos int64
    currentSize int64
}

// record represents the value of keydir map.
type record struct {
    fileId string
    valueSize int64
    valuePos int64
    tstamp int64
    isPending bool
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
    bitcask := Bitcask{
        keyDir: make(map[string]record),
        directoryPath: dirPath,
        config: options{writePermission: ReadOnly, syncOption: SyncOnDemand},
    }

    for _, opt := range opts {
        switch opt {
        case ReadWrite:
            bitcask.config.writePermission = ReadWrite
            bitcask.pendingWrites = make(map[string]string)
        case SyncOnPut:
            bitcask.config.syncOption = SyncOnPut
        }
    }

    bitcaskDir, openErr := os.Open(dirPath)
    defer bitcaskDir.Close()

    if openErr == nil {
        if bitcask.lockCheck() == writer {
            return nil, BitcaskError(WriterExist)
        }
        bitcask.buildKeyDir()
        if bitcask.config.writePermission == ReadOnly {
            bitcask.buildKeyDirFile()
            bitcask.lock = readLock + strconv.Itoa(int(time.Now().UnixMicro()))
            lockFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, bitcask.lock),
            os.O_CREATE, fileMode)
            lockFile.Close()
        } else {
            bitcask.lock = writeLock + strconv.Itoa(int(time.Now().UnixMicro()))
            lockFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, bitcask.lock),
            os.O_CREATE, fileMode)
            lockFile.Close()
            bitcask.createActiveFile()
        }

    } else if os.IsNotExist(openErr) {
        if bitcask.config.writePermission == ReadOnly {
            return nil, BitcaskError(CannotCreateBitcask)
        }
        os.MkdirAll(dirPath, dirMode)
        bitcask.keyDir = make(map[string]record)
        bitcask.createActiveFile()
        bitcask.lock = writeLock + strconv.Itoa(int(time.Now().UnixMicro()))
        lockFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, bitcask.lock),
        os.O_CREATE, fileMode)
        lockFile.Close()
    } else {
        return nil, BitcaskError(fmt.Sprintf("%s: %s", dirPath, CannotOpenThisDir))
    }
    return &bitcask, nil
}

// Get retrieves the value by key from a bitcask datastore.
// returns an error if key does not exist in the bitcask datastore.
func (bitcask *Bitcask) Get(key string) (string, error) {
    recValue, isExist := bitcask.keyDir[key]

    if !isExist {
        return "", BitcaskError(fmt.Sprintf("%s: %s", string(key), KeyDoesNotExist))
    }

    if recValue.isPending {
        _, value, _, _, _ := extractFileLine(bitcask.pendingWrites[key])
        return value, nil
    } else {
        buf := make([]byte, recValue.valueSize)
        file, _ := os.Open(path.Join(bitcask.directoryPath, recValue.fileId))
        file.ReadAt(buf, recValue.valuePos)
        file.Close()
        return string(buf), nil
    }
}

// Put stores a value by key in a bitcask datastore.
// Sync on each put if SyncOnPut option is set.
func (bitcask *Bitcask) Put(key string, value string) error {
    if bitcask.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    tstamp := time.Now().UnixMicro()
    bitcask.keyDir[key] = record{
        fileId:    "",
        valueSize: int64(len(value)),
        valuePos:  0,
        tstamp:    tstamp,
        isPending: true,
    }
    bitcask.addPendingWrite(key, value, tstamp)

    if bitcask.config.syncOption == SyncOnPut {
        bitcask.Sync()
    }

    return nil
}

// Delete removes a key from a bitcask datastore 
// by appending a special TompStone value that will be deleted in the next merge.
// returns an error if key does not exist in the bitcask datastore.
func (bitcask *Bitcask) Delete(key string) error {
    if bitcask.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    _, err := bitcask.Get(key)
    if err != nil {
        return err
    }

    delete(bitcask.keyDir, key)
    delete(bitcask.pendingWrites, key)

    return nil
}

// ListKeys list all keys in a bitcask datastore.
func (bitcask *Bitcask) ListKeys() []string {
    var list []string

    for key := range bitcask.keyDir {
        list = append(list, key)
    }

    return list
}

// Fold folds over all key/value pairs in a bitcask datastore.
// fun is expected to be in the form: F(K, V, Acc) -> Acc
func (bitcask *Bitcask) Fold(fun func(string, string, any) any, acc any) any {
    for key := range bitcask.keyDir {
        value, _ := bitcask.Get(key)
        acc = fun(key, value, acc)
    }
    return acc
}

// Merge rearrange the bitcask datastore in a more compact form.
// Also produces hintfiles to provide a faster startup.
// returns an error if ReadWrite permission is not set.
func (bitcask *Bitcask) Merge() error {
    if bitcask.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    var currentPos int64 = 0
    var currentSize int64 = 0
    newKeyDir := make(map[string]record)

    bitcask.Sync()

    bitcaskDir, _ := os.Open(bitcask.directoryPath)
    defer bitcaskDir.Close()
    bitcaskDirContent, _ := bitcaskDir.Readdir(0)

    mergeFileName := strconv.FormatInt(time.Now().UnixMicro(), 10)
    hintFileName := hintFilePrefix + mergeFileName

    mergeFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, mergeFileName),
    os.O_CREATE | os.O_RDWR, fileMode)

    hintFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, hintFileName),
    os.O_CREATE | os.O_RDWR, fileMode)

    for key, recValue := range bitcask.keyDir {
        if recValue.fileId != bitcask.currentActive.fileName {

            tstamp := time.Now().UnixMicro()
            value, _ := bitcask.Get(key)
            fileLine := string(compressFileLine(key, value, tstamp))

            if int64(len(fileLine)) + currentSize > maxFileSize {
                mergeFile.Close()
                hintFile.Close()

                mergeFileName = strconv.FormatInt(time.Now().UnixMicro(), 10)
                mergeFile, _ = os.OpenFile(path.Join(bitcask.directoryPath, mergeFileName),
                os.O_CREATE | os.O_RDWR, fileMode)

                hintFileName = hintFilePrefix + mergeFileName
                hintFile, _ = os.OpenFile(path.Join(bitcask.directoryPath, hintFileName),
                os.O_CREATE | os.O_RDWR, fileMode)

                currentPos = 0
                currentSize = 0
            }

            newKeyDir[key] = record{
                fileId:    mergeFileName,
                valueSize: int64(len(value)),
                valuePos:  currentPos + staticFields * numberFieldSize + int64(len(key)),
                tstamp:    tstamp,
                isPending: false,
            }

            hintFileLine := buildHintFileLine(newKeyDir[key], key)
            n, _ := fmt.Fprintln(mergeFile, fileLine)
            fmt.Fprintln(hintFile, hintFileLine)
            currentPos += int64(n)
            currentSize += int64(n)
        } else {
            newKeyDir[key] = bitcask.keyDir[key]
        }
    }

    bitcask.keyDir = newKeyDir
    mergeFile.Close()
    hintFile.Close()

    for _, file := range bitcaskDirContent {
        fileName := file.Name()
        // Skip lock and active files
        if !strings.HasPrefix(fileName, ".") && bitcask.currentActive.fileName != fileName {
            os.Remove(path.Join(bitcask.directoryPath, fileName))
        }
    }

    return nil
}

// Sync forces all pending writes to be written into disk.
// returns an error if ReadWrite permission is not set.
func (bitcask *Bitcask) Sync() error {
    if bitcask.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    for key, line := range bitcask.pendingWrites {
        if bitcask.keyDir[key].isPending {
            recValue := bitcask.keyDir[key]

            n := bitcask.writeToActiveFile(string(line))

            recValue.fileId = bitcask.currentActive.fileName
            recValue.valuePos = bitcask.currentActive.currentPos + staticFields * numberFieldSize + int64(len(key))
            recValue.isPending = false
            bitcask.keyDir[key] = recValue

            bitcask.currentActive.currentPos += n
            bitcask.currentActive.currentSize += n

            delete(bitcask.pendingWrites, key)
        }
    }

    return nil
}

// Close flushes all pending writes into disk and closes the bitcask datastore.
func (bitcask *Bitcask) Close() {
    if bitcask.config.writePermission == ReadWrite {
        bitcask.Sync()
        bitcask.currentActive.file.Close()
        os.Remove(path.Join(bitcask.directoryPath, bitcask.lock))
    } else {
        os.Remove(path.Join(bitcask.directoryPath, bitcask.keyDirFile))
        os.Remove(path.Join(bitcask.directoryPath, bitcask.lock))
    }
    bitcask = nil
}
