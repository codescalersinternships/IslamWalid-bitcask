package bitcask

import (
    "fmt"
    "os"
    "path"
    "strconv"
    "time"
)

const maxFileSize = 10 * 1024

const (
    ReadOnly     ConfigOpt = 0
    ReadWrite    ConfigOpt = 1
    SyncOnPut    ConfigOpt = 2
    SyncOnDemand ConfigOpt = 3

    KeyDoesNotExist = "key does not exist"
    CannotOpenThisDir = "cannot open this directory"
    WriteDenied = "write permission denied"
    CannotCreateBitcask = "read only cannot create new bitcask directory"
    WriterExist = "another writer exists in this bitcask"
)

const (
    dirMode = os.FileMode(0700)
    fileMode = os.FileMode(0600)

    keyDirFilePrefix = "keydir"
    hintFilePrefix = "hintfile"

    staticFields = 3
    numberFieldSize = 19

    reader      processAccess = 0
    writer      processAccess = 1
    noProcess   processAccess = 2

    readLock = ".readlock"
    writeLock = ".writelock"

    maxPendingWrites = 100

    tompStone = "DELETE THIS VALUE"
)

type ConfigOpt int

type processAccess int

type BitcaskError string

type Bitcask struct {
    directoryPath string
    lock string
    keyDirFile string
    keyDir map[string]record
    config options
    currentActive activeFile
    pendingWrites map[string]string
}

type activeFile struct {
    file *os.File
    fileName string
    currentPos int64
    currentSize int64
}

type record struct {
    fileId string
    valueSize int64
    valuePos int64
    tstamp int64
    isPending bool
}

type options struct {
    writePermission ConfigOpt
    syncOption ConfigOpt
}

func (e BitcaskError) Error() string {
    return string(e)
}

func Open(dirPath string, opts ...ConfigOpt) (*Bitcask, error) {
    bitcask := Bitcask{
        keyDir: make(map[string]record),
        directoryPath: dirPath,
        config: options{writePermission: ReadOnly, syncOption: SyncOnDemand},
    }

    // parse user options
    for _, opt := range opts {
        switch opt {
        case ReadWrite:
            bitcask.config.writePermission = ReadWrite
            bitcask.pendingWrites = make(map[string]string)
        case SyncOnPut:
            bitcask.config.syncOption = SyncOnPut
        }
    }

    // check if directory exists
    _, openErr := os.Open(dirPath)

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

func (bitcask *Bitcask) ListKeys() []string {
    var list []string

    for key := range bitcask.keyDir {
        list = append(list, key)
    }

    return list
}

func (bitcask *Bitcask) Fold(fun func(string, string, any) any, acc any) any {
    for key := range bitcask.keyDir {
        value, _ := bitcask.Get(key)
        acc = fun(key, value, acc)
    }
    return acc
}

func (bitcask *Bitcask) Merge() error {
    if bitcask.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    var currentPos int64 = 0
    var currentSize int64 = 0
    var oldFiles []string
    newKeyDir := make(map[string]record)

    mergeFileName := strconv.FormatInt(time.Now().UnixMicro(), 10)
    hintFileName := hintFilePrefix + mergeFileName
    bitcask.Sync()

    mergeFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, mergeFileName),
    os.O_CREATE | os.O_RDWR, fileMode)

    hintFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, hintFileName),
    os.O_CREATE | os.O_RDWR, fileMode)

    for key, recValue := range bitcask.keyDir {
        if recValue.fileId != bitcask.currentActive.fileName {

            tstamp := time.Now().UnixMicro()
            oldFiles = append(oldFiles, recValue.fileId)
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
        }
    }

    bitcask.keyDir = newKeyDir
    mergeFile.Close()
    hintFile.Close()

    for _, file := range oldFiles {
        os.Remove(path.Join(bitcask.directoryPath, file))
        os.Remove(path.Join(bitcask.directoryPath, hintFilePrefix + file))
    }

    return nil
}

func (bitcask *Bitcask) Sync() error {
    if bitcask.config.writePermission == ReadOnly {
        return BitcaskError(WriteDenied)
    }

    for key, line := range bitcask.pendingWrites {
        if bitcask.keyDir[key].isPending {
            activeFileInfo, _ := bitcask.currentActive.file.Stat()

            recValue := bitcask.keyDir[key]
            recValue.fileId = activeFileInfo.Name()
            recValue.valuePos = bitcask.currentActive.currentPos + staticFields * numberFieldSize + int64(len(key))
            recValue.isPending = false
            bitcask.keyDir[key] = recValue
            bitcask.writeToActiveFile(string(line))
        }
    }

    return nil
}

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
