package bitcask

import (
	"fmt"
	"io"
	"os"
	"path"
)

const maxFileSize = 1024 * 1024

const (
    ReadOnly ConfigOpt = 0
    ReadWrite          = 1
    SyncOnPut          = 2
    SyncOnDemand       = 3

    dirMode = os.FileMode(0700)
    fileMode = os.FileMode(0600)

    keyDirFileName = "keydir"
    keyDirFileSeprator = " "

    tstampOffset = 0
    keySizeOffset = 19
    valueSizeOffset = 38
    numberFieldSize = 19

    keyDoesNotExist = "key does not exist"
    cannotOpenThisDir = "cannot open this directory"
)

type ConfigOpt int

type BitcaskError string

type key string

type fileLine string

type Bitcask struct {
    directoryPath string
    keyDir map[key]record
    config options
    currentActive activeFile
    pendingWrites map[key]fileLine
}

type activeFile struct {
    file io.Writer
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
    accessPermission ConfigOpt
    syncOption ConfigOpt
}

func (e BitcaskError) Error() string {
    return string(e)
}

func Open(dirPath string, opts ...ConfigOpt) (*Bitcask, error) {
    bitcask := Bitcask{
    	directoryPath: dirPath,
        config: options{accessPermission: ReadOnly, syncOption: SyncOnDemand},
    }

    // parse user options
    for _, opt := range opts {
        switch opt {
        case ReadWrite:
            bitcask.config.accessPermission = ReadWrite
            bitcask.pendingWrites = make(map[key]fileLine)
        case SyncOnPut:
            bitcask.config.syncOption = SyncOnPut
        }
    }

    // check if directory exists
    _, openErr := os.Open(dirPath)

    if openErr == nil {
        bitcask.buildKeyDir()
    } else if os.IsNotExist(openErr) {
        os.MkdirAll(dirPath, dirMode)
        bitcask.keyDir = make(map[key]record)
        bitcask.createActiveFile()
    } else {
        return nil, BitcaskError(fmt.Sprintf("%s: %s", dirPath, cannotOpenThisDir))
    }
    return &bitcask, nil
}

func (bitcask *Bitcask) Get(key key) (string, error) {
    recValue, isExist := bitcask.keyDir[key]

    if !isExist {
        return "", BitcaskError(fmt.Sprintf("%s: %s", string(key), keyDoesNotExist))
    }

    if recValue.isPending {
        _, value, _, _, _ := extractFileLine(bitcask.pendingWrites[key])
        return value, nil
    } else {
        buf := make([]byte, recValue.valueSize)
        file, _ := os.Open(path.Join(bitcask.directoryPath, recValue.fileId))
        file.ReadAt(buf, recValue.valuePos)
        return string(buf), nil
    }
}
