package bitcask

import (
	"io"
	"os"
)

type ConfigOpt int

type BitcaskError string

type key string

type fileLine string

type pendingWrites map[key]fileLine

const maxFileSize = 1024 * 1024

const (
    ReadOnly ConfigOpt = 0
    ReadWrite          = 1
    SyncOnPut          = 2
    SyncOnDemand       = 3
)

const (
    dirMode = os.FileMode(0700)
    fileMode = os.FileMode(0600)
)

const (
    keyDirFileName = "keydir"
    keyDirFileSeprator = " "
)

type Bitcask struct {
    directoryPath string
    keyDir map[key]record
    config options
    currentActive activeFile
    activeTstamp int64
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

func open(dirPath string, opts ...ConfigOpt) (*Bitcask, error) {
    bitcask := Bitcask{
    	directoryPath: dirPath,
        config: options{accessPermission: ReadOnly, syncOption: SyncOnDemand},
    }

    // parse user options
    for _, opt := range opts {
        switch opt {
        case ReadWrite:
            bitcask.config.accessPermission = ReadWrite
        case SyncOnPut:
            bitcask.config.syncOption = SyncOnPut
        }
    }

    // check if directory exists
    _, openErr := os.Open(dirPath)

    if openErr == nil {
        bitcask.buildKeyDir()
        bitcask.setCurrentTstamp()
    } else if os.IsNotExist(openErr) {
        os.MkdirAll(dirPath, dirMode)
        bitcask.keyDir = make(map[key]record)
        bitcask.activeTstamp = 1
        bitcask.createActiveFile()
    } else {
        return nil, openErr
    }

    return &bitcask, nil
}
