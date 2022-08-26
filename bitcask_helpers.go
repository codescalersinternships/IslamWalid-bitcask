package bitcask

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// openExistingDatastore opens an existing bitcask datastore.
func (b *Bitcask) openExistingDatastore() error {
    if b.lockCheck() == writer {
        return BitcaskError(WriterExist)
    }

    b.buildKeyDir()

    if b.config.writePermission == ReadOnly {
        b.buildKeyDirFile()
        b.lock = readLock + strconv.Itoa(int(time.Now().UnixMicro()))
        lockFile, _ := os.OpenFile(path.Join(b.datastorePath, b.lock),
        os.O_CREATE, fileMode)
        lockFile.Close()
    } else {
        b.lock = writeLock + strconv.Itoa(int(time.Now().UnixMicro()))
        lockFile, _ := os.OpenFile(path.Join(b.datastorePath, b.lock),
        os.O_CREATE, fileMode)
        lockFile.Close()
        b.createActiveFile()
    }

    return nil
}

// createNewDatastore builds new bitcask datastore.
func (b *Bitcask) createNewDatastore() error {
    if b.config.writePermission == ReadOnly {
        return BitcaskError(CannotCreateBitcask)
    }

    os.MkdirAll(b.datastorePath, dirMode)
    b.keyDir = make(map[string]record)
    b.createActiveFile()
    b.lock = writeLock + strconv.Itoa(int(time.Now().UnixMicro()))
    lockFile, _ := os.OpenFile(path.Join(b.datastorePath, b.lock),
    os.O_CREATE, fileMode)
    lockFile.Close()

    return nil
}

// createActiveFile creates a new active file.
func (b *Bitcask) createActiveFile() error {
    fileName := strconv.FormatInt(time.Now().UnixMicro(), 10)

    fileFlags := os.O_CREATE | os.O_RDWR
    if b.config.syncOption == SyncOnPut {
        fileFlags |= os.O_SYNC
    }

    activeFile, err := os.OpenFile(path.Join(b.datastorePath, fileName), fileFlags, fileMode)
    if err != nil {
        return err
    }

    b.activeFile.file = activeFile
    b.activeFile.fileName = fileName
    b.activeFile.currentPos = 0
    b.activeFile.currentSize = 0

    return nil
}

// writes to the current active file in the bitcask datastore.
func (b *Bitcask) writeToActiveFile(line string) (int, error) {
    if len(line) + b.activeFile.currentSize > maxFileSize {
        err := b.createActiveFile()
        if err != nil {
            return 0, err
        }
    }

    n, err := b.activeFile.file.Write([]byte(fmt.Sprintln(line)))
    if err != nil {
        return 0, err
    }

    return n, nil
}

// buildKeyDir establishes keydir associated with a bitcask datastore.
func (b *Bitcask) buildKeyDir() {
    if b.config.writePermission == ReadOnly && b.lockCheck() == reader {
        keyDirData, _ := os.ReadFile(path.Join(b.datastorePath, b.keyDirFileCheck()))

        b.keyDir = make(map[string]record)
        keyDirScanner := bufio.NewScanner(strings.NewReader(string(keyDirData)))

        for keyDirScanner.Scan() {
            line := keyDirScanner.Text()

            key, fileId, valueSize, valuePos, tstamp := extractKeyDirFileLine(line)

            b.keyDir[key] = record{
                fileId:    fileId,
                valueSize: valueSize,
                valuePos:  valuePos,
                tstamp:    tstamp,
            }
        }
    } else {
        var fileNames []string
        hintFilesMap := make(map[string]string)
        bitcaskDir, _ := os.Open(b.datastorePath)
        files, _ := bitcaskDir.Readdir(0)

        for _, file := range files {
            name := file.Name()
            if strings.HasPrefix(name, hintFilePrefix) {
                hintFilesMap[strings.Trim(name, hintFilePrefix)] = name
                fileNames = append(fileNames, strings.Trim(name, hintFilePrefix))
            } else {
                fileNames = append(fileNames, name)
            }
        }

        for _, name := range fileNames {
            if hint, isExist := hintFilesMap[name]; isExist {
                b.extractHintFile(hint)
            } else {
                var currentPos int = 0
                fileData, _ := os.ReadFile(path.Join(b.datastorePath, name))
                fileScanner := bufio.NewScanner(strings.NewReader(string(fileData)))
                for fileScanner.Scan() {
                    line := fileScanner.Text()
                    key, _, tstamp, keySize, valueSize := extractFileLine(line)
                    b.keyDir[key] = record{
                    	fileId:    name,
                    	valueSize: valueSize,
                    	valuePos:  currentPos + staticFields * numberFieldSize + keySize,
                    	tstamp:    tstamp,
                    }
                    currentPos += len(line) + 1
                }
            }
        }
    }
}

// buildKeyDirFile creates the file used by another processes to read the keydir of the current running procces.
func (b *Bitcask) buildKeyDirFile() {
    keyDirFileName := keyDirFilePrefix + strconv.FormatInt(time.Now().UnixMicro(), 10)
    b.keyDirFile = keyDirFileName
    keyDirFile, _ := os.Create(path.Join(b.datastorePath, keyDirFileName))
    for key, recValue := range b.keyDir {
        fileId, _ := strconv.Atoi(recValue.fileId)
        fileIdStr:= padWithZero(fileId)
        valueSizeStr:= padWithZero(recValue.valueSize)
        valuePosStr:= padWithZero(recValue.valuePos)
        tstampStr := padWithZero(recValue.tstamp)
        keySizeStr := padWithZero(len(key))

        line := fileIdStr + valueSizeStr + valuePosStr + tstampStr + keySizeStr + key
        fmt.Fprintln(keyDirFile, line)
    }
}

// compressFileLine creates a line in a form to be written into files.
func compressFileLine(key string, value string, tstamp int) []byte {
    tstampStr := padWithZero(tstamp)
    keySize := padWithZero(len([]byte(key)))
    valueSize := padWithZero(len([]byte(value)))
    return []byte(tstampStr + keySize + valueSize + string(key) + value)
}

// extractFileLine extracts the data embedded in the file line.
func extractFileLine(line string) (string, string, int, int, int) {
    tstamp, _ := strconv.Atoi(line[0: 19])
    keySize, _ := strconv.Atoi(line[19:38])
    valueSize, _ := strconv.Atoi(line[38:57])
    key := line[57:57+keySize]
    value := line[57+keySize:]

    return key, value, tstamp, keySize, valueSize
}

// extractKeyDirFileLine extracts the keydir data from keyDirFile.
func extractKeyDirFileLine(line string) (string, string, int, int, int) {
    fileId, _ := strconv.Atoi(line[0:19])
    valueSize, _ := strconv.Atoi(line[19:38])
    valuePos, _ := strconv.Atoi(line[38:57])
    tstamp, _ := strconv.Atoi(line[57:76])
    keySize, _ := strconv.Atoi(line[76:95])
    key := line[95:95+keySize]

    return key, strconv.Itoa(fileId), valueSize, valuePos, tstamp
}

// buildHintFileLine creates a line to be written in hint files.
func buildHintFileLine(recValue record, key string) string {
    tstamp := padWithZero(recValue.tstamp)
    keySize := padWithZero(len(key))
    valueSize := padWithZero(recValue.valueSize)
    valuePos := padWithZero(recValue.valuePos)
    return tstamp + keySize + valueSize + valuePos + key
}

// extractHintFile extracts the data from hint files.
func (b *Bitcask) extractHintFile(hintName string) {
    hintFileData, _ := os.ReadFile(path.Join(b.datastorePath, hintName))
    hintFileScanner := bufio.NewScanner(strings.NewReader(string(hintFileData)))

    fileId := strings.Trim(hintName, hintFilePrefix)

    for hintFileScanner.Scan() {
        line := hintFileScanner.Text()
        tstamp, _ := strconv.Atoi(line[0:19])
        keySize, _ := strconv.Atoi(line[19:38])
        valueSize, _ := strconv.Atoi(line[38:57])
        valuePos, _ := strconv.Atoi(line[57:76])
        key := line[76:76+keySize]

        b.keyDir[key] = record{
        	fileId:    fileId,
        	valueSize: valueSize,
        	valuePos:  valuePos,
        	tstamp:    tstamp,
        }
    }
}

// lockCheck checks if exist another process in the bitcask datastore.
func (b *Bitcask) lockCheck() processAccess {
    bitcaskDir, _ := os.Open(b.datastorePath)

    files, _ := bitcaskDir.Readdir(0)
    
    for _, file := range files {
        if strings.HasPrefix(file.Name(), readLock) {
            return reader
        } else if strings.HasPrefix(file.Name(), writeLock) {
            return writer
        }
    }
    return noProcess
}

// keyDirFileCheck checks if keydir file associated with another existing process exists.
func (b *Bitcask) keyDirFileCheck() string {
    var fileName string
    bitcaskDir, _ := os.Open(b.datastorePath)

    files, _ := bitcaskDir.Readdir(0)
    
    for _, file := range files {
        if strings.HasPrefix(file.Name(), keyDirFilePrefix) {
            fileName = file.Name()
            break
        }
    }
    return fileName
}

func padWithZero(val int) string {
    return fmt.Sprintf("%019d", val)
}
