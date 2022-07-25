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

// createActiveFile creates a new active file.
func (bitcask *Bitcask) createActiveFile() {
    fileName := strconv.FormatInt(time.Now().UnixMicro(), 10)

    activeFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, fileName),
    os.O_CREATE | os.O_RDWR, fileMode)

    bitcask.currentActive.file = activeFile
    bitcask.currentActive.fileName = fileName
    bitcask.currentActive.currentPos = 0
    bitcask.currentActive.currentSize = 0
}

// buildKeyDir establishes keydir associated with a bitcask datastore.
func (bitcask *Bitcask) buildKeyDir() {
    if bitcask.config.writePermission == ReadOnly && bitcask.lockCheck() == reader {
        keyDirData, _ := os.ReadFile(path.Join(bitcask.directoryPath, bitcask.keyDirFileCheck()))

        bitcask.keyDir = make(map[string]record)
        keyDirScanner := bufio.NewScanner(strings.NewReader(string(keyDirData)))

        for keyDirScanner.Scan() {
            line := keyDirScanner.Text()

            key, fileId, valueSize, valuePos, tstamp := extractKeyDirFileLine(line)

            bitcask.keyDir[key] = record{
                fileId:    fileId,
                valueSize: valueSize,
                valuePos:  valuePos,
                tstamp:    tstamp,
                isPending: false,
            }
        }
    } else {
        var fileNames []string
        hintFilesMap := make(map[string]string)
        bitcaskDir, _ := os.Open(bitcask.directoryPath)
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
                bitcask.extractHintFile(hint)
            } else {
                var currentPos int64 = 0
                fileData, _ := os.ReadFile(path.Join(bitcask.directoryPath, name))
                fileScanner := bufio.NewScanner(strings.NewReader(string(fileData)))
                for fileScanner.Scan() {
                    line := fileScanner.Text()
                    key, _, tstamp, keySize, valueSize := extractFileLine(line)
                    bitcask.keyDir[key] = record{
                    	fileId:    name,
                    	valueSize: valueSize,
                    	valuePos:  currentPos + staticFields * numberFieldSize + keySize,
                    	tstamp:    tstamp,
                    	isPending: false,
                    }
                    currentPos += int64(len(line) + 1)
                }
            }
        }
    }
}

// addPendingWrite adds a write to pending writes list.
// it force sync if pending writes number reached the maximum limit.
func (bitcask *Bitcask) addPendingWrite(key string, value string, tstamp int64) {
    if len(bitcask.pendingWrites) == maxPendingWrites {
        bitcask.Sync()
    }
    bitcask.pendingWrites[key] = string(compressFileLine(key, value, tstamp))
}

// writes to the current active file in the bitcask datastore.
func (bitcask *Bitcask) writeToActiveFile(line string) {
    if int64(len(line)) + bitcask.currentActive.currentSize > maxFileSize {
        newActiveFileName := strconv.FormatInt(time.Now().UnixMicro(), 10)
        newActiveFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, newActiveFileName), os.O_CREATE | os.O_RDWR, fileMode)

        bitcask.currentActive.currentSize = 0
        bitcask.currentActive.currentPos = 0
        bitcask.currentActive.file.Close()
        bitcask.currentActive.file = newActiveFile
        bitcask.currentActive.fileName = newActiveFileName
    }

    n, _ := bitcask.currentActive.file.Write([]byte(fmt.Sprintln(line)))
    bitcask.currentActive.currentSize += int64(n)
    bitcask.currentActive.currentPos += int64(n)
}

// compressFileLine creates a line in a form to be written into files.
func compressFileLine(key string, value string, tstamp int64) []byte {
    tstampStr := padWithZero(tstamp)
    keySize := padWithZero(int64(len([]byte(key))))
    valueSize := padWithZero(int64(len([]byte(value))))
    return []byte(tstampStr + keySize + valueSize + string(key) + value)
}

// extractFileLine extracts the data embedded in the file line.
func extractFileLine(line string) (string, string, int64, int64, int64) {
    tstamp, _ := strconv.ParseInt(line[0: 19], 10, 64)
    keySize, _ := strconv.ParseInt(line[19:38], 10, 64)
    valueSize, _ := strconv.ParseInt(line[38:57], 10, 64)
    key := line[57:57+keySize]
    value := line[57+keySize:]

    return key, value, tstamp, keySize, valueSize
}

// buildKeyDirFile creates the file used by another processes to read the keydir of the current running procces.
func (bitcask *Bitcask) buildKeyDirFile() {
    keyDirFileName := keyDirFilePrefix + strconv.FormatInt(time.Now().UnixMicro(), 10)
    bitcask.keyDirFile = keyDirFileName
    keyDirFile, _ := os.Create(path.Join(bitcask.directoryPath, keyDirFileName))
    for key, recValue := range bitcask.keyDir {
        fileId, _ := strconv.ParseInt(recValue.fileId, 10, 64)
        fileIdStr:= padWithZero(fileId)
        valueSizeStr:= padWithZero(recValue.valueSize)
        valuePosStr:= padWithZero(recValue.valuePos)
        tstampStr := padWithZero(recValue.tstamp)
        keySizeStr := padWithZero(int64(len(key)))

        line := fileIdStr + valueSizeStr + valuePosStr + tstampStr + keySizeStr + key
        fmt.Fprintln(keyDirFile, line)
    }
}

// extractKeyDirFileLine extracts the keydir data from keyDirFile.
func extractKeyDirFileLine(line string) (string, string, int64, int64, int64) {
    fileId, _ := strconv.ParseInt(line[0:19], 10, 64)
    valueSize, _ := strconv.ParseInt(line[19:38], 10, 64)
    valuePos, _ := strconv.ParseInt(line[38:57], 10, 64)
    tstamp, _ := strconv.ParseInt(line[57:76], 10, 64)
    keySize, _ := strconv.ParseInt(line[76:95], 10, 64)
    key := line[95:95+keySize]

    return key, strconv.FormatInt(fileId, 10), valueSize, valuePos, tstamp
}

// buildHintFileLine creates a line to be written in hint files.
func buildHintFileLine(recValue record, key string) string {
    tstamp := padWithZero(recValue.tstamp)
    keySize := padWithZero(int64(len(key)))
    valueSize := padWithZero(recValue.valueSize)
    valuePos := padWithZero(recValue.valuePos)
    return tstamp + keySize + valueSize + valuePos + key
}

// extractHintFile extracts the data from hint files.
func (bitcask *Bitcask) extractHintFile(hintName string) {
    hintFileData, _ := os.ReadFile(path.Join(bitcask.directoryPath, hintName))
    hintFileScanner := bufio.NewScanner(strings.NewReader(string(hintFileData)))

    fileId := strings.Trim(hintName, hintFilePrefix)

    for hintFileScanner.Scan() {
        line := hintFileScanner.Text()
        tstamp, _ := strconv.ParseInt(line[0:19], 10, 64)
        keySize, _ := strconv.ParseInt(line[19:38], 10, 64)
        valueSize, _ := strconv.ParseInt(line[38:57], 10, 64)
        valuePos, _ := strconv.ParseInt(line[57:76], 10, 64)
        key := line[76:76+keySize]

        bitcask.keyDir[key] = record{
        	fileId:    fileId,
        	valueSize: valueSize,
        	valuePos:  valuePos,
        	tstamp:    tstamp,
        	isPending: false,
        }
    }
}

// lockCheck checks if exist another process in the bitcask datastore.
func (bitcask *Bitcask) lockCheck() processAccess {
    bitcaskDir, _ := os.Open(bitcask.directoryPath)

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
func (bitcask *Bitcask) keyDirFileCheck() string {
    var fileName string
    bitcaskDir, _ := os.Open(bitcask.directoryPath)

    files, _ := bitcaskDir.Readdir(0)
    
    for _, file := range files {
        if strings.HasPrefix(file.Name(), keyDirFilePrefix) {
            fileName = file.Name()
            break
        }
    }
    return fileName
}

func padWithZero(val int64) string {
    return fmt.Sprintf("%019d", val)
}
