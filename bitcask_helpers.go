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

func (bitcask *Bitcask) createActiveFile() {
    fileName := strconv.FormatInt(time.Now().Unix(), 10)

    activeFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, fileName),
    os.O_CREATE | os.O_RDWR, fileMode)

    bitcask.currentActive.file = activeFile
    bitcask.currentActive.fileName = fileName
    bitcask.currentActive.currentPos = 0
    bitcask.currentActive.currentSize = 0
}

func (bitcask *Bitcask) buildKeyDir() {
    if bitcask.config.writePermission == ReadOnly && bitcask.lockCheck() == reader {
        keyDirData, _ := os.ReadFile(path.Join(bitcask.directoryPath, keyDirFileName))

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
        var hintFilesMap map[string]string
        bitcaskDir, _ := os.Open(bitcask.directoryPath)
        files, _ := bitcaskDir.Readdir(0)

        for _, file := range files {
            name := file.Name()
            if strings.HasSuffix(name, "hint") {
                hintFilesMap[strings.Trim(name, "hint")] = name
                fileNames = append(fileNames, strings.Trim(name, "hint"))
            } else {
                fileNames = append(fileNames, name)
            }
        }

        for _, name := range fileNames {
            if hint, isExist := hintFilesMap[name]; isExist {
                bitcask.extractHintFile(hint)
            } else {
                var currentPos int64 = 0
                fileData, _ := os.ReadFile(name)
                fileScanner := bufio.NewScanner(strings.NewReader(string(fileData)))
                for fileScanner.Scan() {
                    line := fileScanner.Text()
                    currentPos += int64(len(line))
                    key, _, tstamp, keySize, valueSize := extractFileLine(line)
                    bitcask.keyDir[key] = record{
                    	fileId:    name,
                    	valueSize: valueSize,
                    	valuePos:  currentPos + staticFields * numberFieldSize + keySize,
                    	tstamp:    tstamp,
                    	isPending: false,
                    }
                }
            }
        }
    }
}

func (bitcask *Bitcask) addPendingWrite(key string, value string, tstamp int64) {
    if len(bitcask.pendingWrites) == maxPendingWrites {
        bitcask.Sync()
    }
    bitcask.pendingWrites[key] = string(compressFileLine(key, value, tstamp))
}

func (bitcask *Bitcask) writeToActiveFile(line string) {
    if int64(len(line)) + bitcask.currentActive.currentSize > maxFileSize {
        newActiveFileName := path.Join(bitcask.directoryPath, strconv.FormatInt(time.Now().Unix(), 10))
        newActiveFile, _ := os.OpenFile(newActiveFileName, os.O_CREATE | os.O_RDWR, fileMode)

        bitcask.currentActive.currentSize = 0
        bitcask.currentActive.currentPos = 0
        bitcask.currentActive.file.Close()
        bitcask.currentActive.file = newActiveFile
    }

    n, _ := bitcask.currentActive.file.Write([]byte(fmt.Sprintln(line)))
    bitcask.currentActive.currentSize += int64(n)
    bitcask.currentActive.currentPos += int64(n)
}

func compressFileLine(key string, value string, tstamp int64) []byte {
    tstampStr := padWithZero(tstamp)
    keySize := padWithZero(int64(len([]byte(key))))
    valueSize := padWithZero(int64(len([]byte(value))))
    return []byte(tstampStr + keySize + valueSize + string(key) + value)
}

func extractFileLine(line string) (string, string, int64, int64, int64) {
    lineString := string(line)
    tstamp, _ := strconv.ParseInt(lineString[tstampOffset: numberFieldSize], 10, 64)
    keySize, _ := strconv.ParseInt(lineString[keySizeOffset:keySizeOffset + numberFieldSize], 10, 64)
    valueSize, _ := strconv.ParseInt(lineString[valueSizeOffset:valueSizeOffset + numberFieldSize], 10, 64)

    keyFieldPos := int64(valueSizeOffset + numberFieldSize)
    key := lineString[keyFieldPos:keyFieldPos + keySize]

    valueFieldPos := int64(keyFieldPos + keySize)
    value := lineString[valueFieldPos:valueFieldPos + valueSize]

    return key, value, tstamp, keySize, valueSize
}

func (bitcask *Bitcask) buildKeyDirFile() {
    keyDirFile, _ := os.Create(path.Join(bitcask.directoryPath, keyDirFileName))
    for key, recValue := range bitcask.keyDir {
        fileId, _ := strconv.ParseInt(recValue.fileId, 10, 64)
        fileIdStr:= padWithZero(fileId)
        valueSizeStr:= padWithZero(recValue.valueSize)
        valuePosStr:= padWithZero(recValue.valuePos)
        tstampStr := padWithZero(recValue.tstamp)
        keySizeStr := padWithZero(int64(len(key)))

        line := fileIdStr + valueSizeStr + valuePosStr + tstampStr + keySizeStr
        fmt.Fprintln(keyDirFile, line)
    }
}

func extractKeyDirFileLine(line string) (string, string, int64, int64, int64) {
    fileId, _ := strconv.ParseInt(line[0:19], 10, 64)
    valueSize, _ := strconv.ParseInt(line[19:38], 10, 64)
    valuePos, _ := strconv.ParseInt(line[38:57], 10, 64)
    tstamp, _ := strconv.ParseInt(line[57:76], 10, 64)
    keySize, _ := strconv.ParseInt(line[76:95], 10, 64)
    key := line[95:95+keySize]

    return key, strconv.FormatInt(fileId, 10), valueSize, valuePos, tstamp
}

func (bitcask *Bitcask) buildHintFileLine(key string) string {
    tstamp := padWithZero(bitcask.keyDir[key].tstamp)
    keySize := padWithZero(int64(len(key)))
    valueSize := padWithZero(bitcask.keyDir[key].valueSize)
    valuePos := padWithZero(bitcask.keyDir[key].valuePos)
    return tstamp + keySize + valueSize + valuePos + key
}

func (bitcask *Bitcask) extractHintFile(hintName string) {
    hintFileData, _ := os.ReadFile(path.Join(bitcask.directoryPath, hintName))
    hintFileScanner := bufio.NewScanner(strings.NewReader(string(hintFileData)))

    fileId := strings.Trim(hintName, "hint")

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

func padWithZero(val int64) string {
    return fmt.Sprintf("%019d", val)
}
