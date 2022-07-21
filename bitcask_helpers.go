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
    activeFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, 
    strconv.FormatInt(time.Now().Unix(), 10)),
    os.O_CREATE | os.O_RDWR, fileMode)

    bitcask.currentActive.file = activeFile
    bitcask.currentActive.currentPos = 0
    bitcask.currentActive.currentSize = 0
}

func (bitcask *Bitcask) buildKeyDir() {
    keyDirData, _ := os.ReadFile(path.Join(bitcask.directoryPath, keyDirFileName))

    bitcask.keyDir = make(map[key]record)
    keyDirScanner := bufio.NewScanner(strings.NewReader(string(keyDirData)))

    for keyDirScanner.Scan() {
        line := strings.Split(keyDirScanner.Text(), keyDirFileSeprator)
        key := key(line[0])
        fileId := line[1]
        valueSize, _ := strconv.ParseInt(line[2], 10, 64)
        valuePos, _ := strconv.ParseInt(line[3], 10, 64)
        tstamp, _ := strconv.ParseInt(line[4], 10, 64)

        bitcask.keyDir[key] = record{
        	fileId:    fileId,
        	valueSize: valueSize,
        	valuePos:  valuePos,
        	tstamp:    tstamp,
        	isPending: false,
        }
    }
}

func composeFileLine(key key, value string) []byte {
    tstamp := padWithZero(time.Now().Unix())
    keySize := padWithZero(int64(len([]byte(key))))
    valueSize := padWithZero(int64(len([]byte(value))))
    return []byte(tstamp + keySize + valueSize + string(key) + value)
}

func extractFileLine(line fileLine) (key, string, int64, int64, int64) {
    lineString := string(line)
    tstamp, _ := strconv.ParseInt(lineString[tstampOffset: numberFieldSize], 10, 64)
    keySize, _ := strconv.ParseInt(lineString[keySizeOffset:keySizeOffset + numberFieldSize], 10, 64)
    valueSize, _ := strconv.ParseInt(lineString[valueSizeOffset:valueSizeOffset + numberFieldSize], 10, 64)

    keyFieldPos := int64(valueSizeOffset + numberFieldSize)
    key := key(lineString[keyFieldPos:keyFieldPos + keySize])

    valueFieldPos := int64(keyFieldPos + keySize)
    value := lineString[valueFieldPos:valueFieldPos + valueSize]

    return key, value, tstamp, keySize, valueSize
}

func padWithZero(val int64) string {
    return fmt.Sprintf("%019d", val)
}
