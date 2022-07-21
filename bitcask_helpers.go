package bitcask

import (
	"bufio"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

func (b *Bitcask) createActiveFile() {
    activeFile, _ := os.OpenFile(path.Join(b.directoryPath, 
    strconv.FormatInt(time.Now().Unix(), 10)),
    os.O_CREATE | os.O_RDWR, fileMode)

    b.currentActive.file = activeFile
    b.currentActive.currentPos = 0
    b.currentActive.currentSize = 0
}

func (b *Bitcask) buildKeyDir() {
    keyDirData, _ := os.ReadFile(path.Join(b.directoryPath, keyDirFileName))

    b.keyDir = make(map[key]record)
    keyDirScanner := bufio.NewScanner(strings.NewReader(string(keyDirData)))

    for keyDirScanner.Scan() {
        line := strings.Split(keyDirScanner.Text(), keyDirFileSeprator)
        key := key(line[0])
        fileId := line[1]
        valueSize, _ := strconv.ParseInt(line[2], 10, 64)
        valuePos, _ := strconv.ParseInt(line[3], 10, 64)
        tstamp, _ := strconv.ParseInt(line[4], 10, 64)

        b.keyDir[key] = record{
        	fileId:    fileId,
        	valueSize: valueSize,
        	valuePos:  valuePos,
        	tstamp:    tstamp,
        	isPending: false,
        }
    }
}
