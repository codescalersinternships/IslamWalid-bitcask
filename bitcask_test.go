package bitcask

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"
)

var testBitcaskPath = path.Join("testing_dir")
var testKeyDirPath = path.Join("testing_dir", "keydir")

func TestOpen(t *testing.T) {
    t.Run("open new bitcask with read and write permission", func(t *testing.T) {
        open(testBitcaskPath, ReadWrite)
        
        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open new bitcask with sync_on_put option", func(t *testing.T) {
        open(testBitcaskPath, ReadWrite, SyncOnPut)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open new bitcask with default options", func(t *testing.T) {
        open(testBitcaskPath)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open existing bitcask with read and write permission", func(t *testing.T) {
        open(testBitcaskPath, ReadWrite)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "key 1 50 0 3")

        open(testBitcaskPath, ReadWrite)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open existing bitcask with sync on put option", func(t *testing.T) {
        open(testBitcaskPath, SyncOnPut)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "key 1 50 0 3")

        open(testBitcaskPath, SyncOnPut)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open existing bitcask with default options", func(t *testing.T) {
        open(testBitcaskPath)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "key 1 50 0 3")

        open(testBitcaskPath)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open bitcask failed", func(t *testing.T) {
        if runtime.GOOS != "windows" {
            // create a directory that cannot be openned since it has no execute permission
            os.MkdirAll(path.Join("no open dir"), 000)
            _, err := open("no open dir")
            if err == nil {
                t.Fatal("Expected Error since path cannot be openned")
            }
            os.RemoveAll("no open dir")
        }
    })
}
