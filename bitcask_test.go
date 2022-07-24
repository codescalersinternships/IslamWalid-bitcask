package bitcask

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

var testBitcaskPath = path.Join("testing_dir")
var testKeyDirPath = path.Join("testing_dir", "keydir")
var testFilePath = path.Join("testing_dir", "testfile")

func TestOpen(t *testing.T) {
    t.Run("open new bitcask with read and write permission", func(t *testing.T) {
        Open(testBitcaskPath, ReadWrite)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open new bitcask with sync_on_put option", func(t *testing.T) {
        Open(testBitcaskPath, ReadWrite, SyncOnPut)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open new bitcask with default options", func(t *testing.T) {
        Open(testBitcaskPath)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open existing bitcask with read and write permission", func(t *testing.T) {
        Open(testBitcaskPath, ReadWrite)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "00000000000000000010000000000000000120000000000000000005500000000000863956270000000000000000005key12")

        Open(testBitcaskPath, ReadWrite)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open existing bitcask with sync on put option", func(t *testing.T) {
        Open(testBitcaskPath, SyncOnPut)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "00000000000000000010000000000000000120000000000000000005500000000000863956270000000000000000005key12")

        Open(testBitcaskPath, SyncOnPut)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open existing bitcask with default options", func(t *testing.T) {
        Open(testBitcaskPath)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "00000000000000000010000000000000000120000000000000000005500000000000863956270000000000000000005key12")

        Open(testBitcaskPath)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open bitcask failed", func(t *testing.T) {
        if runtime.GOOS != "windows" {
            // create a directory that cannot be openned since it has no execute permission
            os.MkdirAll(path.Join("no open dir"), 000)

            want := "no open dir: cannot open this directory"
            _, err := Open("no open dir")

            assertError(t, err, want)

            os.RemoveAll("no open dir")
        }
    })
}

func TestGet(t *testing.T) {
    t.Run("existing value from file", func(t *testing.T) {
        os.MkdirAll(testBitcaskPath, 0700)
        file, _ := os.Create(testFilePath)
        file.Write(compressFileLine("key12", "value12345", time.Now().Unix()))

        b, _ := Open(testBitcaskPath)

        b.keyDir["key12"] = record{
            fileId:    "testfile",
            valueSize: 10,
            valuePos:  62,
            tstamp:    34567,
            isPending: false,
        }

        got, _ := b.Get("key12")
        want := "value12345"

        assertString(t, got, want)

        os.RemoveAll(testBitcaskPath)
    })

    t.Run("existing value from pending list", func(t *testing.T) {
        b, _ := Open(testBitcaskPath, ReadWrite)

        b.keyDir["key12"] = record{
            fileId:    "testfile",
            valueSize: 10,
            valuePos:  62,
            tstamp:    34567,
            isPending: true,
        }

        b.pendingWrites["key12"] = string(compressFileLine("key12", "value12345", time.Now().Unix()))

        got, _ := b.Get("key12")
        want := "value12345"

        assertString(t, got, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("not existing value", func(t *testing.T) {
        b, _ := Open(testBitcaskPath)

        want := "unknown key: key does not exist"
        _, err := b.Get("unknown key")

        assertError(t, err, want)
        os.RemoveAll(testBitcaskPath)
    })
}

func TestPut(t *testing.T) {
    t.Run("put with sync on demand options is set", func(t *testing.T) {
        b, _ := Open(testBitcaskPath, ReadWrite, SyncOnDemand)
        b.Put("key12", "value12345")

        want := "value12345"
        got, _ := b.Get("key12")

        assertString(t, got, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("reach max pending writes limit", func(t *testing.T) {
        b, _ := Open(testBitcaskPath, ReadWrite, SyncOnDemand)

        for i := 0; i <= maxPendingWrites; i++ {
            key := fmt.Sprintf("key%d", i + 1)
            value := fmt.Sprintf("value%d", i + 1)
            b.Put(key, value)
        }

        _, isExist := b.pendingWrites["key101"]
        if len(b.pendingWrites) != 1 && !isExist {
            t.Error("max pending writes limit reached and no force sync happened")
            t.Error(len(b.pendingWrites))
        }
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("put with no write permission", func(t *testing.T) {
        b, _ := Open(testBitcaskPath)

        err := b.Put("key12", "value12345")

        assertError(t, err, "write permission denied")
        os.RemoveAll(testBitcaskPath)
    })
}

func TestDelete(t *testing.T) {
    t.Run("delete existing key", func(t *testing.T) {
        b, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)
        b.Put("key12", "value12345")
        b.Delete("key12")
        _, err := b.Get("key12")
        assertError(t, err, "key12: key does not exist")
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("delete not existing key", func(t *testing.T) {
        b, _ := Open(testBitcaskPath, ReadWrite, SyncOnDemand)
        err := b.Delete("key12")
        assertError(t, err, "key12: key does not exist")
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("delete with no write permission", func(t *testing.T) {
        b, _ := Open(testBitcaskPath)
        err := b.Delete("key12")
        assertError(t, err, "write permission denied")
        os.RemoveAll(testBitcaskPath)
    })
}

func TestListkeys(t *testing.T) {
    b, _ := Open(testBitcaskPath, ReadWrite, SyncOnDemand)

    key := "key12"
    value := "value12345"
    b.Put(key, value)

    want := []string{"key12"}
    got := b.ListKeys()

    if !reflect.DeepEqual(got, want) {
        t.Errorf("got:\n%v\nwant:\n%v", got, want)
    }
    os.RemoveAll(testBitcaskPath)
}

func TestFold(t *testing.T) {
    b, _ := Open(testBitcaskPath, ReadWrite, SyncOnDemand)

    for i := 0; i < 10; i++ {
        b.Put(fmt.Sprint(i + 1), fmt.Sprint(i + 1))
    }
    
    want := 110
    got := b.Fold(func(s1, s2 string, a any) any {
        acc, _ := a.(int)
        k, _ := strconv.Atoi(s1)
        v, _ := strconv.Atoi(s2)

        return acc + k + v
    }, 0)

    if got != want {
        t.Errorf("got:%d, want:%d", got, want)
    }
    os.RemoveAll(testBitcaskPath)
}

func TestMerge(t *testing.T) {
    t.Run("with write permission", func(t *testing.T) {
        b1, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)
        b1.Put("key12", "value12345")
        b1.Merge()

        b2, _ := Open(testBitcaskPath)

        want := "value12345"
        got, _ := b2.Get("key12")

        t.Errorf("%v\n", b2.keyDir)
        assertString(t, got, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("with no write permission", func(t *testing.T) {
        b1, _ := Open(testBitcaskPath)
        err := b1.Merge()

        want := "write permission denied"

        assertError(t, err, want)
        os.RemoveAll(testBitcaskPath)
    })
}

func TestSync(t *testing.T) {
    t.Run("put with sync on put option is set", func(t *testing.T) {
        b, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)
        b.Put("key12", "value12345")

        want := "value12345"
        got, _ := b.Get("key12")

        assertString(t, got, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("reach max file size limit", func(t *testing.T) {
        b, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)

        for i := 0; i < 25; i++ {
            key := fmt.Sprintf("key%d", i + 1)
            value := fmt.Sprintf("value%d", i + 1)
            b.Put(key, value)
        }

        want := "value25"
        got, _ := b.Get("key25")

        assertString(t, got, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("sync with no write permission", func(t *testing.T) {
        b, _ := Open(testBitcaskPath)

        err := b.Sync()

        assertError(t, err, "write permission denied")
        os.RemoveAll(testBitcaskPath)
    })
}

func assertError(t testing.TB, err error, want string) {
    t.Helper()
    if err == nil {
        t.Fatalf("Expected Error %q", want)
    }
    assertString(t, err.Error(), want)
}

func assertString(t testing.TB, got, want string) {
    t.Helper()
    if got != want {
        t.Errorf("got:\n%q\nwant:\n%q", got, want)
    }
}
