package bitcask

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"
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
        _, err := Open(testBitcaskPath)
        assertError(t, err, "read only cannot create new bitcask datastore")
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open existing bitcask with write permission", func(t *testing.T) {
        b1, _ := Open(testBitcaskPath, ReadWrite)
        b1.Put("key12", "value12345")
        b1.Close()

        b2, _ := Open(testBitcaskPath, ReadWrite)

        want := "value12345"
        got, _ := b2.Get("key12")
        b2.Close()

        assertString(t, got, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("two readers in the same bitcask at the same time", func(t *testing.T) {
        b1, _ := Open(testBitcaskPath, ReadWrite)
        b1.Put("key2", "value2")
        b1.Put("key3", "value3")
        b1.Close()

        b2, _ := Open(testBitcaskPath)
        b3, _ := Open(testBitcaskPath)

        want := "value2"
        got, _ := b2.Get("key2")
        assertString(t, got, want)
        b2.Close()

        got, _ = b3.Get("key2")
        assertString(t, got, want)
        b3.Close()
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open existing bitcask with hint files in it", func(t *testing.T) {
        b1, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)

        for i := 0; i < 1000; i++ {
            key := fmt.Sprintf("key%d", i + 1)
            value := fmt.Sprintf("value%d", i + 1)
            b1.Put(key, value)
        }
        b1.Merge()
        b1.Close()

        b2, _ := Open(testBitcaskPath)
        got, _ := b2.Get("key50")
        want := "value50"

        assertString(t, got, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open bitcask with writer exists in it", func(t *testing.T) {
        Open(testBitcaskPath, ReadWrite)
        _, err := Open(testBitcaskPath)

        assertError(t, err, "another writer exists in this bitcask")
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("open bitcask failed", func(t *testing.T) {
        // create a directory that cannot be openned since it has no execute permission
        os.MkdirAll(path.Join("no open dir"), 000)

        want := "no open dir: cannot open this directory"
        _, err := Open("no open dir")

        assertError(t, err, want)
        os.RemoveAll("no open dir")
    })
}

func TestGet(t *testing.T) {
    t.Run("existing value from file", func(t *testing.T) {
        b1, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)
        b1.Put("key12", "value12345")
        b1.Close()

        b2, _ := Open(testBitcaskPath)

        got, _ := b2.Get("key12")
        want := "value12345"

        assertString(t, got, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("existing value from pending list", func(t *testing.T) {
        b, _ := Open(testBitcaskPath, ReadWrite)
        b.Put("key12", "value12345")

        got, _ := b.Get("key12")
        want := "value12345"

        assertString(t, got, want)
        os.RemoveAll(testBitcaskPath)
    })

    t.Run("not existing value", func(t *testing.T) {
        b, _ := Open(testBitcaskPath, ReadWrite)

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
        b1, _ := Open(testBitcaskPath, ReadWrite)
        b1.Close()

        b2, _ := Open(testBitcaskPath)
        err := b2.Put("key12", "value12345")

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
        b1, _ := Open(testBitcaskPath, ReadWrite)
        b1.Close()

        b2, _ := Open(testBitcaskPath)
        err := b2.Delete("key12")
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
        b, _ := Open(testBitcaskPath, ReadWrite, SyncOnPut)

        for i := 0; i < 1000; i++ {
            key := fmt.Sprintf("key%d", i + 1)
            value := fmt.Sprintf("value%d", i + 1)
            b.Put(key, value)
        }
        b.Merge()
        want := "value50"
        got, _ := b.Get("key50")

        b.Close()
        assertString(t, got, want)
        // os.RemoveAll(testBitcaskPath)
    })

    t.Run("with no write permission", func(t *testing.T) {
        b1, _ := Open(testBitcaskPath, ReadWrite)
        b1.Close()

        b2, _ := Open(testBitcaskPath)

        err := b2.Merge()
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

        for i := 0; i < 1000; i++ {
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
        b1, _ := Open(testBitcaskPath, ReadWrite)
        b1.Close()

        b2, _ := Open(testBitcaskPath)
        err := b2.Sync()

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
