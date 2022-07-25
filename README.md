# Bitcask Description
The origin of Bitcask is tied to the history of the Riak distributed database. In a Riak key/value cluster, each
node uses pluggable local storage; nearly anything k/v-shaped can be used as the per-host storage engine. This
pluggability allowed progress on Riak to be parallelized such that storage engines could be improved and tested
without impact on the rest of the codebase.
**NOTE:** All project specifications and usage are mentioned in this [Official Bitcask Design Paper](https://riak.com/assets/bitcask-intro.pdf)

## Basic demo_writer
```go
package main
import (
	"bitcask"
	"fmt"
	"path"
)
func main() {
	bc, err := bitcask.Open(path.Join("bitcask"), bitcask.ReadWrite)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("****** append 100 item ******")
	for i := 0; i < 100; i++ {
		key := "key" + fmt.Sprintf("%d", i)
		value := "value" + fmt.Sprintf("%d", i)
		bc.Put(key, value)
	}
	fmt.Println("****** merge old files ******")
	bc.Merge()
	fmt.Println("****** Get some items ******")
	val2, _ := bc.Get("key2")
	fmt.Printf("value of key2 is: %s\n", string(val2))
	val15, _ := bc.Get("key15")
	fmt.Printf("value of key15 is: %s\n", string(val15))
	val77, _ := bc.Get("key77")
	fmt.Printf("value of key77 is: %s\n", string(val77))
	bc.Close()
}
```
## output with this only writer
```
$ go run demo_writer/writer.go 
****** append 100 item ******
****** merge old files ******
****** Get some items ******
value of key2 is: value2
value of key15 is: 
value of key77 is: value77
```
## output if there another writer
```
$ go run demo_writer/writer.go
another writer exists in this bitcask
```
-----
## Basic demo_infinite_writer
```go
package main
import (
	"bitcask"
	"fmt"
	"path"
)
func main() {
	bc, err := bitcask.Open(path.Join("bitcask"), bitcask.ReadWrite)
	if err != nil {
		fmt.Println(err.Error())
	}
	for {}
	bc.Close()
}
```
#### **NOTE:** there is no output for this infinite writer, it is used to lock the bitcask.
---
## Basic demo_reader
```go
package main
import (
	"bitcask"
	"fmt"
	"path"
)
func main() {
	bc, err := bitcask.Open(path.Join("bitcask"))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("****** Get some items ********")
	val26, _ := bc.Get("key26")
	fmt.Printf("value of key26 is: %s\n", string(val26))
	val10, _ := bc.Get("key10")
	fmt.Printf("value of key10 is: %s\n", string(val10))
	val89, _ := bc.Get("key89")
	fmt.Printf("value of key89 is: %s\n", string(val89))
	bc.Close()
}
```
## output if exist bitcask
```
$ go run demo_reader/reader.go
****** Get some items ********
value of key26 is: value26
value of key10 is: value10
value of key89 is: value89
```
## output if no exist bitcask
```
$ go run demo_reader/reader.go
read only cannot create new bitcask directory
```
## output if there is writer exist
```
$ go run demo_reader/reader.go
another writer exists in this bitcask
```
# Bitcask API

| Function                                                      | Description                                            |
|---------------------------------------------------------------|--------------------------------------------------------|
| ```func Open(dirPath string, opts ...ConfigOpt) (*Bitcask, error)```| Open a new or an existing bitcask datastore |
| ```func (bitcask *Bitcask) Put(key string, value string) error```| Stores a key and a value in the bitcask datastore |
| ```func (bitcask *Bitcask) Get(key string) (string, error)```| Reads a value by key from a datastore |
| ```func (bitcask *Bitcask) Delete(key string) error```| Removes a key from the datastore |
| ```func (bitcask *Bitcask) Close()```| Close a bitcask data store and flushes all pending writes to disk |
| ```func (bitcask *Bitcask) ListKeys() []string```| Returns list of all keys |
| ```func (bitcask *Bitcask) Sync() error```| Force any writes to sync to disk |
| ```func (bitcask *Bitcask) Merge() error```| Merge several data files within a Bitcask datastore into a more compact form. Also, produce hintfiles for faster startup. |
| ```func (bitcask *Bitcask) Fold(fun func(string, string, any) any, acc any) any```| Fold over all K/V pairs in a Bitcask datastore.→ Acc Fun is expected to be of the form: F(K,V,Acc0) → Acc |
