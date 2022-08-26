# Bitcask Description
The origin of Bitcask is tied to the history of the Riak distributed database. In a Riak key/value cluster, each
node uses pluggable local storage; nearly anything k/v-shaped can be used as the per-host storage engine. This
pluggability allowed progress on Riak to be parallelized such that storage engines could be improved and tested
without impact on the rest of the codebase.
**NOTE:** All project specifications and usage are mentioned in this [Official Bitcask Design Paper](https://riak.com/assets/bitcask-intro.pdf)

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
