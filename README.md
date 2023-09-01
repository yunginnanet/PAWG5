# pawg5

[![Go Report Card](https://goreportcard.com/badge/yunginnanet/pawg5)](https://goreportcard.com/report/yunginnanet/pawg5)
[![GoDoc](https://godoc.org/git.tcp.direct/kayos/pawg5?status.svg)](https://pkg.go.dev/git.tcp.direct/kayos/pawg5)

a ~~P.A.W.G rub~~ [pogreb](https://git.mills.io/prologic/bitcask) distributed key/value store
using [raft](https://github.com/hashicorp/raft) for concensus with a
[redis](https://redis.org) compatible API

based off of [kvnode](https://github.com/tidwall/kvnode) and [bitraft](https://git.mills.io/prologic/bitraft)
(See [LICENSE.old](/LICENSE.old) and [LICENSE.prologic](/LICENSE.prologic))

- redis compatible API
- pogreb disk-based storage
- Raft support with [finn](https://github.com/tidwall/finn) commands
- compatible with existing Redis clients (probably?)

### works like you'd expect prolly

```
SET key value
GET key
DEL key [key ...]
KEYS pattern
FLUSHDB
SHUTDOWN
```

### back that (*p*)hat (*a*)ss \[up\] (*w*)hite (*g*)irl, \[on\] (5)

```
RAFTSNAPSHOT
```
this will creates a new snapshot in the `data/snapshots` directory.
Each snapshot contains two files, `meta.json` and `state.bin`.
the state file is the database in a compressed format. 
The meta file is details about the state including the term, index, crc, and size.

ideally you call `RAFTSNAPSHOT` and then store the state.bin on some other server like S3.

To restore:
- create a new raft cluster
- download the state.bin snapshot
- pipe the commands using the `pawg5 --parse-snapshot` and `redis-cli --pipe` commands

#### Example:
```
pawg5 --parse-snapshot state.bin | redis-cli -h 10.0.1.5 -p 4920 --pipe
```

this will execute all of the `state.bin` commands on the leader at `10.0.1.5:4920`


for information on the `redis-cli --pipe` command see [Redis Mass Insert](https://redis.io/topics/mass-insert).
