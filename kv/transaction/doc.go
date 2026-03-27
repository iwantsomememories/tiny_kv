package transaction

// The transaction package implements TinyKV's 'transaction' layer. This takes incoming requests from the kv/server/server.go
// as input and turns them into reads and writes of the underlying key/value store (defined by Storage in kv/storage/storage.go).
// The storage engine handles communicating with other nodes and writing data to disk. The transaction layer must
// translate high-level TinyKV commands into low-level raw key/value commands and ensure that processing of commands do
// not interfere with processing other commands.
//
// Note that there are two kinds of transactions in play: TinySQL transactions are collaborative between TinyKV and its
// client (e.g., TinySQL). They are implemented using multiple TinyKV requests and ensure that multiple SQL commands can
// be executed atomically. There are also mvcc transactions which are an implementation detail of this
// layer in TinyKV (represented by MvccTxn in kv/transaction/mvcc/transaction.go). These ensure that a *single* request
// is executed atomically.
//
// *Locks* are used to implement TinySQL transactions. Setting or checking a lock in a TinySQL transaction is lowered to
// writing to the underlying store.
//
// *Latches* are used to implement mvcc transactions and are not visible to the client. They are stored outside the
// underlying storage (or equivalently, you can think of every key having its own latch). See the latches package for details.
//
// Within the `mvcc` package, `Lock` and `Write` provide abstractions for lowering locks and writes into simple keys and values.
//
// ## Encoding user key/values
//
// The mvcc strategy is essentially to store all data (committed and uncommitted) at every point in time. So for example, if we store
// a value for a key, then store another value (a logical overwrite) at a later time, both values are preserved in the underlying
// storage.
//
// This is implemented by encoding user keys with their timestamps (the starting timestamp of the transaction in which they are
// written) to make an encoded key (see codec.go). The `default` CF is a mapping from encoded keys to their values.
//
// Locking a key means writing into the `lock` CF. In this CF, we use the user key (i.e., not the encoded key so that a key is locked
// for all timestamps). The value in the `lock` CF consists of the 'primary key' for the transaction, the kind of lock (for 'put',
// 'delete', or 'rollback'), the start timestamp of the transaction, and the lock's ttl (time to live). See lock.go for the
// implementation.
//
// The status of values is stored in the `write` CF. Here we map keys encoded with their commit timestamps (i.e., the time at which a
// a transaction is committed) to a value containing the transaction's starting timestamp, and the kind of write ('put', 'delete', or
// 'rollback'). Note that for transactions which are rolled back, the start timestamp is used for the commit timestamp in the encoded
// key.

// transaction 包实现了 TinyKV 的“事务”层。它将来自 kv/server/server.go 的传入请求
// 作为输入，并把它们转换为对底层键值存储的读写（该存储由 kv/storage/storage.go 中的 Storage 定义）。
// 存储引擎负责与其他节点通信并将数据写入磁盘。事务层必须
// 将高层的 TinyKV 命令翻译为底层原始的键值命令，并确保这些命令的处理
// 不会干扰其他命令的处理。
//
// 请注意，这里涉及两种事务：TinySQL 事务是 TinyKV 与其
// 客户端（例如 TinySQL）之间协作完成的。它们通过多个 TinyKV 请求来实现，
// 并确保多个 SQL 命令能够原子地执行。还有 mvcc 事务，它是 TinyKV 中这一层的
// 一个实现细节（在 kv/transaction/mvcc/transaction.go 中由 MvccTxn 表示）。
// 它们保证单个请求能够原子地执行。
//
// *Locks*（锁）用于实现 TinySQL 事务。在 TinySQL 事务中设置或检查一个锁，
// 会被降级为对底层存储的写操作。
//
// *Latches*（闩锁）用于实现 mvcc 事务，对客户端不可见。它们存储在底层存储之外
// （或者等价地，你可以认为每个 key 都有其自己的 latch）。详情请参见 latches 包。
//
// 在 `mvcc` 包中，`Lock` 和 `Write` 提供了抽象，
// 用于将锁和写操作降级为简单的键和值。
//
// ## 用户 key/value 的编码
//
// mvcc 策略的本质是在每一个时间点都保存所有数据（包括已提交和未提交的数据）。
// 因此，例如，如果我们先为某个 key 存储一个 value，之后在更晚的时间再存储另一个 value
// （逻辑上的覆盖写），那么这两个 value 都会保留在底层存储中。
//
// 其实现方式是：将用户 key 与其时间戳（即写入该 key 的事务的开始时间戳）进行编码，
// 生成一个编码后的 key（参见 codec.go）。`default` CF 是从编码后的 key 到其 value 的映射。
//
// 对一个 key 加锁意味着向 `lock` CF 中写入数据。在这个 CF 中，我们使用用户 key
// （即不是编码后的 key，这样同一个 key 会在所有时间戳上都被锁定）。
// `lock` CF 中的 value 包含该事务的“主键（primary key）”、锁的类型
// （`put`、`delete` 或 `rollback`）、事务的开始时间戳，以及锁的 ttl（生存时间）。
// 其实现参见 lock.go。
//
// value 的状态存储在 `write` CF 中。在这里，我们将“以提交时间戳编码后的 key”
// （也就是事务提交时的时间）映射到一个 value，该 value 包含事务的开始时间戳，
// 以及写入类型（`put`、`delete` 或 `rollback`）。请注意，对于被回滚的事务，
// 编码 key 中的提交时间戳会使用开始时间戳。
