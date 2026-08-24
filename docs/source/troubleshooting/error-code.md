# Error Code Explanation

## Mooncake TransferEngine

TransferEngine may generate various types of errors during execution. For most APIs, the return value indicates the error reason. For details, refer to `mooncake-transfer-engine/include/error.h`.

| Group      | Return Value                 | Description                                                                                               |
|------------|-----------------------------|-----------------------------------------------------------------------------------------------------------|
| Normal     | 0                           | Normal execution                                                                                           |
| Error Args | ERR_INVALID_ARGUMENT        | Input parameters are incorrect (and cannot be detailed into other items in this group)                  |
|            | ERR_TOO_MANY_REQUESTS       | The number of requests passed by the user when calling the `SubmitTransfer` interface exceeds the maximum value specified by the allocated `BatchID` |
|            | ERR_ADDRESS_NOT_REGISTERED  | The source address and/or target address in the request initiated by the user are not registered (including the situation where it has been registered locally but not uploaded to the metadata server) |
|            | ERR_BATCH_BUSY              | Reclaim a `BatchID` that is currently executing a request                                    |
|            | ERR_DEVICE_NOT_FOUND        | No available RDMA device to execute the user's request                                                    |
|            | ERR_ADDRESS_OVERLAPPED      | Registering overlapping memory areas multiple times                                                        |
| Handshake  | ERR_DNS                     | Local server name is not a valid DNS hostname or IP address, preventing other nodes from handshaking with this node |
|            | ERR_SOCKET                  | Errors related to TCP Socket during the handshake process                                             |
|            | ERR_MALFORMED_JSON          | Data format error during handshake exchange                                                                |
|            | ERR_REJECT_HANDSHAKE        | Peer rejects the handshake due to peer's errors                             |
| Other      | ERR_METADATA                | Failed to communicate with metadata server                                                           |
|            | ERR_ENDPOINT                | Exceptions during the creation and use of `RdmaEndPoint` objects                                          |
|            | ERR_NUMA                    | The system does not support numa interface                                                                 |
|            | ERR_CLOCK                   | The system does not support `clock_gettime` interface                                                         |
|            | ERR_MEMORY                  | Out of memory                                                                            |

## Mooncake Store

Mooncake Store may generate various types of errors during execution. For most APIs, the return value indicates the error reason. For details, refer to `mooncake-store/include/types.h`.

| Group                    | Return Value                    | Description                                                                                               |
|--------------------------|--------------------------------|-----------------------------------------------------------------------------------------------------------|
| Normal                   | 0                              | Operation successful                                                                                       |
| Internal                 | INTERNAL_ERROR (-1)            | Internal error occurred                                                                                    |
|                          | NOT_IMPLEMENTED (-2)           | Requested capability is not implemented for the selected deployment mode                                  |
| Buffer Allocation        | BUFFER_OVERFLOW (-10)          | Insufficient buffer space                                                                                  |
| Segment Selection        | SHARD_INDEX_OUT_OF_RANGE (-100)| Shard index is out of bounds                                                                              |
|                          | SEGMENT_NOT_FOUND (-101)       | No available segments found                                                                               |
|                          | SEGMENT_ALREADY_EXISTS (-102)  | Segment already exists                                                                                    |
|                          | CLIENT_NOT_FOUND (-103)        | Client was not found                                                                                       |
|                          | CLIENT_ALREADY_EXISTS (-104)   | Client is already registered                                                                               |
|                          | CLIENT_UNHEALTHY (-105)        | Client is not healthy enough for the requested operation                                                   |
|                          | NO_AVAILABLE_CANDIDATE (-106)  | No eligible P2P write-route candidate is available                                                         |
| Handle Selection         | NO_AVAILABLE_HANDLE (-200)     | Memory allocation failed due to insufficient space                                                        |
| Version                  | INVALID_VERSION (-300)         | Invalid version                                                                                           |
|                          | CAS_FAILED (-301)              | Optimistic compare-and-swap failed                                                                         |
| Key                      | INVALID_KEY (-400)             | Invalid key                                                                                              |
| Engine                   | WRITE_FAIL (-500)              | Write operation failed                                                                                    |
| Parameter                | INVALID_PARAMS (-600)          | Invalid parameters                                                                                        |
|                          | ILLEGAL_CLIENT (-601)          | Client is not permitted to perform the operation                                                           |
|                          | NON_CONTIGUOUS_BUFFER_NOT_SUPPORTED (-602) | Forward transfer mode does not support the supplied non-contiguous buffers                    |
| Engine Operation         | INVALID_WRITE (-700)           | Invalid write operation                                                                                   |
|                          | INVALID_READ (-701)            | Invalid read operation                                                                                    |
|                          | INVALID_REPLICA (-702)         | Invalid replica operation                                                                                 |
| Object                   | REPLICA_IS_NOT_READY (-703)    | Replica is not ready                                                                                      |
|                          | OBJECT_NOT_FOUND (-704)        | Object not found                                                                                          |
|                          | OBJECT_ALREADY_EXISTS (-705)   | Object already exists                                                                                     |
|                          | OBJECT_HAS_LEASE (-706)        | Object has lease                                                                                          |
|                          | LEASE_EXPIRED (-707)           | Lease expired before data transfer completed                                                              |
|                          | OBJECT_HAS_REPLICATION_TASK (-708) | Object already has a replication task                                                                |
|                          | OBJECT_NO_REPLICATION_TASK (-709) | Object has no replication task                                                                          |
|                          | REPLICA_NOT_FOUND (-710)       | Replica was not found                                                                                      |
|                          | REPLICA_ALREADY_EXISTS (-711)  | Replica already exists                                                                                     |
|                          | REPLICA_IS_GONE (-712)         | Replica existed previously but is gone                                                                     |
|                          | REPLICA_NUM_EXCEEDED (-713)    | Requested replica count exceeds the configured limit                                                       |
|                          | REPLICA_IS_PROCESSING (-714)   | Replica is processing an in-flight write                                                                  |
| Transfer                 | TRANSFER_FAIL (-800)           | Transfer operation failed                                                                                 |
| RPC                      | RPC_FAIL (-900)                | RPC operation failed                                                                                      |
|                          | HEARTBEAT_RPC_UNREACHABLE (-901) | Dedicated heartbeat RPC server is unreachable                                                           |
|                          | HEARTBEAT_ROUTING_MISMATCH (-902) | Client and P2P master disagree about dedicated heartbeat routing                                        |
| High Availability        | ETCD_OPERATION_ERROR (-1000)   | etcd operation failed                                                                                     |
|                          | ETCD_KEY_NOT_EXIST (-1001)     | Key not found in etcd                                                                                    |
|                          | ETCD_TRANSACTION_FAIL (-1002)  | etcd transaction failed                                                                                   |
|                          | ETCD_CTX_CANCELLED (-1003)     | etcd context cancelled                                                                                    |
|                          | OPLOG_ENTRY_NOT_FOUND (-1004)  | Requested OpLog entry was not found                                                                        |
|                          | OPLOG_TRIMMED (-1005)          | Requested OpLog range has already been trimmed                                                             |
|                          | UNAVAILABLE_IN_CURRENT_STATUS (-1010) | Request cannot be done in current status                                                      |
|                          | UNAVAILABLE_IN_CURRENT_MODE (-1011)   | Request cannot be done in current mode                                                           |
| File                     | FILE_NOT_FOUND (-1100)         | File not found                                                                                            |
|                          | FILE_OPEN_FAIL (-1101)         | Error opening file or writing to an existing file                                                        |
|                          | FILE_READ_FAIL (-1102)         | Error reading file                                                                                        |
|                          | FILE_WRITE_FAIL (-1103)        | Error writing file                                                                                        |
|                          | FILE_INVALID_BUFFER (-1104)    | File buffer is wrong                                                                                      |
|                          | FILE_LOCK_FAIL (-1105)         | File lock operation failed                                                                                |
|                          | FILE_INVALID_HANDLE (-1106)    | Invalid file handle                                                                                       |
| Bucket                   | BUCKET_NOT_FOUND (-1200)       | Storage bucket was not found                                                                               |
|                          | BUCKET_ALREADY_EXISTS (-1201)  | Storage bucket already exists                                                                              |
|                          | KEYS_EXCEED_BUCKET_LIMIT (-1202) | Bucket key count exceeds its configured limit                                                            |
|                          | KEYS_ULTRA_LIMIT (-1203)       | Key count exceeds the hard bucket limit                                                                    |
| Offload                  | UNABLE_OFFLOAD (-1300)         | Offload capability is disabled                                                                             |
|                          | UNABLE_OFFLOADING (-1301)      | Object cannot currently be offloaded                                                                       |
| Task                     | TASK_NOT_FOUND (-1400)         | Task was not found                                                                                          |
|                          | TASK_PENDING_LIMIT_EXCEEDED (-1401) | Pending task count exceeds the configured limit                                                      |
| Tiered Backend           | EMPTY_REPLICAS (-1500)         | No replica is available for tiered storage                                                                  |
|                          | TIER_NOT_FOUND (-1501)         | Requested storage tier was not found                                                                        |
|                          | DATA_COPY_FAILED (-1502)       | Data copy between tiers failed                                                                              |
| Store Lifecycle          | SHUTTING_DOWN (-1600)          | Store is shutting down and rejects new requests                                                             |
|                          | ASYNC_ENQUEUE_FAILED (-1601)   | Asynchronous metadata queue is full or stopped                                                              |
