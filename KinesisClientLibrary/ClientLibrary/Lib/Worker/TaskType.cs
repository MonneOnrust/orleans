using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
 * Enumerates types of tasks executed as part of processing a shard.
 */
    public enum TaskType
    {
        /**
         * Polls and waits until parent shard(s) have been fully processed.
         */
        BLOCK_ON_PARENT_SHARDS,
        /**
         * Initialization of RecordProcessor (and Amazon Kinesis Client Library internal state for a shard).
         */
        INITIALIZE,
        /**
         * Fetching and processing of records.
         */
        PROCESS,
        /**
         * Shutdown of RecordProcessor.
         */
        SHUTDOWN,
        /**
         * Graceful shutdown has been requested, and notification of the record processor will occur.
         */
        SHUTDOWN_NOTIFICATION,
        /**
         * Occurs once the shutdown has been completed
         */
        SHUTDOWN_COMPLETE,
        /**
         * Sync leases/activities corresponding to Kinesis shards.
         */
        SHARDSYNC
    }
}