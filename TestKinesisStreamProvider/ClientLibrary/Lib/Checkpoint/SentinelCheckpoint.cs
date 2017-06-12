using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Checkpoint
{
    /**
 * Enumeration of the sentinel values of checkpoints.
 * Used during initialization of ShardConsumers to determine the starting point
 * in the shard and to flag that a shard has been completely processed.
 */
    public enum SentinelCheckpoint
    {
        /**
         * Start from the first available record in the shard.
         */
        TRIM_HORIZON,
        /**
         * Start from the latest record in the shard.
         */
        LATEST,
        /**
         * We've completely processed all records in this shard.
         */
        SHARD_END,
        /**
         * Start from the record at or after the specified server-side timestamp.
         */
        AT_TIMESTAMP
    }
}