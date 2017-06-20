using System;
using System.Collections.Generic;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Types;

namespace KinesisClientLibrary.ClientLibrary.Interfaces
{
    /**
 * Interface for checkpoint trackers.
 */
    public interface ICheckpoint
    {

        /**
         * Record a checkpoint for a shard (e.g. sequence and subsequence numbers of last record processed 
         * by application). Upon failover, record processing is resumed from this point.
         * 
         * @param shardId Checkpoint is specified for this shard.
         * @param checkpointValue Value of the checkpoint (e.g. Kinesis sequence number and subsequence number)
         * @param concurrencyToken Used with conditional writes to prevent stale updates
         *        (e.g. if there was a fail over to a different record processor, we don't want to 
         *        overwrite it's checkpoint)
         * @throws KinesisClientLibException Thrown if we were unable to save the checkpoint
         */
        void SetCheckpoint(String shardId, ExtendedSequenceNumber checkpointValue, String concurrencyToken);

        /**
         * Get the current checkpoint stored for the specified shard. Useful for checking that the parent shard
         * has been completely processed before we start processing the child shard.
         * 
         * @param shardId Current checkpoint for this shard is fetched
         * @return Current checkpoint for this shard, null if there is no record for this shard.
         * @throws KinesisClientLibException Thrown if we are unable to fetch the checkpoint
         */
        ExtendedSequenceNumber GetCheckpoint(String shardId);
    }
}