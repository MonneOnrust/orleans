using System;
using System.Collections.Generic;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Leases.Impl;
using KinesisClientLibrary.ClientLibrary.Types;

namespace KinesisClientLibrary.ClientLibrary.Leases.Interfaces
{
    /**
 * A decoration of ILeaseManager that adds methods to get/update checkpoints.
 */
    public interface IKinesisClientLeaseManager : ILeaseManager<KinesisClientLease>
    {

        /**
         * Gets the current checkpoint of the shard. This is useful in the resharding use case
         * where we will wait for the parent shard to complete before starting on the records from a child shard.
         * 
         * @param shardId Checkpoint of this shard will be returned
         * @return Checkpoint of this shard, or null if the shard record doesn't exist.
         * 
         * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
         * @throws InvalidStateException if lease table does not exist
         * @throws DependencyException if DynamoDB update fails in an unexpected way
         */
        ExtendedSequenceNumber GetCheckpoint(String shardId);
    }
}
