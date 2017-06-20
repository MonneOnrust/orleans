using System;
using System.Collections.Generic;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Types;
using KinesisClientLibrary.ClientLibrary.Utils;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
 * Used to pass shard related info among different classes and as a key to the map of shard consumers.
 */
    public class ShardInfo
    {
        private readonly String shardId;
        private readonly String concurrencyToken;
        // Sorted list of parent shardIds.
        private readonly List<String> parentShardIds;
        private readonly ExtendedSequenceNumber checkpoint;

        /**
         * Creates a new ShardInfo object. The checkpoint is not part of the equality, but is used for debugging output.
         * 
         * @param shardId
         *            Kinesis shardId that this will be about
         * @param concurrencyToken
         *            Used to differentiate between lost and reclaimed leases
         * @param parentShardIds
         *            Parent shards of the shard identified by Kinesis shardId
         * @param checkpoint
         *            the latest checkpoint from lease
         */
        public ShardInfo(String shardId, String concurrencyToken, List<String> parentShardIds, ExtendedSequenceNumber checkpoint)
        {
            this.shardId = shardId;
            this.concurrencyToken = concurrencyToken;
            this.parentShardIds = new List<String>();
            if (parentShardIds != null)
            {
                this.parentShardIds.AddRange(parentShardIds);
            }
            // ShardInfo stores parent shard Ids in canonical order in the parentShardIds list.
            // This makes it easy to check for equality in ShardInfo.equals method.
            this.parentShardIds.Sort();
            this.checkpoint = checkpoint;
        }

        /**
         * The shardId that this ShardInfo contains data about
         * 
         * @return the shardId
         */
        public String getShardId()
        {
            return shardId;
        }

        /**
         * Concurrency token for the lease that this shard is part of
         *
         * @return the concurrencyToken
         */
        public String getConcurrencyToken()
        {
            return concurrencyToken;
        }

        /**
         * A list of shards that are parents of this shard. This may be empty if the shard has no parents.
         * 
         * @return a list of shardId's that are parents of this shard, or empty if the shard has no parents.
         */
        public List<String> getParentShardIds()
        {
            return new List<String>(parentShardIds);
        }

        /**
         * Whether the shard has been completely processed or not.
         *
         * @return completion status of the shard
         */
        protected bool isCompleted()
        {
            return ExtendedSequenceNumber.SHARD_END.Equals(checkpoint);
        }

        /**
         * {@inheritDoc}
         */
        public override int GetHashCode()
        {
            return HashCodeBuilder.ComputeHashFrom(concurrencyToken, parentShardIds, shardId);
        }

        /**
         * This method assumes parentShardIds is ordered. The Worker.cleanupShardConsumers() method relies on this method
         * returning true for ShardInfo objects which may have been instantiated with parentShardIds in a different order
         * (and rest of the fields being the equal). For example shardInfo1.equals(shardInfo2) should return true with
         * shardInfo1 and shardInfo2 defined as follows.
         * ShardInfo shardInfo1 = new ShardInfo(shardId1, concurrencyToken1, Arrays.asList("parent1", "parent2"));
         * ShardInfo shardInfo2 = new ShardInfo(shardId1, concurrencyToken1, Arrays.asList("parent2", "parent1"));
         */
        public override bool Equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (GetType() != obj.GetType())
            {
                return false;
            }
            ShardInfo other = (ShardInfo)obj;

            if ((concurrencyToken != null ^ other.concurrencyToken != null) || !concurrencyToken.Equals(other.concurrencyToken)) return false;
            if ((shardId != null ^ other.shardId != null) || !shardId.Equals(other.shardId)) return false;
            if ((parentShardIds != null ^ other.parentShardIds != null)) return false;

            if (parentShardIds == null) return true;

            if (parentShardIds.Count != other.parentShardIds.Count) return false;

            for (int i = 0; i < parentShardIds.Count; i++)
            {
                if (!parentShardIds[i].Equals(other.parentShardIds[i])) return false;
            }

            return true;
        }

        public override String ToString()
        {
            return "ShardInfo [shardId=" + shardId + ", concurrencyToken=" + concurrencyToken + ", parentShardIds=" + parentShardIds + ", checkpoint=" + checkpoint + "]";
        }
    }
}