using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using TestKinesisStreamProvider.ClientLibrary.Exceptions;
using TestKinesisStreamProvider.ClientLibrary.Leases.Impl;
using TestKinesisStreamProvider.ClientLibrary.Leases.Interfaces;
using TestKinesisStreamProvider.ClientLibrary.Types;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Worker
{
    /**
     * Task to block until processing of all data records in the parent shard(s) is completed.
     * We check if we have checkpoint(s) for the parent shard(s).
     * If a checkpoint for a parent shard is found, we poll and wait until the checkpoint value is SHARD_END
     *    (application has checkpointed after processing all records in the shard).
     * If we don't find a checkpoint for the parent shard(s), we assume they have been trimmed and directly
     * proceed with processing data from the shard.
     */
    public class BlockOnParentShardTask : ITask
    {
        //private static readonly Log LOG = LogFactory.getLog(BlockOnParentShardTask.class);
        private readonly ShardInfo shardInfo;
        private readonly ILeaseManager<KinesisClientLease> leaseManager;

        private readonly TaskType taskType = TaskType.BLOCK_ON_PARENT_SHARDS;
        // Sleep for this duration if the parent shards have not completed processing, or we encounter an exception.
        private readonly long parentShardPollIntervalMillis;

        /**
         * @param shardInfo Information about the shard we are working on
         * @param leaseManager Used to fetch the lease and checkpoint info for parent shards
         * @param parentShardPollIntervalMillis Sleep time if the parent shard has not completed processing
         */
        public BlockOnParentShardTask(ShardInfo shardInfo, ILeaseManager<KinesisClientLease> leaseManager, long parentShardPollIntervalMillis)
        {
            this.shardInfo = shardInfo;
            this.leaseManager = leaseManager;
            this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        }

        /* (non-Javadoc)
         * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
         */
        public TaskResult Call()
        {
            Exception exception = null;

            try
            {
                bool blockedOnParentShard = false;
                foreach (String shardId in shardInfo.getParentShardIds())
                {
                    KinesisClientLease lease = leaseManager.GetLease(shardId);
                    if (lease != null)
                    {
                        ExtendedSequenceNumber checkpoint = lease.Checkpoint;
                        if ((checkpoint == null) || (!checkpoint.Equals(ExtendedSequenceNumber.SHARD_END)))
                        {
                            Trace.WriteLine("Shard " + shardId + " is not yet done. Its current checkpoint is " + checkpoint);
                            blockedOnParentShard = true;
                            exception = new BlockedOnParentShardException("Parent shard not yet done");
                            break;
                        }
                        else
                        {
                            Trace.WriteLine("Shard " + shardId + " has been completely processed.");
                        }
                    }
                    else
                    {
                        Trace.TraceInformation("No lease found for shard " + shardId + ". Not blocking on completion of this shard.");
                    }
                }

                if (!blockedOnParentShard)
                {
                    Trace.TraceInformation("No need to block on parents " + shardInfo.getParentShardIds() + " of shard " + shardInfo.getShardId());
                    return new TaskResult(null);
                }
            }
            catch (Exception e)
            {
                Trace.TraceError("Caught exception when checking for parent shard checkpoint", e);
                exception = e;
            }
            //try
            //{
                Thread.Sleep((int)parentShardPollIntervalMillis);
            //}
            //catch (InterruptedException e)
            //{
            //    Trace.TraceError("Sleep interrupted when waiting on parent shard(s) of " + shardInfo.getShardId(), e);
            //}

            return new TaskResult(exception);
        }

        /* (non-Javadoc)
         * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#getTaskType()
         */
        public TaskType TaskType
        {
            get { return taskType; }
        }
    }
}