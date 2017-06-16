using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using TestKinesisStreamProvider.ClientLibrary.Interfaces;
using TestKinesisStreamProvider.ClientLibrary.Leases.Impl;
using TestKinesisStreamProvider.ClientLibrary.Leases.Interfaces;
using TestKinesisStreamProvider.ClientLibrary.Leases.Utils;
using TestKinesisStreamProvider.ClientLibrary.Proxies;
using TestKinesisStreamProvider.ClientLibrary.Types;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Worker
{
    /**
 * Task for invoking the RecordProcessor shutdown() callback.
 */
    public class ShutdownTask : ITask
    {
        //private static readonly Log LOG = LogFactory.getLog(ShutdownTask.class);

        //private static readonly String RECORD_PROCESSOR_SHUTDOWN_METRIC = "RecordProcessor.shutdown";

        private readonly ShardInfo shardInfo;
        private readonly IRecordProcessor recordProcessor;
        private readonly RecordProcessorCheckpointer recordProcessorCheckpointer;
        private readonly ShutdownReason reason;
        private readonly IKinesisProxy kinesisProxy;
        private readonly ILeaseManager<KinesisClientLease> leaseManager;
        private readonly InitialPositionInStreamExtended initialPositionInStream;
        private readonly bool cleanupLeasesOfCompletedShards;
        private readonly TaskType taskType = TaskType.SHUTDOWN;
        private readonly long backoffTimeMillis;

        /**
         * Constructor.
         */
        // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
        public ShutdownTask(ShardInfo shardInfo,
                            IRecordProcessor recordProcessor,
                            RecordProcessorCheckpointer recordProcessorCheckpointer,
                            ShutdownReason reason,
                            IKinesisProxy kinesisProxy,
                            InitialPositionInStreamExtended initialPositionInStream,
                            bool cleanupLeasesOfCompletedShards,
                            ILeaseManager<KinesisClientLease> leaseManager,
                            long backoffTimeMillis)
        {
            this.shardInfo = shardInfo;
            this.recordProcessor = recordProcessor;
            this.recordProcessorCheckpointer = recordProcessorCheckpointer;
            this.reason = reason;
            this.kinesisProxy = kinesisProxy;
            this.initialPositionInStream = initialPositionInStream;
            this.cleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
            this.leaseManager = leaseManager;
            this.backoffTimeMillis = backoffTimeMillis;
        }

        /*
         * Invokes RecordProcessor shutdown() API.
         * (non-Javadoc)
         * 
         * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
         */
        public TaskResult Call()
        {
            Exception exception = null;
            bool applicationException = false;

            try
            {
                // If we reached end of the shard, set sequence number to SHARD_END.
                if (reason == ShutdownReason.TERMINATE)
                {
                    recordProcessorCheckpointer.SetSequenceNumberAtShardEnd(recordProcessorCheckpointer.GetLargestPermittedCheckpointValue());
                    recordProcessorCheckpointer.SetLargestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
                }

                Trace.WriteLine("Invoking shutdown() for shard " + shardInfo.getShardId() + ", concurrencyToken " + shardInfo.getConcurrencyToken() + ". Shutdown reason: " + reason);
                ShutdownInput shutdownInput = new ShutdownInput()
                        .WithShutdownReason(reason)
                        .WithCheckpointer(recordProcessorCheckpointer);
                long recordProcessorStartTimeMillis = Time.MilliTime;
                try
                {
                    recordProcessor.Shutdown(shutdownInput);
                    ExtendedSequenceNumber lastCheckpointValue = recordProcessorCheckpointer.GetLastCheckpointValue();

                    if (reason == ShutdownReason.TERMINATE)
                    {
                        if ((lastCheckpointValue == null)
                                || (!lastCheckpointValue.Equals(ExtendedSequenceNumber.SHARD_END)))
                        {
                            throw new ArgumentException("Application didn't checkpoint at end of shard " + shardInfo.getShardId());
                        }
                    }
                    Trace.WriteLine("Record processor completed shutdown() for shard " + shardInfo.getShardId());
                }
                catch (Exception e)
                {
                    applicationException = true;
                    throw e;
                }
                finally
                {
                    //MetricsHelper.addLatency(RECORD_PROCESSOR_SHUTDOWN_METRIC, recordProcessorStartTimeMillis, MetricsLevel.SUMMARY);
                }

                if (reason == ShutdownReason.TERMINATE)
                {
                    Trace.WriteLine("Looking for child shards of shard " + shardInfo.getShardId());
                    // create leases for the child shards
                    ShardSyncer.CheckAndCreateLeasesForNewShards(kinesisProxy, leaseManager, initialPositionInStream, cleanupLeasesOfCompletedShards);
                    Trace.WriteLine("Finished checking for child shards of shard " + shardInfo.getShardId());
                }

                return new TaskResult(null);
            }
            catch (Exception e)
            {
                if (applicationException)
                {
                    Trace.TraceError("Application exception. ", e);
                }
                else
                {
                    Trace.TraceError("Caught exception: ", e);
                }
                exception = e;
                // backoff if we encounter an exception.
                //try
                //{
                    Thread.Sleep((int)this.backoffTimeMillis);
                //}
                //catch (InterruptedException ie)
                //{
                //    LOG.debug("Interrupted sleep", ie);
                //}
            }

            return new TaskResult(exception);
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#getTaskType()
         */
        public TaskType TaskType
        {
            get { return taskType; }
        }

        //@VisibleForTesting
        ShutdownReason GetReason()
        {
            return reason;
        }
    }
}