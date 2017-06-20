using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using KinesisClientLibrary.ClientLibrary.Interfaces;
using KinesisClientLibrary.ClientLibrary.Leases.Utils;
using KinesisClientLibrary.ClientLibrary.Types;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
 * Task for initializing shard position and invoking the RecordProcessor initialize() API.
 */
    public class InitializeTask : ITask
    {
        //private static readonly Log LOG = LogFactory.getLog(InitializeTask.class);

        //private static readonly String RECORD_PROCESSOR_INITIALIZE_METRIC = "RecordProcessor.initialize";

        private readonly ShardInfo shardInfo;
        private readonly IRecordProcessor recordProcessor;
        private readonly KinesisDataFetcher dataFetcher;
        private readonly TaskType taskType = TaskType.INITIALIZE;
        private readonly ICheckpoint checkpoint;
        private readonly RecordProcessorCheckpointer recordProcessorCheckpointer;
        // Back off for this interval if we encounter a problem (exception)
        private readonly long backoffTimeMillis;
        private readonly StreamConfig streamConfig;

        /**
         * Constructor.
         */
        public InitializeTask(ShardInfo shardInfo,
                            IRecordProcessor recordProcessor,
                            ICheckpoint checkpoint,
                            RecordProcessorCheckpointer recordProcessorCheckpointer,
                            KinesisDataFetcher dataFetcher,
                            long backoffTimeMillis,
                            StreamConfig streamConfig)
        {
            this.shardInfo = shardInfo;
            this.recordProcessor = recordProcessor;
            this.checkpoint = checkpoint;
            this.recordProcessorCheckpointer = recordProcessorCheckpointer;
            this.dataFetcher = dataFetcher;
            this.backoffTimeMillis = backoffTimeMillis;
            this.streamConfig = streamConfig;
        }

        /*
         * Initializes the data fetcher (position in shard) and invokes the RecordProcessor initialize() API.
         * (non-Javadoc)
         *
         * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
         */
        public TaskResult Call()
        {
            bool applicationException = false;
            Exception exception = null;

            try
            {
                Trace.WriteLine("Initializing ShardId " + shardInfo.getShardId());
                ExtendedSequenceNumber initialCheckpoint = checkpoint.GetCheckpoint(shardInfo.getShardId());

                dataFetcher.Initialize(initialCheckpoint.SequenceNumber, streamConfig.InitialPositionInStream);
                recordProcessorCheckpointer.SetLargestPermittedCheckpointValue(initialCheckpoint);
                recordProcessorCheckpointer.SetInitialCheckpointValue(initialCheckpoint);

                Trace.WriteLine("Calling the record processor initialize().");
                InitializationInput initializationInput = new InitializationInput()
                    .withShardId(shardInfo.getShardId())
                    .withExtendedSequenceNumber(initialCheckpoint);
                long recordProcessorStartTimeMillis = Time.MilliTime;
                try
                {
                    recordProcessor.Initialize(initializationInput);
                    Trace.WriteLine("Record processor initialize() completed.");
                }
                catch (Exception e)
                {
                    applicationException = true;
                    throw e;
                }
                finally
                {
                    //MetricsHelper.addLatency(RECORD_PROCESSOR_INITIALIZE_METRIC, recordProcessorStartTimeMillis, MetricsLevel.SUMMARY);
                }

                return new TaskResult(null);
            }
            catch (Exception e)
            {
                if (applicationException)
                {
                    Trace.TraceError("Application initialize() threw exception: ", e);
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
                //    Trace.WriteLine("Interrupted sleep", ie);
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
    }
}