using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using KinesisClientLibrary.ClientLibrary.Exceptions;
using KinesisClientLibrary.ClientLibrary.Interfaces;
using KinesisClientLibrary.ClientLibrary.Leases.Utils;
using KinesisClientLibrary.ClientLibrary.Proxies;
using KinesisClientLibrary.ClientLibrary.Types;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
 * Task for fetching data records and invoking processRecords() on the record processor instance.
 */
    public class ProcessTask : ITask
    {

        //private static readonly Log LOG = LogFactory.getLog(ProcessTask.class);

        //private static readonly String EXPIRED_ITERATOR_METRIC = "ExpiredIterator";
        //private static readonly String DATA_BYTES_PROCESSED_METRIC = "DataBytesProcessed";
        //private static readonly String RECORDS_PROCESSED_METRIC = "RecordsProcessed";
        //private static readonly String MILLIS_BEHIND_LATEST_METRIC = "MillisBehindLatest";
        //private static readonly String RECORD_PROCESSOR_PROCESS_RECORDS_METRIC = "RecordProcessor.processRecords";
        private static readonly int MAX_CONSECUTIVE_THROTTLES = 5;

        private readonly ShardInfo shardInfo;
        private readonly IRecordProcessor recordProcessor;
        private readonly RecordProcessorCheckpointer recordProcessorCheckpointer;
        private readonly KinesisDataFetcher dataFetcher;
        private readonly TaskType taskType = TaskType.PROCESS;
        private readonly StreamConfig streamConfig;
        private readonly long backoffTimeMillis;
        private readonly Shard shard;
        private readonly ThrottlingReporter throttlingReporter;

        /**
         * @param shardInfo
         *            contains information about the shard
         * @param streamConfig
         *            Stream configuration
         * @param recordProcessor
         *            Record processor used to process the data records for the shard
         * @param recordProcessorCheckpointer
         *            Passed to the RecordProcessor so it can checkpoint progress
         * @param dataFetcher
         *            Kinesis data fetcher (used to fetch records from Kinesis)
         * @param backoffTimeMillis
         *            backoff time when catching exceptions
         */
        public ProcessTask(ShardInfo shardInfo, StreamConfig streamConfig, IRecordProcessor recordProcessor,
                           RecordProcessorCheckpointer recordProcessorCheckpointer, KinesisDataFetcher dataFetcher,
                           long backoffTimeMillis, bool skipShardSyncAtWorkerInitializationIfLeasesExist)
            : this(shardInfo, streamConfig, recordProcessor, recordProcessorCheckpointer, dataFetcher, backoffTimeMillis,
                    skipShardSyncAtWorkerInitializationIfLeasesExist, new ThrottlingReporter(MAX_CONSECUTIVE_THROTTLES, shardInfo.getShardId()))
        { }

        /**
         * @param shardInfo
         *            contains information about the shard
         * @param streamConfig
         *            Stream configuration
         * @param recordProcessor
         *            Record processor used to process the data records for the shard
         * @param recordProcessorCheckpointer
         *            Passed to the RecordProcessor so it can checkpoint progress
         * @param dataFetcher
         *            Kinesis data fetcher (used to fetch records from Kinesis)
         * @param backoffTimeMillis
         *            backoff time when catching exceptions
         * @param throttlingReporter
         *            determines how throttling events should be reported in the log.
         */
        public ProcessTask(ShardInfo shardInfo, StreamConfig streamConfig, IRecordProcessor recordProcessor,
                RecordProcessorCheckpointer recordProcessorCheckpointer, KinesisDataFetcher dataFetcher,
                long backoffTimeMillis, bool skipShardSyncAtWorkerInitializationIfLeasesExist, ThrottlingReporter throttlingReporter) : base()
        {
            this.shardInfo = shardInfo;
            this.recordProcessor = recordProcessor;
            this.recordProcessorCheckpointer = recordProcessorCheckpointer;
            this.dataFetcher = dataFetcher;
            this.streamConfig = streamConfig;
            this.backoffTimeMillis = backoffTimeMillis;
            this.throttlingReporter = throttlingReporter;
            IKinesisProxy kinesisProxy = this.streamConfig.StreamProxy;
            // If skipShardSyncAtWorkerInitializationIfLeasesExist is set, we will not get the shard for
            // this ProcessTask. In this case, duplicate KPL user records in the event of resharding will
            // not be dropped during deaggregation of Amazon Kinesis records. This is only applicable if
            // KPL is used for ingestion and KPL's aggregation feature is used.
            if (!skipShardSyncAtWorkerInitializationIfLeasesExist && kinesisProxy is IKinesisProxyExtended)
            {
                this.shard = ((IKinesisProxyExtended)kinesisProxy).GetShard(this.shardInfo.getShardId());
            }
            else
            {
                this.shard = null;
            }
            if (this.shard == null && !skipShardSyncAtWorkerInitializationIfLeasesExist)
            {
                Trace.TraceWarning("Cannot get the shard for this ProcessTask, so duplicate KPL user records "
                        + "in the event of resharding will not be dropped during deaggregation of Amazon Kinesis records.");
            }
        }

        /*
         * (non-Javadoc)
         *
         * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
         */
        public TaskResult Call()
        {
            long startTimeMillis = Time.MilliTime;
            //IMetricsScope scope = MetricsHelper.getMetricsScope();
            //scope.addDimension(MetricsHelper.SHARD_ID_DIMENSION_NAME, shardInfo.getShardId());
            //scope.addData(RECORDS_PROCESSED_METRIC, 0, StandardUnit.Count, MetricsLevel.SUMMARY);
            //scope.addData(DATA_BYTES_PROCESSED_METRIC, 0, StandardUnit.Bytes, MetricsLevel.SUMMARY);

            Exception exception = null;

            try
            {
                if (dataFetcher.IsShardEndReached)
                {
                    Trace.TraceInformation("Reached end of shard " + shardInfo.getShardId());
                    return new TaskResult(null, true);
                }

                GetRecordsResponse getRecordsResult = GetRecordsResult();
                throttlingReporter.Success();
                List<Record> records = getRecordsResult.Records;

                if (records.Count != 0)
                {
                    //scope.addData(RECORDS_PROCESSED_METRIC, records.size(), StandardUnit.Count, MetricsLevel.SUMMARY);
                }
                else
                {
                    HandleNoRecords(startTimeMillis);
                }
                records = DeaggregateRecords(records);

                recordProcessorCheckpointer.SetLargestPermittedCheckpointValue(
                        FilterAndGetMaxExtendedSequenceNumber(/*scope, */records, recordProcessorCheckpointer.GetLastCheckpointValue(), recordProcessorCheckpointer.GetLargestPermittedCheckpointValue()));

                if (ShouldCallProcessRecords(records))
                {
                    CallProcessRecords(getRecordsResult, records);
                }
            }
            catch (ProvisionedThroughputExceededException pte)
            {
                throttlingReporter.Throttled();
                exception = pte;
                backoff();

            }
            catch (RuntimeException e)
            {
                Trace.TraceError("ShardId " + shardInfo.getShardId() + ": Caught exception: ", e);
                exception = e;
                backoff();
            }

            return new TaskResult(exception);
        }

        /**
         * Sleeps for the configured backoff period. This is usually only called when an exception occurs.
         */
        private void backoff()
        {
            // backoff if we encounter an exception.
            //try
            //{
            Thread.Sleep((int)this.backoffTimeMillis);
            //}
            //catch (InterruptedException ie)
            //{
            //    LOG.debug(shardInfo.getShardId() + ": Sleep was interrupted", ie);
            //}
        }

        /**
         * Dispatches a batch of records to the record processor, and handles any fallout from that.
         * 
         * @param getRecordsResult
         *            the result of the last call to Kinesis
         * @param records
         *            the records to be dispatched. It's possible the records have been adjusted by KPL deaggregation.
         */
        private void CallProcessRecords(GetRecordsResponse getRecordsResult, List<Record> records)
        {
            Trace.WriteLine("Calling application processRecords() with " + records.Count + " records from " + shardInfo.getShardId());
            ProcessRecordsInput processRecordsInput = new ProcessRecordsInput().withRecords(records)
                                                                               .withCheckpointer(recordProcessorCheckpointer)
                                                                               .withMillisBehindLatest(getRecordsResult.MillisBehindLatest);

            long recordProcessorStartTimeMillis = Time.MilliTime;
            try
            {
                recordProcessor.ProcessRecords(processRecordsInput);
            }
            catch (Exception e)
            {
                Trace.TraceError("ShardId " + shardInfo.getShardId() + ": Application processRecords() threw an exception when processing shard ", e);
                Trace.TraceError("ShardId " + shardInfo.getShardId() + ": Skipping over the following data records: " + records);
            }
            finally
            {
                //MetricsHelper.addLatencyPerShard(shardInfo.getShardId(), RECORD_PROCESSOR_PROCESS_RECORDS_METRIC, recordProcessorStartTimeMillis, MetricsLevel.SUMMARY);
            }
        }

        /**
         * Whether we should call process records or not
         * 
         * @param records
         *            the records returned from the call to Kinesis, and/or deaggregation
         * @return true if the set of records should be dispatched to the record process, false if they should not.
         */
        private bool ShouldCallProcessRecords(List<Record> records)
        {
            return records.Count != 0 || streamConfig.CallProcessRecordsEvenForEmptyRecordList;
        }

        /**
         * Determines whether to deaggregate the given records, and if they are KPL records dispatches them to deaggregation
         * 
         * @param records
         *            the records to deaggregate is deaggregation is required.
         * @return returns either the deaggregated records, or the original records
         */
        //@SuppressWarnings("unchecked")
        private List<Record> DeaggregateRecords(List<Record> records)
        {
            // We deaggregate if and only if we got actual Kinesis records, i.e.
            // not instances of some subclass thereof.
            if (records.Count != 0 && records[0].GetType().Equals(typeof(Record)))
            {
                if (this.shard != null)
                {
                    return UserRecord.Deaggregate(records, long.Parse(this.shard.HashKeyRange.StartingHashKey), //new BigInteger(this.shard.HashKeyRange.StartingHashKey),
                                                           long.Parse(this.shard.HashKeyRange.EndingHashKey)) //new BigInteger(this.shard.HashKeyRange.EndingHashKey));
                                                           .Cast<Record>().ToList();
                }
                else
                {
                    return UserRecord.Deaggregate(records).Cast<Record>().ToList(); 
                }
            }
            return records;
        }

        /**
         * Emits metrics, and sleeps if there are no records available
         * 
         * @param startTimeMillis
         *            the time when the task started
         */
        private void HandleNoRecords(long startTimeMillis)
        {
            Trace.WriteLine("Kinesis didn't return any records for shard " + shardInfo.getShardId());

            long sleepTimeMillis = streamConfig.IdleTimeInMilliseconds - (Time.MilliTime - startTimeMillis);
            if (sleepTimeMillis > 0)
            {
                sleepTimeMillis = Math.Max(sleepTimeMillis, streamConfig.IdleTimeInMilliseconds);
                //try
                //{
                Trace.WriteLine("Sleeping for " + sleepTimeMillis + " ms since there were no new records in shard " + shardInfo.getShardId());
                Thread.Sleep((int)sleepTimeMillis);
                //}
                //catch (InterruptedException e)
                //{
                //    LOG.debug("ShardId " + shardInfo.getShardId() + ": Sleep was interrupted");
                //}
            }
        }

        public TaskType TaskType
        {
            get { return taskType; }
        }

        /**
         * Scans a list of records to filter out records up to and including the most recent checkpoint value and to get
         * the greatest extended sequence number from the retained records. Also emits metrics about the records.
         *
         * @param scope metrics scope to emit metrics into
         * @param records list of records to scan and change in-place as needed
         * @param lastCheckpointValue the most recent checkpoint value
         * @param lastLargestPermittedCheckpointValue previous largest permitted checkpoint value
         * @return the largest extended sequence number among the retained records
         */
        private ExtendedSequenceNumber FilterAndGetMaxExtendedSequenceNumber(/*IMetricsScope scope, */List<Record> records,
                                                        ExtendedSequenceNumber lastCheckpointValue, ExtendedSequenceNumber lastLargestPermittedCheckpointValue)
        {
            ExtendedSequenceNumber largestExtendedSequenceNumber = lastLargestPermittedCheckpointValue;

            for (int i = records.Count - 1; i >= 0; i--)
            {
                Record record = records[i];
                ExtendedSequenceNumber extendedSequenceNumber = new ExtendedSequenceNumber(record.SequenceNumber,
                                                                                           record is UserRecord ? ((UserRecord)record).getSubSequenceNumber() : 0L);

                if (extendedSequenceNumber.CompareTo(lastCheckpointValue) <= 0)
                {
                    records.RemoveAt(i);
                    Trace.WriteLine("removing record with ESN " + extendedSequenceNumber + " because the ESN is <= checkpoint (" + lastCheckpointValue + ")");
                    continue;
                }

                if (largestExtendedSequenceNumber == null || largestExtendedSequenceNumber.CompareTo(extendedSequenceNumber) < 0)
                {
                    largestExtendedSequenceNumber = extendedSequenceNumber;
                }

                //scope.addData(DATA_BYTES_PROCESSED_METRIC, record.getData().limit(), StandardUnit.Bytes, MetricsLevel.SUMMARY);
            }

            return largestExtendedSequenceNumber;
        }

        /**
         * Gets records from Kinesis and retries once in the event of an ExpiredIteratorException.
         *
         * @return list of data records from Kinesis
         */
        private GetRecordsResponse GetRecordsResult()
        {
            try
            {
                return GetRecordsResultAndRecordMillisBehindLatest();
            }
            catch (ExpiredIteratorException e)
            {
                // If we see a ExpiredIteratorException, try once to restart from the greatest remembered sequence number
                Trace.TraceInformation("ShardId " + shardInfo.getShardId() + ": getRecords threw ExpiredIteratorException - restarting after greatest seqNum passed to customer", e);
                //MetricsHelper.getMetricsScope().addData(EXPIRED_ITERATOR_METRIC, 1, StandardUnit.Count, MetricsLevel.SUMMARY);

                /*
                 * Advance the iterator to after the greatest processed sequence number (remembered by
                 * recordProcessorCheckpointer).
                 */
                dataFetcher.AdvanceIteratorTo(recordProcessorCheckpointer.GetLargestPermittedCheckpointValue().SequenceNumber, streamConfig.InitialPositionInStream);

                // Try a second time - if we fail this time, expose the failure.
                try
                {
                    return GetRecordsResultAndRecordMillisBehindLatest();
                }
                catch (ExpiredIteratorException ex)
                {
                    String msg = "Shard " + shardInfo.getShardId() + ": getRecords threw ExpiredIteratorException with a fresh iterator.";
                    Trace.TraceError(msg, ex);
                    throw ex;
                }
            }
        }

        /**
         * Gets records from Kinesis and records the MillisBehindLatest metric if present.
         *
         * @return list of data records from Kinesis
         */
        private GetRecordsResponse GetRecordsResultAndRecordMillisBehindLatest()
        {
            GetRecordsResponse getRecordsResult = dataFetcher.GetRecords(streamConfig.MaxRecords);

            if (getRecordsResult == null)
            {
                // Stream no longer exists
                return new GetRecordsResponse() { Records = new List<Record>() };
            }

            //if (getRecordsResult.MillisBehindLatest != null)
            //{
            //MetricsHelper.getMetricsScope().addData(MILLIS_BEHIND_LATEST_METRIC, getRecordsResult.getMillisBehindLatest(), StandardUnit.Milliseconds, MetricsLevel.SUMMARY);
            //}

            return getRecordsResult;
        }
    }
}