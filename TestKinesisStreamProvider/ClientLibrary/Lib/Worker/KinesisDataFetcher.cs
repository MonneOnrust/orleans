using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using TestKinesisStreamProvider.ClientLibrary.Lib.Checkpoint;
using TestKinesisStreamProvider.ClientLibrary.Proxies;
using TestKinesisStreamProvider.ClientLibrary.Types;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Worker
{
    /**
 * Used to get data from Amazon Kinesis. Tracks iterator state internally.
 */
    public class KinesisDataFetcher
    {
        private String nextIterator;
        private IKinesisProxy kinesisProxy;
        private readonly String shardId;
        private bool isShardEndReached;
        private bool isInitialized;

        /**
         *
         * @param kinesisProxy Kinesis proxy
         * @param shardInfo The shardInfo object.
         */
        public KinesisDataFetcher(IKinesisProxy kinesisProxy, ShardInfo shardInfo)
        {
            this.shardId = shardInfo.getShardId();
            this.kinesisProxy = kinesisProxy; //new MetricsCollectingKinesisProxyDecorator("KinesisDataFetcher", kinesisProxy, this.shardId);
        }

        /**
         * Get records from the current position in the stream (up to maxRecords).
         *
         * @param maxRecords Max records to fetch
         * @return list of records of up to maxRecords size
         */
        public GetRecordsResponse GetRecords(int maxRecords)
        {
            if (!isInitialized)
            {
                throw new ArgumentException("KinesisDataFetcher.getRecords called before initialization.");
            }

            GetRecordsResponse response = null;
            if (nextIterator != null)
            {
                try
                {
                    response = kinesisProxy.Get(nextIterator, maxRecords);
                    nextIterator = response.NextShardIterator;
                }
                catch (ResourceNotFoundException)
                {
                    Trace.TraceInformation("Caught ResourceNotFoundException when fetching records for shard " + shardId);
                    nextIterator = null;
                }
                if (nextIterator == null)
                {
                    isShardEndReached = true;
                }
            }
            else
            {
                isShardEndReached = true;
            }

            return response;
        }

        /**
         * Initializes this KinesisDataFetcher's iterator based on the checkpointed sequence number.
         * @param initialCheckpoint Current checkpoint sequence number for this shard.
         * @param initialPositionInStream The initialPositionInStream.
         */
        public void Initialize(String initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream)
        {
            Trace.TraceInformation("Initializing shard " + shardId + " with " + initialCheckpoint);
            AdvanceIteratorTo(initialCheckpoint, initialPositionInStream);
            isInitialized = true;
        }

        public void Initialize(ExtendedSequenceNumber initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream)
        {
            Trace.TraceInformation("Initializing shard " + shardId + " with " + initialCheckpoint.SequenceNumber);
            AdvanceIteratorTo(initialCheckpoint.SequenceNumber, initialPositionInStream);
            isInitialized = true;
        }

        /**
         * Advances this KinesisDataFetcher's internal iterator to be at the passed-in sequence number.
         *
         * @param sequenceNumber advance the iterator to the record at this sequence number.
         * @param initialPositionInStream The initialPositionInStream.
         */
        void AdvanceIteratorTo(String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream)
        {
            if (sequenceNumber == null)
            {
                throw new ArgumentException("SequenceNumber should not be null: shardId " + shardId);
            }
            else if (sequenceNumber.Equals(SentinelCheckpoint.LATEST.ToString()))
            {
                nextIterator = GetIterator(ShardIteratorType.LATEST.ToString());
            }
            else if (sequenceNumber.Equals(SentinelCheckpoint.TRIM_HORIZON.ToString()))
            {
                nextIterator = GetIterator(ShardIteratorType.TRIM_HORIZON.ToString());
            }
            else if (sequenceNumber.Equals(SentinelCheckpoint.AT_TIMESTAMP.ToString()))
            {
                nextIterator = GetIterator(initialPositionInStream.Timestamp);
            }
            else if (sequenceNumber.Equals(SentinelCheckpoint.SHARD_END.ToString()))
            {
                nextIterator = null;
            }
            else
            {
                nextIterator = GetIterator(ShardIteratorType.AT_SEQUENCE_NUMBER.ToString(), sequenceNumber);
            }
            if (nextIterator == null)
            {
                isShardEndReached = true;
            }
        }

        /**
         * @param iteratorType The iteratorType - either AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER.
         * @param sequenceNumber The sequenceNumber.
         *
         * @return iterator or null if we catch a ResourceNotFound exception
         */
        private String GetIterator(String iteratorType, String sequenceNumber)
        {
            String iterator = null;
            try
            {
                Trace.WriteLine("Calling getIterator for " + shardId + ", iterator type " + iteratorType + " and sequence number " + sequenceNumber);
                iterator = kinesisProxy.GetIterator(shardId, iteratorType, sequenceNumber);
            }
            catch (ResourceNotFoundException e)
            {
                Trace.TraceInformation("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
            }
            return iterator;
        }

        /**
         * @param iteratorType The iteratorType - either TRIM_HORIZON or LATEST.
         * @return iterator or null if we catch a ResourceNotFound exception
         */
        private String GetIterator(String iteratorType)
        {
            String iterator = null;
            try
            {
                Trace.WriteLine("Calling getIterator for " + shardId + " and iterator type " + iteratorType);
                iterator = kinesisProxy.GetIterator(shardId, iteratorType);
            }
            catch (ResourceNotFoundException e)
            {
                Trace.TraceInformation("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
            }
            return iterator;
        }

        /**
         * @param timestamp The timestamp.
         * @return iterator or null if we catch a ResourceNotFound exception
         */
        private String GetIterator(DateTime timestamp)
        {
            String iterator = null;
            try
            {
                Trace.WriteLine("Calling getIterator for " + shardId + " and timestamp " + timestamp);
                iterator = kinesisProxy.GetIterator(shardId, timestamp);
            }
            catch (ResourceNotFoundException e)
            {
                Trace.TraceInformation("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
            }
            return iterator;
        }
    }
}