using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using TestKinesisStreamProvider.ClientLibrary.Exceptions;

namespace TestKinesisStreamProvider.ClientLibrary.Proxies
{
    /**
 * Kinesis proxy - used to make calls to Amazon Kinesis (e.g. fetch data records and list of shards).
 */
    public class KinesisProxy : IKinesisProxyExtended
    {
        //private static readonly Log LOG = LogFactory.getLog(KinesisProxy.class);

        private static readonly List<ShardIteratorType> EXPECTED_ITERATOR_TYPES = new List<ShardIteratorType> { ShardIteratorType.AT_SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER };

        private static String defaultServiceName = "kinesis";
        private static String defaultRegionId = "us-east-1";

        private IAmazonKinesis client;
        private AWSCredentials credentials;
        private List<Shard> listOfShardsSinceLastGet = new List<Shard>();
        private ShardIterationState shardIterationState = null;

        private readonly String streamName;

        private static readonly long DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS = 1000L;
        private static readonly int DEFAULT_DESCRIBE_STREAM_RETRY_TIMES = 50;
        private readonly long describeStreamBackoffTimeInMillis;
        private readonly int maxDescribeStreamRetryAttempts;

        /**
         * Public constructor.
         * 
         * @param streamName Data records will be fetched from this stream
         * @param credentialProvider Provides credentials for signing Kinesis requests
         * @param endpoint Kinesis endpoint
         */

        public KinesisProxy(String streamName, AWSCredentials credentials, String endpoint)
                : this(streamName, credentials, endpoint, defaultServiceName, defaultRegionId, DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES)
        { }

        /**
         * Public constructor.
         * 
         * @param streamName Data records will be fetched from this stream
         * @param credentialProvider Provides credentials for signing Kinesis requests
         * @param endpoint Kinesis endpoint
         * @param serviceName service name
         * @param regionId region id
         * @param describeStreamBackoffTimeInMillis Backoff time for DescribeStream calls in milliseconds
         * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
         */
        public KinesisProxy(String streamName,
                AWSCredentials credentials,
                String endpoint,
                String serviceName,
                String regionId,
                long describeStreamBackoffTimeInMillis,
                int maxDescribeStreamRetryAttempts)
        : this(streamName, credentials, BuildClientSettingEndpoint(credentials, endpoint, serviceName, regionId), describeStreamBackoffTimeInMillis, maxDescribeStreamRetryAttempts)
        {
            Trace.WriteLine("KinesisProxy has created a kinesisClient");
        }

        private static AmazonKinesisClient BuildClientSettingEndpoint(AWSCredentials credentials, String endpoint, String serviceName, String regionId)
        {
            var regionEndpoint = Amazon.RegionEndpoint.GetBySystemName(regionId);
            var config = new AmazonKinesisConfig { RegionEndpoint = regionEndpoint, AuthenticationRegion = regionId };
            AmazonKinesisClient client = new AmazonKinesisClient(credentials, config);
            //client.setEndpoint(endpoint);
            //client.setSignerRegionOverride(regionId);
            return client;
        }

        /**
         * Public constructor.
         * 
         * @param streamName Data records will be fetched from this stream
         * @param credentialProvider Provides credentials for signing Kinesis requests
         * @param kinesisClient Kinesis client (used to fetch data from Kinesis)
         * @param describeStreamBackoffTimeInMillis Backoff time for DescribeStream calls in milliseconds
         * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
         */
        public KinesisProxy(String streamName,
                AWSCredentials credentials,
                IAmazonKinesis kinesisClient,
                long describeStreamBackoffTimeInMillis,
                int maxDescribeStreamRetryAttempts)
        {
            this.streamName = streamName;
            this.credentials = credentials;
            this.describeStreamBackoffTimeInMillis = describeStreamBackoffTimeInMillis;
            this.maxDescribeStreamRetryAttempts = maxDescribeStreamRetryAttempts;
            this.client = kinesisClient;

            Trace.WriteLine("KinesisProxy( " + streamName + ")");
        }

        /**
         * {@inheritDoc}
         */
        public GetRecordsResponse Get(String shardIterator, int maxRecords)
        {
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            //getRecordsRequest.RequestCredentials = (credentialsProvider);
            getRecordsRequest.ShardIterator = (shardIterator);
            getRecordsRequest.Limit = (maxRecords);

            var task = client.GetRecordsAsync(getRecordsRequest);
            task.Wait();
            return task.Result;
        }

        /**
         * {@inheritDoc}
         */
        public DescribeStreamResponse GetStreamInfo(String startShardId)
        {
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
            //describeStreamRequest.RequestCredentials = (credentialsProvider);
            describeStreamRequest.StreamName = (streamName);
            describeStreamRequest.ExclusiveStartShardId = (startShardId);
            DescribeStreamResponse response = null;

            LimitExceededException lastException = null;

            int remainingRetryTimes = this.maxDescribeStreamRetryAttempts;
            // Call DescribeStream, with backoff and retries (if we get LimitExceededException).
            while (response == null)
            {
                try
                {
                    var task = client.DescribeStreamAsync(describeStreamRequest);
                    task.Wait();

                    response = task.Result;
                }
                catch (LimitExceededException le)
                {
                    Trace.TraceInformation("Got LimitExceededException when describing stream " + streamName + ". Backing off for " + this.describeStreamBackoffTimeInMillis + " millis.");
                    //try
                    //{
                        Thread.Sleep((int)this.describeStreamBackoffTimeInMillis);
                    //}
                    //catch (InterruptedException ie)
                    //{
                    //    Trace.WriteLine("Stream " + streamName + " : Sleep  was interrupted ", ie);
                    //}
                    lastException = le;
                }
                remainingRetryTimes--;
                if (remainingRetryTimes <= 0 && response == null)
                {
                    if (lastException != null)
                    {
                        throw lastException;
                    }
                    throw new IllegalStateException("Received null from DescribeStream call.");
                }
            }

            if (StreamStatus.ACTIVE.ToString().Equals(response.StreamDescription.StreamStatus)
             || StreamStatus.UPDATING.ToString().Equals(response.StreamDescription.StreamStatus))
            {
                return response;
            }
            else
            {
                Trace.TraceInformation("Stream is in status " + response.StreamDescription.StreamStatus + ", KinesisProxy.DescribeStream returning null (wait until stream is Active or Updating");
                return null;
            }
        }

        /**
         * {@inheritDoc}
         */
        public Shard GetShard(String shardId)
        {
            if (this.listOfShardsSinceLastGet == null)
            {
                //Update this.listOfShardsSinceLastGet as needed.
                this.GetShardList();
            }

            var list = listOfShardsSinceLastGet;

            foreach (Shard shard in list)
            {
                if (shard.ShardId.Equals(shardId))
                {
                    return shard;
                }
            }

            Trace.TraceWarning("Cannot find the shard given the shardId " + shardId);
            return null;
        }

        /**
         * {@inheritDoc}
         */
        public List<Shard> GetShardList()
        {
            lock (this)
            {
                DescribeStreamResponse response;
                if (shardIterationState == null)
                {
                    shardIterationState = new ShardIterationState();
                }

                do
                {
                    response = GetStreamInfo(shardIterationState.LastShardId);

                    if (response == null)
                    {
                        /*
                         * If getStreamInfo ever returns null, we should bail and return null. This indicates the stream is not
                         * in ACTIVE or UPDATING state and we may not have accurate/consistent information about the stream.
                         */
                        return null;
                    }
                    else
                    {
                        shardIterationState.Update(response.StreamDescription.Shards);
                    }
                } while (response.StreamDescription.HasMoreShards);
                this.listOfShardsSinceLastGet = shardIterationState.Shards;

                shardIterationState = new ShardIterationState();
                return listOfShardsSinceLastGet;
            }
        }

        /**
         * {@inheritDoc}
         */
        public HashSet<String> GetAllShardIds()
        {
            List<Shard> shards = GetShardList();
            if (shards == null)
            {
                return null;
            }
            else
            {
                HashSet<String> shardIds = new HashSet<String>();

                foreach (Shard shard in GetShardList())
                {
                    shardIds.Add(shard.ShardId);
                }

                return shardIds;
            }
        }

        /**
         * {@inheritDoc}
         */
        public String GetIterator(String shardId, String iteratorType, String sequenceNumber)
        {
            ShardIteratorType shardIteratorType;
            try
            {
                shardIteratorType = ShardIteratorType.FindValue(iteratorType);
            }
            catch (ArgumentException iae)
            {
                Trace.TraceError("Caught illegal argument exception while parsing iteratorType: " + iteratorType, iae);
                shardIteratorType = null;
            }

            if (!EXPECTED_ITERATOR_TYPES.Contains(shardIteratorType))
            {
                Trace.TraceInformation("This method should only be used for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER ShardIteratorTypes. For methods to use with other ShardIteratorTypes, see IKinesisProxy.java");
            }
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            //getShardIteratorRequest.RequestCredentials = (credentialsProvider);
            getShardIteratorRequest.StreamName = (streamName);
            getShardIteratorRequest.ShardId = (shardId);
            getShardIteratorRequest.ShardIteratorType = (iteratorType);
            getShardIteratorRequest.StartingSequenceNumber = (sequenceNumber);
            //getShardIteratorRequest.Timestamp = (null);

            var task = client.GetShardIteratorAsync(getShardIteratorRequest);
            task.Wait();
            GetShardIteratorResponse response = task.Result;

            return response.ShardIterator;
        }

        /**
         * {@inheritDoc}
         */
        public String GetIterator(String shardId, String iteratorType)
        {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            //getShardIteratorRequest.RequestCredentials = (credentialsProvider);
            getShardIteratorRequest.StreamName = (streamName);
            getShardIteratorRequest.ShardId = (shardId);
            getShardIteratorRequest.ShardIteratorType = (iteratorType);
            getShardIteratorRequest.StartingSequenceNumber = (null);
            //getShardIteratorRequest.Timestamp = (null);

            var task = client.GetShardIteratorAsync(getShardIteratorRequest);
            task.Wait();
            GetShardIteratorResponse response = task.Result;
            return response.ShardIterator;
        }

        /**
         * {@inheritDoc}
         */
        public String GetIterator(String shardId, DateTime timestamp)
        {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            //getShardIteratorRequest.RequestCredentials = (credentialsProvider);
            getShardIteratorRequest.StreamName = (streamName);
            getShardIteratorRequest.ShardId = (shardId);
            getShardIteratorRequest.ShardIteratorType = (ShardIteratorType.AT_TIMESTAMP);
            getShardIteratorRequest.StartingSequenceNumber = (null);
            getShardIteratorRequest.Timestamp = (timestamp);
            //GetShardIteratorResponse response = client.GetShardIterator(getShardIteratorRequest);
            var task = client.GetShardIteratorAsync(getShardIteratorRequest);
            task.Wait();
            GetShardIteratorResponse response = task.Result;
            return response.ShardIterator;
        }

        /**
         * {@inheritDoc}
         */
        public PutRecordResponse Put(String exclusiveMinimumSequenceNumber, String explicitHashKey, String partitionKey, MemoryStream data)
        {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            //putRecordRequest.RequestCredentials = (credentialsProvider);
            putRecordRequest.StreamName = (streamName);
            putRecordRequest.SequenceNumberForOrdering = (exclusiveMinimumSequenceNumber);
            putRecordRequest.ExplicitHashKey = (explicitHashKey);
            putRecordRequest.PartitionKey = (partitionKey);
            putRecordRequest.Data = (data);

            var task = client.PutRecordAsync(putRecordRequest);
            task.Wait();
            PutRecordResponse response = task.Result;
            return response;
        }

        private class ShardIterationState
        {
            public List<Shard> Shards { get; private set; }
            public String LastShardId { get; private set; }

            public ShardIterationState()
            {
                Shards = new List<Shard>();
            }

            public void Update(List<Shard> shards)
            {
                if (shards == null || shards.Count == 0)
                {
                    return;
                }
                this.Shards.AddRange(shards);

                Shard lastShard = shards.Last();
                if (LastShardId == null || LastShardId.CompareTo(lastShard.ShardId) < 0)
                {
                    LastShardId = lastShard.ShardId;
                }
            }
        }
    }
}