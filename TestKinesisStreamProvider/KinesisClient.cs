using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace TestKinesisStreamProvider
{
    public class KinesisClient
    {
        private string ShardID;
        private string SequenceNumber;

        private string streamName;
        private AmazonKinesisClient client;

        private AmazonKinesisConfig KinesisConfig
        {
            get
            {
                var config = new AmazonKinesisConfig
                {
                    ServiceURL = "http://localhost:4567"
                };

                return config;
            }
        }

        public KinesisClient(string streamName)
        {
            this.streamName = streamName;
            this.client = new AmazonKinesisClient("bla", "bla", KinesisConfig);
        }

        public async Task EnsureStreamExists()
        {
            var streams = await client.ListStreamsAsync();

            if (!streams.StreamNames.Contains(streamName))
            {
                var createRequest = new CreateStreamRequest { StreamName = streamName, ShardCount = 1 };
                var createResponse = await client.CreateStreamAsync(createRequest);
                createResponse.EnsureSuccessResponse();
            }

            var describeRequest = new DescribeStreamRequest { StreamName = streamName };
            var describeResponse = await client.DescribeStreamAsync(describeRequest);
            describeResponse.EnsureSuccessResponse();

            ShardID = describeResponse.StreamDescription.Shards.First().ShardId;
            SequenceNumber = describeResponse.StreamDescription.Shards.First().SequenceNumberRange.StartingSequenceNumber;
        }

        public async Task PutRecordAsync(KinesisQueueMessage message)
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            {
                writer.Write(message.Content);
                writer.Flush();
                stream.Position = 0;

                var putRequest = new PutRecordRequest
                {
                    StreamName = streamName,
                    Data = stream,
                    PartitionKey = "A"
                };

                var putResponse = await client.PutRecordAsync(putRequest);
                putResponse.EnsureSuccessResponse();

                Console.WriteLine($"Put record finished, sequencenumber: {putResponse.SequenceNumber}");
            }
        }

        public async Task<KinesisQueueMessage[]> GetRecordsAsync(int count, TimeSpan? messageVisibilityTimeout)
        {
            var iteratorRequest = new GetShardIteratorRequest { StreamName = streamName, ShardIteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER, ShardId = ShardID, StartingSequenceNumber = SequenceNumber };
            var iteratorResponse = await client.GetShardIteratorAsync(iteratorRequest);
            iteratorResponse.EnsureSuccessResponse();

            var getRequest = new GetRecordsRequest { ShardIterator = iteratorResponse.ShardIterator, Limit = count };
            var getResponse = await client.GetRecordsAsync(getRequest);
            getResponse.EnsureSuccessResponse();

            string data = "";

            foreach (var record in getResponse.Records)
            {
                using (var reader = new StreamReader(record.Data))
                {
                    data = reader.ReadToEnd();
                }

                Console.WriteLine($"Get record finished, sequencenumber: {record.SequenceNumber}, data: {data}");
            }

            return new[] { new KinesisQueueMessage(getResponse.Records.First().Data.ToArray()) };
        }
    }

    public class KinesisQueueMessage
    {
        public const int MaxNumberOfMessagesToPeek = 1; // TODO mon: real implementation

        public KinesisQueueMessage(byte[] content)
        {
            this.Content = content;
        }

        public byte[] Content { get; private set; }
    }

    public class KinesisUtils
    {
        public static void ValidateQueueName(string name)
        {
            // TODO mon: real implementation
        }

        public static KinesisClient GetKinesisClient(string streamName, string connectionstring, IRetryPolicy retryPolicy, TimeSpan timout, Logger logger)
        {
            // TODO mon: moet dit per shard?

            return new KinesisClient(streamName);
        }
    }

    public interface IRetryPolicy { }// TODO mon: real implementation
    public class LinearRetry : IRetryPolicy
    {
        public LinearRetry(TimeSpan pause, int max)
        {

        }
    }
}
