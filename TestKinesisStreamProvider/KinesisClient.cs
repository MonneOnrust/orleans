using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace TestKinesisStreamProvider
{
    public class KinesisClient
    {
        private AmazonKinesisClient client;
        private string streamName;

        private string ShardID;
        private string SequenceNumber;

        public KinesisClient(string streamName, string serviceUrl = "http://localhost:4567")
        {
            this.streamName = streamName;

            var config = new AmazonKinesisConfig { ServiceURL = serviceUrl };
            this.client = new AmazonKinesisClient("bla", "bla", config);
        }

        public async Task InitStreamAsync()
        {
            var streams = await client.ListStreamsAsync();

            if (!streams.StreamNames.Contains(streamName))
            {
                await CreateStream(streamName, 1);
            }

            var describeResponse = await DescribeStream(streamName);

            ShardID = describeResponse.StreamDescription.Shards.First().ShardId;
            SequenceNumber = describeResponse.StreamDescription.Shards.First().SequenceNumberRange.StartingSequenceNumber;
        }

        public async Task<List<Shard>> GetShardsAsync()
        {
            var response = await DescribeStream(streamName);

            return response.StreamDescription.Shards;
        }

        public async Task PutRecordAsync(KinesisStreamMessage message)
        {
            using (var stream = new MemoryStream())
            {
                stream.Write(message.Content, 0, message.Content.Length);
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

        public async Task<KinesisStreamMessage[]> GetRecordsAsync(int count, TimeSpan? messageVisibilityTimeout)
        {
            var iteratorRequest = new GetShardIteratorRequest { StreamName = streamName, ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardId = ShardID, StartingSequenceNumber = SequenceNumber };
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
                    SequenceNumber = record.SequenceNumber;
                }

                Console.WriteLine($"Get record finished, sequencenumber: {record.SequenceNumber}, data: {data}");
            }

            if (getResponse.Records.Count > 0)
            {
                return new[] { new KinesisStreamMessage(getResponse.Records.First().Data.ToArray()) };
            }
            else return new KinesisStreamMessage[0];
        }

        private async Task<CreateStreamResponse> CreateStream(string streamName, int shardCount)
        {
            var createRequest = new CreateStreamRequest { StreamName = streamName, ShardCount = shardCount };
            var createResponse = await client.CreateStreamAsync(createRequest);
            createResponse.EnsureSuccessResponse();

            return createResponse;
        }

        private async Task<DescribeStreamResponse> DescribeStream(string streamName)
        {
            var describeRequest = new DescribeStreamRequest { StreamName = streamName };
            var describeResponse = await client.DescribeStreamAsync(describeRequest);
            describeResponse.EnsureSuccessResponse();

            return describeResponse;
        }
    }

    public class KinesisStreamMessage
    {
        public const int MaxNumberOfMessagesToPeek = 1; // TODO mon: real implementation

        public KinesisStreamMessage(byte[] content)
        {
            this.Content = content;
        }

        public byte[] Content { get; private set; }
    }

    public class KinesisUtils
    {
        public static void ValidateStreamName(string name)
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
