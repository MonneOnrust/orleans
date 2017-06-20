using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Proxies
{
    /**
 * Kinesis proxy interface. Operates on a single stream (set up at initialization).
 */
    public interface IKinesisProxy
    {

        /**
         * Get records from stream.
         * 
         * @param shardIterator Fetch data records using this shard iterator
         * @param maxRecords Fetch at most this many records
         * @return List of data records from Kinesis.
         * @throws InvalidArgumentException Invalid input parameters
         * @throws ResourceNotFoundException The Kinesis stream or shard was not found
         * @throws ExpiredIteratorException The iterator has expired
         */
        GetRecordsResponse Get(String shardIterator, int maxRecords);

        /**
         * Fetch information about stream. Useful for fetching the list of shards in a stream.
         * 
         * @param startShardId exclusive start shardId - used when paginating the list of shards.
         * @return DescribeStreamOutput object containing a description of the stream.
         * @throws ResourceNotFoundException The Kinesis stream was not found
         */
        DescribeStreamResponse GetStreamInfo(String startShardId);

        /**
         * Fetch the shardIds of all shards in the stream.
         * 
         * @return Set of all shardIds
         * @throws ResourceNotFoundException If the specified Kinesis stream was not found
         */
        HashSet<String> GetAllShardIds();

        /**
         * Fetch all the shards defined for the stream (e.g. obtained via calls to the DescribeStream API).
         * This can be used to discover new shards and consume data from them.
         * 
         * @return List of all shards in the Kinesis stream.
         * @throws ResourceNotFoundException The Kinesis stream was not found.
         */
        List<Shard> GetShardList();

        /**
         * Fetch a shard iterator from the specified position in the shard.
         * This is to fetch a shard iterator for ShardIteratorType AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER which
         * requires the starting sequence number.
         *
         * NOTE: Currently this method continues to fetch iterators for ShardIteratorTypes TRIM_HORIZON, LATEST,
         * AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER.
         * But this behavior will change in the next release, after which this method will only serve
         * AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER ShardIteratorTypes.
         * We recommend users who call this method directly to use the appropriate getIterator method based on the
         * ShardIteratorType.
         *
         * @param shardId Shard id
         * @param iteratorEnum one of: TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER
         * @param sequenceNumber the sequence number - must be null unless iteratorEnum is AT_SEQUENCE_NUMBER or
         *        AFTER_SEQUENCE_NUMBER
         * @return shard iterator which can be used to read data from Kinesis.
         * @throws ResourceNotFoundException The Kinesis stream or shard was not found
         * @throws InvalidArgumentException Invalid input parameters
         */
        String GetIterator(String shardId, String iteratorEnum, String sequenceNumber);

        /**
         * Fetch a shard iterator from the specified position in the shard.
         * This is to fetch a shard iterator for ShardIteratorType LATEST or TRIM_HORIZON which doesn't require a starting
         * sequence number.
         *
         * @param shardId Shard id
         * @param iteratorEnum Either TRIM_HORIZON or LATEST.
         * @return shard iterator which can be used to read data from Kinesis.
         * @throws ResourceNotFoundException The Kinesis stream or shard was not found
         * @throws InvalidArgumentException Invalid input parameters
         */
        String GetIterator(String shardId, String iteratorEnum);

        /**
         * Fetch a shard iterator from the specified position in the shard.
         * This is to fetch a shard iterator for ShardIteratorType AT_TIMESTAMP which requires the timestamp field.
         *
         * @param shardId   Shard id
         * @param timestamp The timestamp.
         * @return shard iterator which can be used to read data from Kinesis.
         * @throws ResourceNotFoundException The Kinesis stream or shard was not found
         * @throws InvalidArgumentException  Invalid input parameters
         */
        String GetIterator(String shardId, DateTime timestamp);

        /**
         * @param sequenceNumberForOrdering (optional) used for record ordering
         * @param explicitHashKey optionally supplied transformation of partitionkey
         * @param partitionKey for this record
         * @param data payload
         * @return PutRecordResponse (contains the Kinesis sequence number of the record).
         * @throws ResourceNotFoundException The Kinesis stream was not found.
         * @throws InvalidArgumentException InvalidArgumentException.
         */
        PutRecordResponse Put(String sequenceNumberForOrdering, String explicitHashKey, String partitionKey, MemoryStream data);
    }
}