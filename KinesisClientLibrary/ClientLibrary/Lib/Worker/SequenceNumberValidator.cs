using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using KinesisClientLibrary.ClientLibrary.Exceptions;
using KinesisClientLibrary.ClientLibrary.Proxies;
using KinesisClientLibrary.ClientLibrary.Types;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
 * This class provides some methods for validating sequence numbers. It provides a method
 * {@link #validateSequenceNumber(String)} which validates a sequence number by attempting to get an iterator from
 * Amazon Kinesis for that sequence number. (e.g. Before checkpointing a client provided sequence number in
 * {@link RecordProcessorCheckpointer#checkpoint(String)} to prevent invalid sequence numbers from being checkpointed,
 * which could prevent another shard consumer instance from processing the shard later on). This class also provides a
 * utility function {@link #isDigits(String)} which is used to check whether a string is all digits
 */
    public class SequenceNumberValidator
    {
        //private static final Log LOG = LogFactory.getLog(SequenceNumberValidator.class);

        private IKinesisProxy proxy;
        private String shardId;
        private bool validateWithGetIterator;
        private static readonly int SERVER_SIDE_ERROR_CODE = 500;

        /**
         * Constructor.
         * 
         * @param proxy Kinesis proxy to be used for getIterator call
         * @param shardId ShardId to check with sequence numbers
         * @param validateWithGetIterator Whether to attempt to get an iterator for this shard id and the sequence numbers
         *        being validated
         */
        public SequenceNumberValidator(IKinesisProxy proxy, String shardId, bool validateWithGetIterator)
        {
            this.proxy = proxy;
            this.shardId = shardId;
            this.validateWithGetIterator = validateWithGetIterator;
        }

        /**
         * Validates the sequence number by attempting to get an iterator from Amazon Kinesis. Repackages exceptions from
         * Amazon Kinesis into the appropriate KCL exception to allow clients to determine exception handling strategies
         * 
         * @param sequenceNumber The sequence number to be validated. Must be a numeric string
         * @throws IllegalArgumentException Thrown when sequence number validation fails.
         * @throws ThrottlingException Thrown when GetShardIterator returns a ProvisionedThroughputExceededException which
         *         indicates that too many getIterator calls are being made for this shard.
         * @throws KinesisClientLibDependencyException Thrown when a service side error is received. This way clients have
         *         the option of retrying
         */
        public void ValidateSequenceNumber(String sequenceNumber)
        {
            if (!IsDigits(sequenceNumber))
            {
                Trace.TraceInformation("Sequence number must be numeric, but was " + sequenceNumber);
                throw new ArgumentException("Sequence number must be numeric, but was " + sequenceNumber);
            }
            try
            {
                if (validateWithGetIterator)
                {
                    proxy.GetIterator(shardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER.ToString(), sequenceNumber);
                    Trace.TraceInformation("Validated sequence number " + sequenceNumber + " with shard id " + shardId);
                }
            }
            catch (InvalidArgumentException e)
            {
                Trace.TraceInformation("Sequence number " + sequenceNumber + " is invalid for shard " + shardId, e);
                throw new ArgumentException("Sequence number " + sequenceNumber + " is invalid for shard " + shardId, e);
            }
            catch (ProvisionedThroughputExceededException e)
            {
                // clients should have back off logic in their checkpoint logic
                Trace.TraceInformation("Exceeded throughput while getting an iterator for shard " + shardId, e);
                throw new ThrottlingException("Exceeded throughput while getting an iterator for shard " + shardId, e);
            }
            catch (AmazonServiceException e)
            {
                Trace.TraceInformation("Encountered service exception while getting an iterator for shard " + shardId, e);
                if ((int)e.StatusCode >= SERVER_SIDE_ERROR_CODE)
                {
                    // clients can choose whether to retry in their checkpoint logic
                    throw new KinesisClientLibDependencyException("Encountered service exception while getting an iterator"
                            + " for shard " + shardId, e);
                }
                // Just throw any other exceptions, e.g. 400 errors caused by the client
                throw e;
            }
        }

        public void ValidateSequenceNumber(ExtendedSequenceNumber checkpoint)
        {
            ValidateSequenceNumber(checkpoint.SequenceNumber);
            if (checkpoint.SubSequenceNumber < 0)
            {
                throw new ArgumentException("SubSequence number must be non-negative, but was " + checkpoint.SubSequenceNumber);
            }
        }

        /**
         * Checks if the string is composed of only digits.
         * 
         * @param string
         * @return true for a string of all digits, false otherwise (including false for null and empty string)
         */
        static bool IsDigits(String @string)
        {
            if (@string == null || @string.Length == 0)
            {
                return false;
            }
            for (int i = 0; i < @string.Length; i++)
            {
                if (!char.IsDigit(@string[i]))
                {
                    return false;
                }
            }
            return true;
        }
    }

}
