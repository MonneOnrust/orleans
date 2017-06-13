using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using TestKinesisStreamProvider.ClientLibrary.Interfaces;
using TestKinesisStreamProvider.ClientLibrary.Types;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Worker
{
    /**
 * This class is used to enable RecordProcessors to checkpoint their progress.
 * The Amazon Kinesis Client Library will instantiate an object and provide a reference to the application
 * RecordProcessor instance. Amazon Kinesis Client Library will create one instance per shard assignment.
 */
    public class RecordProcessorCheckpointer : IRecordProcessorCheckpointer
    {
        //private static final Log LOG = LogFactory.getLog(RecordProcessorCheckpointer.class);

        private ICheckpoint checkpoint;

        private ExtendedSequenceNumber largestPermittedCheckpointValue;
        // Set to the last value set via checkpoint().
        // Sample use: verify application shutdown() invoked checkpoint() at the end of a shard.
        private ExtendedSequenceNumber lastCheckpointValue;

        private ShardInfo shardInfo;

        private SequenceNumberValidator sequenceNumberValidator;

        private ExtendedSequenceNumber sequenceNumberAtShardEnd;

        /**
         * Only has package level access, since only the Amazon Kinesis Client Library should be creating these.
         *
         * @param checkpoint Used to checkpoint progress of a RecordProcessor
         * @param validator Used for validating sequence numbers
         */
        RecordProcessorCheckpointer(ShardInfo shardInfo, ICheckpoint checkpoint, SequenceNumberValidator validator)
        {
            this.shardInfo = shardInfo;
            this.checkpoint = checkpoint;
            this.sequenceNumberValidator = validator;
        }

        /**
         * {@inheritDoc}
         */
        public void Checkpoint()
        {
            lock (this)
            {
                Trace.WriteLine("Checkpointing " + shardInfo.getShardId() + ", " + " token " + shardInfo.getConcurrencyToken() + " at largest permitted value " + this.largestPermittedCheckpointValue);
                AdvancePosition(this.largestPermittedCheckpointValue);
            }
        }

        /**
         * {@inheritDoc}
         */
        public void Checkpoint(Record record)
        {
            lock (this)
            {
                if (record == null)
                {
                    throw new ArgumentException("Could not checkpoint a null record");
                }
                else if (record is UserRecord)
                {
                    checkpoint(record.SequenceNumber, ((UserRecord)record).getSubSequenceNumber());
                }
                else
                {
                    checkpoint(record.SequenceNumber, 0);
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        public void Checkpoint(String sequenceNumber)
        {
            lock (this)
            {
                Checkpoint(sequenceNumber, 0);
            }
        }

        /**
         * {@inheritDoc}
         */
        public void Checkpoint(String sequenceNumber, long subSequenceNumber)
        {
            lock (this)
            {
                if (subSequenceNumber < 0)
                {
                    throw new ArgumentException("Could not checkpoint at invalid, negative subsequence number " + subSequenceNumber);
                }

                // throws exception if sequence number shouldn't be checkpointed for this shard
                sequenceNumberValidator.validateSequenceNumber(sequenceNumber);

                Trace.WriteLine("Validated checkpoint sequence number " + sequenceNumber + " for " + shardInfo.getShardId() + ", token " + shardInfo.getConcurrencyToken());

                /*
                 * If there isn't a last checkpoint value, we only care about checking the upper bound.
                 * If there is a last checkpoint value, we want to check both the lower and upper bound.
                 */
                ExtendedSequenceNumber newCheckpoint = new ExtendedSequenceNumber(sequenceNumber, subSequenceNumber);
                if ((lastCheckpointValue.CompareTo(newCheckpoint) <= 0)
                        && newCheckpoint.CompareTo(largestPermittedCheckpointValue) <= 0)
                {

                    Trace.WriteLine("Checkpointing " + shardInfo.getShardId() + ", token " + shardInfo.getConcurrencyToken() + " at specific extended sequence number " + newCheckpoint);
                    this.AdvancePosition(newCheckpoint);
                }
                else
                {
                    throw new ArgumentException(String.Format(
                            "Could not checkpoint at extended sequence number {0} as it did not fall into acceptable range "
                            + "between the last checkpoint {1} and the greatest extended sequence number passed to this "
                            + "record processor {2}",
                            newCheckpoint, this.lastCheckpointValue, this.largestPermittedCheckpointValue));
                }
            }
        }

        /**
         * @return the lastCheckpointValue
         */
        ExtendedSequenceNumber GetLastCheckpointValue()
        {
            return lastCheckpointValue;
        }


        void SetInitialCheckpointValue(ExtendedSequenceNumber initialCheckpoint)
        {
            lock (this)
            {
                lastCheckpointValue = initialCheckpoint;
            }
        }

        /**
         * Used for testing.
         *
         * @return the largest permitted checkpoint
         */
        ExtendedSequenceNumber GetLargestPermittedCheckpointValue()
        {
            lock (this)
            {
                return largestPermittedCheckpointValue;
            }
        }

        /**
         * @param checkpoint the checkpoint value to set
         */
        void SetLargestPermittedCheckpointValue(ExtendedSequenceNumber largestPermittedCheckpointValue)
        {
            lock (this)
            {
                this.largestPermittedCheckpointValue = largestPermittedCheckpointValue;
            }
        }

        /**
         * Used to remember the last extended sequence number before SHARD_END to allow us to prevent the checkpointer
         * from checkpointing at the end of the shard twice (i.e. at the last extended sequence number and then again
         * at SHARD_END).
         *
         * @param extendedSequenceNumber
         */
        void SetSequenceNumberAtShardEnd(ExtendedSequenceNumber extendedSequenceNumber)
        {
            lock (this)
            {
                this.sequenceNumberAtShardEnd = extendedSequenceNumber;
            }
        }


        /**
         * Internal API - has package level access only for testing purposes.
         *
         * @param sequenceNumber
         *
         * @throws KinesisClientLibDependencyException
         * @throws ThrottlingException
         * @throws ShutdownException
         * @throws InvalidStateException
         */
        void AdvancePosition(String sequenceNumber)
        {
            AdvancePosition(new ExtendedSequenceNumber(sequenceNumber));
        }

        void AdvancePosition(ExtendedSequenceNumber extendedSequenceNumber)
        {
            ExtendedSequenceNumber checkpointToRecord = extendedSequenceNumber;
            if (sequenceNumberAtShardEnd != null && sequenceNumberAtShardEnd.Equals(extendedSequenceNumber))
            {
                // If we are about to checkpoint the very last sequence number for this shard, we might as well
                // just checkpoint at SHARD_END
                checkpointToRecord = ExtendedSequenceNumber.SHARD_END;
            }
            // Don't checkpoint a value we already successfully checkpointed
            if (extendedSequenceNumber != null && !extendedSequenceNumber.Equals(lastCheckpointValue))
            {
                try
                {
                    Trace.WriteLine("Setting " + shardInfo.getShardId() + ", token " + shardInfo.getConcurrencyToken() + " checkpoint to " + checkpointToRecord);
                    checkpoint.SetCheckpoint(shardInfo.getShardId(), checkpointToRecord, shardInfo.getConcurrencyToken());
                    lastCheckpointValue = checkpointToRecord;
                }
                catch (ThrottlingException | ShutdownException | InvalidStateException | KinesisClientLibDependencyException e) 
                {
                    throw e;
                }
                catch (KinesisClientLibException e)
                {
                    Trace.WriteLine("Caught exception setting checkpoint.", e);
                    throw new KinesisClientLibDependencyException("Caught exception while checkpointing", e);
                }
            }
        }
    }
}