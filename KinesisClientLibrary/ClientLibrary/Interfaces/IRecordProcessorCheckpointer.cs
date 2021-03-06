﻿using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Interfaces
{
    /**
 * Used by RecordProcessors when they want to checkpoint their progress.
 * The Amazon Kinesis Client Library will pass an object implementing this interface to RecordProcessors, so they can
 * checkpoint their progress.
 */
    public interface IRecordProcessorCheckpointer
    {

        /**
         * This method will checkpoint the progress at the last data record that was delivered to the record processor.
         * Upon fail over (after a successful checkpoint() call), the new/replacement RecordProcessor instance
         * will receive data records whose sequenceNumber > checkpoint position (for each partition key).
         * In steady state, applications should checkpoint periodically (e.g. once every 5 minutes).
         * Calling this API too frequently can slow down the application (because it puts pressure on the underlying
         * checkpoint storage layer).
         * 
         * @throws ThrottlingException Can't store checkpoint. Can be caused by checkpointing too frequently.
         *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
         * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
         *         started processing some of these records already.
         *         The application should abort processing via this RecordProcessor instance.
         * @throws InvalidStateException Can't store checkpoint.
         *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
         * @throws KinesisClientLibDependencyException Encountered an issue when storing the checkpoint. The application can
         *         backoff and retry.
         */
        void Checkpoint();

        /**
         * This method will checkpoint the progress at the provided record. This method is analogous to
         * {@link #checkpoint()} but provides the ability to specify the record at which to
         * checkpoint.
         * 
         * @param record A record at which to checkpoint in this shard. Upon failover,
         *        the Kinesis Client Library will start fetching records after this record's sequence number.
         * @throws ThrottlingException Can't store checkpoint. Can be caused by checkpointing too frequently.
         *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
         * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
         *         started processing some of these records already.
         *         The application should abort processing via this RecordProcessor instance.
         * @throws InvalidStateException Can't store checkpoint.
         *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
         * @throws KinesisClientLibDependencyException Encountered an issue when storing the checkpoint. The application can
         *         backoff and retry.
         */
        void Checkpoint(Record record);

        /**
         * This method will checkpoint the progress at the provided sequenceNumber. This method is analogous to
         * {@link #checkpoint()} but provides the ability to specify the sequence number at which to
         * checkpoint.
         * 
         * @param sequenceNumber A sequence number at which to checkpoint in this shard. Upon failover,
         *        the Kinesis Client Library will start fetching records after this sequence number.
         * @throws ThrottlingException Can't store checkpoint. Can be caused by checkpointing too frequently.
         *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
         * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
         *         started processing some of these records already.
         *         The application should abort processing via this RecordProcessor instance.
         * @throws InvalidStateException Can't store checkpoint.
         *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
         * @throws KinesisClientLibDependencyException Encountered an issue when storing the checkpoint. The application can
         *         backoff and retry.
         * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
         *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
         *         greatest sequence number seen by the associated record processor.
         *         2.) It is not a valid sequence number for a record in this shard.
         */
        void Checkpoint(String sequenceNumber);

        /**
         * This method will checkpoint the progress at the provided sequenceNumber and subSequenceNumber, the latter for
         * aggregated records produced with the Producer Library. This method is analogous to {@link #checkpoint()} 
         * but provides the ability to specify the sequence and subsequence numbers at which to checkpoint.
         * 
         * @param sequenceNumber A sequence number at which to checkpoint in this shard. Upon failover, the Kinesis
         *        Client Library will start fetching records after the given sequence and subsequence numbers.
         * @param subSequenceNumber A subsequence number at which to checkpoint within this shard. Upon failover, the
         *        Kinesis Client Library will start fetching records after the given sequence and subsequence numbers.
         * @throws ThrottlingException Can't store checkpoint. Can be caused by checkpointing too frequently.
         *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
         * @throws ShutdownException The record processor instance has been shutdown. Another instance may have
         *         started processing some of these records already.
         *         The application should abort processing via this RecordProcessor instance.
         * @throws InvalidStateException Can't store checkpoint.
         *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
         * @throws KinesisClientLibDependencyException Encountered an issue when storing the checkpoint. The application can
         *         backoff and retry.
         * @throws IllegalArgumentException The sequence number is invalid for one of the following reasons:
         *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
         *         greatest sequence number seen by the associated record processor.
         *         2.) It is not a valid sequence number for a record in this shard.
         */
        void Checkpoint(String sequenceNumber, long subSequenceNumber);
    }

}
