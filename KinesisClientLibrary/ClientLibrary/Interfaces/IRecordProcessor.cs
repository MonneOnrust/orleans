using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Lib.Worker;
using KinesisClientLibrary.ClientLibrary.Types;

namespace KinesisClientLibrary.ClientLibrary.Interfaces
{
    /**
 * The Amazon Kinesis Client Library will instantiate record processors to process data records fetched from Amazon
 * Kinesis.
 */
    public interface IRecordProcessor
    {

        /**
         * Invoked by the Amazon Kinesis Client Library before data records are delivered to the RecordProcessor instance
         * (via processRecords).
         *
         * @param initializationInput Provides information related to initialization 
         */
        void Initialize(InitializationInput initializationInput);

        /**
         * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
         * application.
         * Upon fail over, the new instance will get records with sequence number > checkpoint position
         * for each partition key.
         *
         * @param processRecordsInput Provides the records to be processed as well as information and capabilities related
         *        to them (eg checkpointing).
         */
        void ProcessRecords(ProcessRecordsInput processRecordsInput);

        /**
         * Invoked by the Amazon Kinesis Client Library to indicate it will no longer send data records to this
         * RecordProcessor instance.
         *
         * <h2><b>Warning</b></h2>
         *
         * When the value of {@link ShutdownInput#getShutdownReason()} is
         * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason#TERMINATE} it is required that you
         * checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
         *
         * @param shutdownInput
         *            Provides information and capabilities (eg checkpointing) related to shutdown of this record processor.
         */
        void Shutdown(ShutdownInput shutdownInput);
    }
}