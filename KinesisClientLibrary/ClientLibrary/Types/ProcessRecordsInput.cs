using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Interfaces;

namespace KinesisClientLibrary.ClientLibrary.Types
{
    /**
 * Container for the parameters to the IRecordProcessor's
 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#processRecords(
 * ProcessRecordsInput processRecordsInput) processRecords} method.
 */
    public class ProcessRecordsInput
    {
        /**
         * Get records.
         *
         * @return Data records to be processed
         */
        public List<Record> Records { get; private set; }
        /**
         * Get Checkpointer.
         *
         * @return RecordProcessor should use this instance to checkpoint their progress.
         */
        public IRecordProcessorCheckpointer Checkpointer { get; private set; }
        /**
         * Get milliseconds behind latest.
         *
         * @return The number of milliseconds this batch of records is from the tip of the stream, indicating how far behind current time the record processor is.
         */
        public long MillisBehindLatest { get; private set; }

        /**
         * Default constructor.
         */
        public ProcessRecordsInput()
        {
        }

        /**
         * Set records.
         *
         * @param records Data records to be processed
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public ProcessRecordsInput withRecords(List<Record> records)
        {
            this.Records = records;
            return this;
        }

        /**
         * Set Checkpointer.
         *
         * @param checkpointer RecordProcessor should use this instance to checkpoint their progress.
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public ProcessRecordsInput withCheckpointer(IRecordProcessorCheckpointer checkpointer)
        {
            this.Checkpointer = checkpointer;
            return this;
        }

        /**
         * Set milliseconds behind latest.
         *
         * @param millisBehindLatest The number of milliseconds this batch of records is from the tip of the stream,
         *         indicating how far behind current time the record processor is.
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public ProcessRecordsInput withMillisBehindLatest(long millisBehindLatest)
        {
            this.MillisBehindLatest = millisBehindLatest;
            return this;
        }
    }
}