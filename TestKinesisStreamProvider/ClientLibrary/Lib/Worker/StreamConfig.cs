using System;
using System.Collections.Generic;
using System.Text;
using TestKinesisStreamProvider.ClientLibrary.Proxies;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Worker
{
    /**
 * Used to capture stream configuration and pass it along.
 */
    public class StreamConfig
    {
        public IKinesisProxy StreamProxy { get; private set; }
        public int MaxRecords { get; private set; }
        public long IdleTimeInMilliseconds { get; private set; }
        public bool CallProcessRecordsEvenForEmptyRecordList { get; private set; }
        public InitialPositionInStreamExtended InitialPositionInStream { get; private set; }
        public bool ValidateSequenceNumberBeforeCheckpointing { get; private set; }

        /**
         * @param proxy Used to fetch records and information about the stream
         * @param maxRecords Max records to be fetched in a call
         * @param idleTimeInMilliseconds Idle time between get calls to the stream
         * @param callProcessRecordsEvenForEmptyRecordList Call the IRecordProcessor::processRecords() API even if
         *        GetRecords returned an empty record list.
         * @param validateSequenceNumberBeforeCheckpointing Whether to call Amazon Kinesis to validate sequence numbers
         * @param initialPositionInStream Initial position in stream
         */
        StreamConfig(IKinesisProxy proxy,
                int maxRecords,
                long idleTimeInMilliseconds,
                bool callProcessRecordsEvenForEmptyRecordList,
                bool validateSequenceNumberBeforeCheckpointing,
                InitialPositionInStreamExtended initialPositionInStream)
        {
            this.StreamProxy = proxy;
            this.MaxRecords = maxRecords;
            this.IdleTimeInMilliseconds = idleTimeInMilliseconds;
            this.CallProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList;
            this.ValidateSequenceNumberBeforeCheckpointing = validateSequenceNumberBeforeCheckpointing;
            this.InitialPositionInStream = initialPositionInStream;
        }
    }
}