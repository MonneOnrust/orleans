using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.IO;

namespace KinesisClientLibrary.ClientLibrary.Types
{
    /**
 * This class represents a KPL user record.
 */
    //@SuppressWarnings("serial")
    public class UserRecord : Record
    {
        //private static final Log LOG = LogFactory.getLog(UserRecord.class);

        //private static readonly byte[] AGGREGATED_RECORD_MAGIC = new byte[] { -13, -119, -102, -62 };
        private static readonly byte[] AGGREGATED_RECORD_MAGIC = new byte[] { 243, 137, 154, 194 };
        private static readonly int DIGEST_SIZE = 16;
        private static readonly long SMALLEST_HASH_KEY = 0L;
        // largest hash key = 2^128-1
        private static readonly long LARGEST_HASH_KEY = (long)(Math.Pow(2, 128) - 1); //(StringUtils.repeat("FF", 16), 16);

        private readonly long subSequenceNumber;
        private readonly String explicitHashKey;
        private readonly bool aggregated;

        /**
         * Create a User Record from a Kinesis Record.
         *
         * @param record Kinesis record
         */
        public UserRecord(Record record) : this(false, record, 0, null)
        {
        }

        /**
         * Create a User Record.
         * 
         * @param aggregated whether the record is aggregated
         * @param record Kinesis record
         * @param subSequenceNumber subsequence number
         * @param explicitHashKey explicit hash key
         */
        protected UserRecord(bool aggregated, Record record, long subSequenceNumber, String explicitHashKey)
        {
            if (subSequenceNumber < 0)
            {
                throw new ArgumentException("Cannot have an invalid, negative subsequence number");
            }

            this.aggregated = aggregated;
            this.subSequenceNumber = subSequenceNumber;
            this.explicitHashKey = explicitHashKey;

            this.SequenceNumber = (record.SequenceNumber);
            this.Data = (record.Data);
            this.PartitionKey = (record.PartitionKey);
            this.ApproximateArrivalTimestamp = (record.ApproximateArrivalTimestamp);
        }

        /**
         * @return subSequenceNumber of this UserRecord.
         */
        public long getSubSequenceNumber()
        {
            return subSequenceNumber;
        }

        /**
         * @return explicitHashKey of this UserRecord.
         */
        public String getExplicitHashKey()
        {
            return explicitHashKey;
        }

        /**
         * @return a boolean indicating whether this UserRecord is aggregated.
         */
        public bool isAggregated()
        {
            return aggregated;
        }

        /**
         * @return the String representation of this UserRecord.
         */
        public override String ToString()
        {
            return "UserRecord [subSequenceNumber=" + subSequenceNumber + ", explicitHashKey=" + explicitHashKey
                    + ", aggregated=" + aggregated + ", getSequenceNumber()=" + SequenceNumber + ", getData()="
                    + Data + ", getPartitionKey()=" + PartitionKey + "]";
        }

        /**
         * {@inheritDoc}
         */
        public override int GetHashCode()
        {
            const int prime = 31;
            int result = base.GetHashCode();
            result = prime * result + (aggregated ? 1231 : 1237);
            result = prime * result + ((explicitHashKey == null) ? 0 : explicitHashKey.GetHashCode());
            result = prime * result + (int)(subSequenceNumber ^ (subSequenceNumber >>/*>*/ 32));
            return result;
        }

        /**
         * {@inheritDoc}
         */
        public override bool Equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!base.Equals(obj))
            {
                return false;
            }
            if (GetType() != obj.GetType())
            {
                return false;
            }
            UserRecord other = (UserRecord)obj;
            if (aggregated != other.aggregated)
            {
                return false;
            }
            if (explicitHashKey == null)
            {
                if (other.explicitHashKey != null)
                {
                    return false;
                }
            }
            else if (!explicitHashKey.Equals(other.explicitHashKey))
            {
                return false;
            }
            if (subSequenceNumber != other.subSequenceNumber)
            {
                return false;
            }
            return true;
        }

        //private static byte[] md5(byte[] data)
        //{
        //    try
        //    {
        //        MessageDigest d = MessageDigest.getInstance("MD5");
        //        return d.digest(data);
        //    }
        //    catch (NoSuchAlgorithmException e)
        //    {
        //        throw new RuntimeException(e);
        //    }
        //}

        /**
         * This method deaggregates the given list of Amazon Kinesis records into a
         * list of KPL user records. This method will then return the resulting list
         * of KPL user records.
         * 
         * @param records
         *            A list of Amazon Kinesis records, each possibly aggregated.
         * @return A resulting list of deaggregated KPL user records.
         */
        public static List<UserRecord> Deaggregate(List<Record> records)
        {
            return Deaggregate(records, SMALLEST_HASH_KEY, LARGEST_HASH_KEY);
        }

        /**
         * This method deaggregates the given list of Amazon Kinesis records into a
         * list of KPL user records. Any KPL user records whose explicit hash key or
         * partition key falls outside the range of the startingHashKey and the
         * endingHashKey are discarded from the resulting list. This method will
         * then return the resulting list of KPL user records.
         * 
         * @param records
         *            A list of Amazon Kinesis records, each possibly aggregated.
         * @param startingHashKey
         *            A BigInteger representing the starting hash key that the
         *            explicit hash keys or partition keys of retained resulting KPL
         *            user records must be greater than or equal to.
         * @param endingHashKey
         *            A BigInteger representing the ending hash key that the the
         *            explicit hash keys or partition keys of retained resulting KPL
         *            user records must be smaller than or equal to.
         * @return A resulting list of KPL user records whose explicit hash keys or
         *          partition keys fall within the range of the startingHashKey and
         *          the endingHashKey.
         */
        // CHECKSTYLE:OFF NPathComplexity
        public static List<UserRecord> Deaggregate(List<Record> records, long startingHashKey, long endingHashKey)
        {
            List<UserRecord> result = new List<UserRecord>();
            byte[] magic = new byte[AGGREGATED_RECORD_MAGIC.Length];
            byte[] digest = new byte[DIGEST_SIZE];

            foreach (Record r in records)
            {
                bool isAggregated = true;
                //long subSeqNum = 0;
                MemoryStream bb = r.Data;

                if ((bb.Length - bb.Position) >= magic.Length)
                {
                    bb.Read(magic, 0, magic.Length);
                }
                else
                {
                    isAggregated = false;
                }

                if (!Array.Equals(AGGREGATED_RECORD_MAGIC, magic) || (bb.Length - bb.Position) <= DIGEST_SIZE)
                {
                    isAggregated = false;
                }

                // monne: Assumption: records aren't aggregated in our case. Otherwise this needs to be uncommented.
                //if (isAggregated)
                //{
                //    int oldLimit = bb.limit();
                //    bb.limit(oldLimit - DIGEST_SIZE);
                //    byte[] messageData = new byte[bb.remaining()];
                //    bb.get(messageData);
                //    bb.limit(oldLimit);
                //    bb.get(digest);
                //    byte[] calculatedDigest = md5(messageData);

                //    if (!Array.Equals(digest, calculatedDigest))
                //    {
                //        isAggregated = false;
                //    }
                //    else
                //    {
                //        try
                //        {
                //            Messages.AggregatedRecord ar = Messages.AggregatedRecord.parseFrom(messageData);
                //            List<String> pks = ar.getPartitionKeyTableList();
                //            List<String> ehks = ar.getExplicitHashKeyTableList();
                //            long aat = r.ApproximateArrivalTimestamp == null ? -1 : r.ApproximateArrivalTimestamp.Time;
                //            try
                //            {
                //                int recordsInCurrRecord = 0;
                //                foreach (Messages.Record mr in ar.getRecordsList())
                //                {
                //                    String explicitHashKey = null;
                //                    String partitionKey = pks[(int)mr.getPartitionKeyIndex()];
                //                    if (mr.hasExplicitHashKeyIndex())
                //                    {
                //                        explicitHashKey = ehks[(int)mr.getExplicitHashKeyIndex()];
                //                    }

                //                    long effectiveHashKey = explicitHashKey != null ? long.Parse(explicitHashKey) : new BigInteger(1, md5(partitionKey.getBytes("UTF-8")));

                //                    if (effectiveHashKey.CompareTo(startingHashKey) < 0 || effectiveHashKey.CompareTo(endingHashKey) > 0)
                //                    {
                //                        for (int toRemove = 0; toRemove < recordsInCurrRecord; ++toRemove)
                //                        {
                //                            result.RemoveAt(result.Count - 1);
                //                        }
                //                        break;
                //                    }

                //                    ++recordsInCurrRecord;
                //                    Record record = new Record()
                //                    {
                //                        Data = (ByteBuffer.wrap(mr.getData().toByteArray())),
                //                        PartitionKey = (partitionKey),
                //                        SequenceNumber = (r.SequenceNumber),
                //                        ApproximateArrivalTimestamp = (aat < 0 ? null : new DateTime(aat))
                //                    };
                //                    result.Add(new UserRecord(true, record, subSeqNum++, explicitHashKey));
                //                }
                //            }
                //            catch (Exception e)
                //            {
                //                StringBuilder sb = new StringBuilder();
                //                sb.Append("Unexpected exception during deaggregation, record was:\n");
                //                sb.Append("PKS:\n");
                //                foreach (String s in pks)
                //                {
                //                    sb.Append(s).Append("\n");
                //                }
                //                sb.Append("EHKS: \n");
                //                foreach (String s in ehks)
                //                {
                //                    sb.Append(s).Append("\n");
                //                }
                //                foreach (Messages.Record mr in ar.getRecordsList())
                //                {
                //                    sb.Append("Record: [hasEhk=").Append(mr.hasExplicitHashKeyIndex()).Append(", ")
                //                      .Append("ehkIdx=").Append(mr.getExplicitHashKeyIndex()).Append(", ")
                //                      .Append("pkIdx=").Append(mr.getPartitionKeyIndex()).Append(", ")
                //                      .Append("dataLen=").Append(mr.getData().toByteArray().length).Append("]\n");
                //                }
                //                sb.Append("Sequence number: ").Append(r.SequenceNumber).Append("\n")
                //                  .Append("Raw data: ")
                //                  .Append(javax.xml.bind.DatatypeConverter.printBase64Binary(messageData)).Append("\n");
                //                Trace.TraceError(sb.ToString(), e);
                //            }
                //        }
                //        catch (InvalidProtocolBufferException e)
                //        {
                //            isAggregated = false;
                //        }
                //    }
                //}

                if (!isAggregated)
                {
                    //bb.rewind();
                    bb.Position = 0;
                    result.Add(new UserRecord(r));
                }
            }
            return result;
        }
        // CHECKSTYLE:ON NPathComplexity
    }
}