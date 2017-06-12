using System;
using System.Collections.Generic;
using System.Text;
using TestKinesisStreamProvider.ClientLibrary.Lib.Checkpoint;

namespace TestKinesisStreamProvider.ClientLibrary.Types
{
    /**
 * Represents a two-part sequence number for records aggregated by the Kinesis
 * Producer Library.
 * 
 * <p>
 * The KPL combines multiple user records into a single Kinesis record. Each
 * user record therefore has an integer sub-sequence number, in addition to the
 * regular sequence number of the Kinesis record. The sub-sequence number is
 * used to checkpoint within an aggregated record.
 * 
 * @author daphnliu
 *
 */
    public class ExtendedSequenceNumber : IComparable<ExtendedSequenceNumber>
    {
        public String SequenceNumber { get; private set; }
        public long SubSequenceNumber { get; private set; }

        // Define TRIM_HORIZON, LATEST, and AT_TIMESTAMP to be less than all sequence numbers
        private static readonly long TRIM_HORIZON_BIG_INTEGER_VALUE = -2L;
        private static readonly long LATEST_BIG_INTEGER_VALUE = -1L;
        private static readonly long AT_TIMESTAMP_BIG_INTEGER_VALUE = -3L;

        /**
         * Special value for LATEST.
         */
        public static readonly ExtendedSequenceNumber LATEST = new ExtendedSequenceNumber(SentinelCheckpoint.LATEST.ToString());

        /**
         * Special value for SHARD_END.
         */
        public static readonly ExtendedSequenceNumber SHARD_END = new ExtendedSequenceNumber(SentinelCheckpoint.SHARD_END.ToString());
        /**
         * 
         * Special value for TRIM_HORIZON.
         */
        public static readonly ExtendedSequenceNumber TRIM_HORIZON = new ExtendedSequenceNumber(SentinelCheckpoint.TRIM_HORIZON.ToString());

        /**
         * Special value for AT_TIMESTAMP.
         */
        public static readonly ExtendedSequenceNumber AT_TIMESTAMP = new ExtendedSequenceNumber(SentinelCheckpoint.AT_TIMESTAMP.ToString());

        /**
         * Construct an ExtendedSequenceNumber. The sub-sequence number defaults to
         * 0.
         * 
         * @param sequenceNumber
         *            Sequence number of the Kinesis record
         */
        public ExtendedSequenceNumber(String sequenceNumber) : this(sequenceNumber, 0L)
        {
        }

        /**
         * Construct an ExtendedSequenceNumber.
         * 
         * @param sequenceNumber
         *            Sequence number of the Kinesis record
         * @param subSequenceNumber
         *            Sub-sequence number of the user record within the Kinesis
         *            record
         */
        public ExtendedSequenceNumber(String sequenceNumber, long subSequenceNumber)
        {
            this.SequenceNumber = sequenceNumber;
            this.SubSequenceNumber = /*subSequenceNumber == null ? 0 : */ subSequenceNumber/*.longValue()*/;
        }

        /**
         * Compares this with another ExtendedSequenceNumber using these rules.
         * 
         * SHARD_END is considered greatest
         * TRIM_HORIZON, LATEST and AT_TIMESTAMP are considered less than sequence numbers
         * sequence numbers are given their big integer value
         * 
         * @param extendedSequenceNumber The ExtendedSequenceNumber to compare against
         * @return returns negative/0/positive if this is less than/equal to/greater than extendedSequenceNumber
         */


        public int CompareTo(ExtendedSequenceNumber extendedSequenceNumber)
        {
            String secondSequenceNumber = extendedSequenceNumber.SequenceNumber;

            if (!IsDigitsOrSentinelValue(SequenceNumber) || !IsDigitsOrSentinelValue(secondSequenceNumber))
            {
                throw new ArgumentException("Expected a sequence number or a sentinel checkpoint value but received: first=" + SequenceNumber + " and second=" + secondSequenceNumber);
            }

            // SHARD_END is the greatest
            if (SentinelCheckpoint.SHARD_END.ToString().Equals(SequenceNumber) && SentinelCheckpoint.SHARD_END.ToString().Equals(secondSequenceNumber))
            {
                return 0;
            }
            else if (SentinelCheckpoint.SHARD_END.ToString().Equals(secondSequenceNumber))
            {
                return -1;
            }
            else if (SentinelCheckpoint.SHARD_END.ToString().Equals(SequenceNumber))
            {
                return 1;
            }

            // Compare other sentinel values and serial numbers after converting them to a big integer value
            int result = BigIntegerValue(SequenceNumber).CompareTo(BigIntegerValue(secondSequenceNumber));
            return result == 0 ? SubSequenceNumber.CompareTo(extendedSequenceNumber.SubSequenceNumber) : result;
        }

        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{");
            if (SequenceNumber != null)
            {
                sb.Append("SequenceNumber: " + SequenceNumber + ",");
            }
            if (SubSequenceNumber >= 0)
            {
                sb.Append("SubsequenceNumber: " + SubSequenceNumber);
            }
            sb.Append("}");
            return sb.ToString();
        }

        public override int GetHashCode()
        {
            const int prime = 31;
            const int shift = 32;
            int hashCode = 1;
            hashCode = prime * hashCode + ((SequenceNumber == null) ? 0 : SequenceNumber.GetHashCode());
            hashCode = prime * hashCode + ((SubSequenceNumber < 0) ? 0 : (int)(SubSequenceNumber ^ (SubSequenceNumber >>/*>*/ shift)));
            return hashCode;
        }


        public override bool Equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }

            if (!(obj is ExtendedSequenceNumber))
            {
                return false;
            }
            ExtendedSequenceNumber other = (ExtendedSequenceNumber)obj;

            if (!SequenceNumber.Equals(other.SequenceNumber))
            {
                return false;
            }
            return SubSequenceNumber == other.SubSequenceNumber;
        }

        /**
         * Sequence numbers are converted, sentinels are given a value of -1. Note this method is only used after special
         * logic associated with SHARD_END and the case of comparing two sentinel values has already passed, so we map
         * sentinel values LATEST, TRIM_HORIZON and AT_TIMESTAMP to negative numbers so that they are considered less than
         * sequence numbers.
         * 
         * @param sequenceNumber The string to convert to big integer value
         * @return a BigInteger value representation of the sequenceNumber
         */
        private static long BigIntegerValue(String sequenceNumber)
        {
            if (IsDigits(sequenceNumber))
            {
                return long.Parse(sequenceNumber);
            }
            else if (SentinelCheckpoint.LATEST.ToString().Equals(sequenceNumber))
            {
                return LATEST_BIG_INTEGER_VALUE;
            }
            else if (SentinelCheckpoint.TRIM_HORIZON.ToString().Equals(sequenceNumber))
            {
                return TRIM_HORIZON_BIG_INTEGER_VALUE;
            }
            else if (SentinelCheckpoint.AT_TIMESTAMP.ToString().Equals(sequenceNumber))
            {
                return AT_TIMESTAMP_BIG_INTEGER_VALUE;
            }
            else
            {
                throw new ArgumentException("Expected a string of digits, TRIM_HORIZON, LATEST or AT_TIMESTAMP but received " + sequenceNumber);
            }
        }

        /**
         * Checks if the string is all digits or one of the SentinelCheckpoint values.
         * 
         * @param string
         * @return true if and only if the string is all digits or one of the SentinelCheckpoint values
         */
        private static bool IsDigitsOrSentinelValue(String @string)
        {
            return IsDigits(@string) || IsSentinelValue(@string);
        }

        /**
         * Checks if the string is a SentinelCheckpoint value.
         * 
         * @param string
         * @return true if and only if the string can be converted to a SentinelCheckpoint
         */
        private static bool IsSentinelValue(String @string)
        {
            return Enum.TryParse(@string, out SentinelCheckpoint @value);
        }

        /**
         * Checks if the string is composed of only digits.
         * 
         * @param string
         * @return true for a string of all digits, false otherwise (including false for null and empty string)
         */
        private static bool IsDigits(String @string)
        {
            if (@string == null || @string.Length == 0)
            {
                return false;
            }
            for (int i = 0; i < @string.Length; ++i)
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