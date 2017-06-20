using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Types
{
    /**
 * Container for the parameters to the IRecordProcessor's
 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#initialize(InitializationInput
 * initializationInput) initialize} method.
 */
    public class InitializationInput
    {

        private String shardId;
        private ExtendedSequenceNumber extendedSequenceNumber;

        /**
         * Default constructor.
         */
        public InitializationInput()
        {
        }

        /**
         * Get shard Id.
         *
         * @return The record processor will be responsible for processing records of this shard.
         */
        public String getShardId()
        {
            return shardId;
        }

        /**
         * Set shard Id.
         *
         * @param shardId The record processor will be responsible for processing records of this shard.
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public InitializationInput withShardId(String shardId)
        {
            this.shardId = shardId;
            return this;
        }

        /**
         * Get starting {@link ExtendedSequenceNumber}.
         *
         * @return The {@link ExtendedSequenceNumber} in the shard from which records will be delivered to this
         *         record processor.
         */
        public ExtendedSequenceNumber getExtendedSequenceNumber()
        {
            return extendedSequenceNumber;
        }

        /**
         * Set starting {@link ExtendedSequenceNumber}.
         *
         * @param extendedSequenceNumber The {@link ExtendedSequenceNumber} in the shard from which records will be
         *        delivered to this record processor.
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public InitializationInput withExtendedSequenceNumber(ExtendedSequenceNumber extendedSequenceNumber)
        {
            this.extendedSequenceNumber = extendedSequenceNumber;
            return this;
        }
    }

}
