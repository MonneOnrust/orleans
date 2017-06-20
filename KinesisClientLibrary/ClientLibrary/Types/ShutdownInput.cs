using System;
using System.Collections.Generic;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Interfaces;
using KinesisClientLibrary.ClientLibrary.Lib.Worker;

namespace KinesisClientLibrary.ClientLibrary.Types
{
    /**
 * Container for the parameters to the IRecordProcessor's
 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#shutdown(ShutdownInput
 * shutdownInput) shutdown} method.
 */
    public class ShutdownInput
    {
        /**
         * Get shutdown reason.
         *
         * @return Reason for the shutdown (ShutdownReason.TERMINATE indicates the shard is closed and there are no
         *         more records to process. Shutdown.ZOMBIE indicates a fail over has occurred).
         */
        public ShutdownReason ShutdownReason { get; private set; }
        /**
         * Get Checkpointer.
         *
         * @return The checkpointer object that the record processor should use to checkpoint
         */
        public IRecordProcessorCheckpointer Checkpointer { get; private set; }

        /**
         * Default constructor.
         */
        public ShutdownInput()
        {
        }

        /**
         * Set shutdown reason.
         *
         * @param shutdownReason Reason for the shutdown
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public ShutdownInput WithShutdownReason(ShutdownReason shutdownReason)
        {
            this.ShutdownReason = shutdownReason;
            return this;
        }

        /**
         * Set the checkpointer.
         *
         * @param checkpointer The checkpointer object that the record processor should use to checkpoint
         * @return A reference to this updated object so that method calls can be chained together.
         */
        public ShutdownInput WithCheckpointer(IRecordProcessorCheckpointer checkpointer)
        {
            this.Checkpointer = checkpointer;
            return this;
        }
    }
}