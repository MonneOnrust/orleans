using System;
using System.Collections.Generic;
using System.Text;
//using static TestKinesisStreamProvider.ClientLibrary.Lib.Worker.ConsumerStates;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Worker
{
    /**
 * Reason the RecordProcessor is being shutdown.
 * Used to distinguish between a fail-over vs. a termination (shard is closed and all records have been delivered).
 * In case of a fail over, applications should NOT checkpoint as part of shutdown,
 * since another record processor may have already started processing records for that shard.
 * In case of termination (resharding use case), applications SHOULD checkpoint their progress to indicate
 * that they have successfully processed all the records (processing of child shards can then begin).
 */
    public class ShutdownReason
    {
        private static readonly ShutdownReason zombie = new ShutdownReason(3, ConsumerStates.ShardConsumerState.SHUTTING_DOWN.ConsumerState);
        private static readonly ShutdownReason terminate = new ShutdownReason(2, ConsumerStates.ShardConsumerState.SHUTTING_DOWN.ConsumerState);
        private static readonly ShutdownReason requested = new ShutdownReason(1, ConsumerStates.ShardConsumerState.SHUTDOWN_REQUESTED.ConsumerState);

        /**
         * Processing will be moved to a different record processor (fail over, load balancing use cases).
         * Applications SHOULD NOT checkpoint their progress (as another record processor may have already started processing data).
         */
        public static ShutdownReason ZOMBIE { get { return zombie; } }
        /**
         * Terminate processing for this RecordProcessor (resharding use case).
         * Indicates that the shard is closed and all records from the shard have been delivered to the application.
         * Applications SHOULD checkpoint their progress to indicate that they have successfully processed all records
         * from this shard and processing of child shards can be started.
         */
        public static ShutdownReason TERMINATE { get { return terminate; } }
        /**
         * Indicates that the entire application is being shutdown, and if desired the record processor will be given a
         * final chance to checkpoint. This state will not trigger a direct call to
         * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#shutdown(ShutdownInput)}, but
         * instead depend on a different interface for backward compatibility.
         */
        public static ShutdownReason REQUESTED { get { return requested; } }

        private readonly int rank;
        public ConsumerStates.ConsumerState ShutdownState { get; private set; }

        ShutdownReason(int rank, ConsumerStates.ConsumerState shutdownState)
        {
            this.rank = rank;
            this.ShutdownState = shutdownState;
        }

        /**
         * Indicates whether the given reason can override the current reason.
         * 
         * @param reason the reason to transition to
         * @return true if the transition is allowed, false if it's not.
         */
        public bool CanTransitionTo(ShutdownReason reason)
        {
            if (reason == null)
            {
                return false;
            }
            return reason.rank > this.rank;
        }
    }
}