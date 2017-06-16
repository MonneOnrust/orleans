using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Worker
{
    /**
 * Class that houses the entities needed to specify the position in the stream from where a new application should
 * start.
 */
    public class InitialPositionInStreamExtended
    {
        private readonly InitialPositionInStream position;
        public DateTime Timestamp { get; private set; }

        /**
         * This is scoped as private to forbid callers from using it directly and to convey the intent to use the
         * static methods instead.
         *
         * @param position  One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. The Amazon Kinesis Client Library will start
         *                  fetching records from this position when the application starts up if there are no checkpoints.
         *                  If there are checkpoints, we will process records from the checkpoint position.
         * @param timestamp The timestamp to use with the AT_TIMESTAMP value for initialPositionInStream.
         */
        private InitialPositionInStreamExtended(InitialPositionInStream position, DateTime timestamp)
        {
            this.position = position;
            this.Timestamp = timestamp;
        }

        /**
         * Get the initial position in the stream where the application should start from.
         *
         * @return The initial position in stream.
         */
        public InitialPositionInStream GetInitialPositionInStream()
        {
            return this.position;
        }

        /**
         * Get the timestamp from where we need to start the application.
         * Valid only for initial position of type AT_TIMESTAMP, returns null for other positions.
         *
         * @return The timestamp from where we need to start the application.
         */
        protected DateTime GetTimestamp()
        {
            return this.Timestamp;
        }

        protected static InitialPositionInStreamExtended NewInitialPosition(InitialPositionInStream position)
        {
            switch (position)
            {
                case InitialPositionInStream.LATEST:
                    return new InitialPositionInStreamExtended(InitialPositionInStream.LATEST, DateTime.MinValue);
                case InitialPositionInStream.TRIM_HORIZON:
                    return new InitialPositionInStreamExtended(InitialPositionInStream.TRIM_HORIZON, DateTime.MinValue);
                default:
                    throw new ArgumentException("Invalid InitialPosition: " + position);
            }
        }

        protected static InitialPositionInStreamExtended NewInitialPositionAtTimestamp(DateTime timestamp)
        {
            if (timestamp == null)
            {
                throw new ArgumentException("Timestamp must be specified for InitialPosition AT_TIMESTAMP");
            }
            return new InitialPositionInStreamExtended(InitialPositionInStream.AT_TIMESTAMP, timestamp);
        }
    }
}