using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
  * Used to specify the position in the stream where a new application should start from.
  * This is used during initial application bootstrap (when a checkpoint doesn't exist for a shard or its parents).
  */
    public enum InitialPositionInStream
    {
        /**
         * Start after the most recent data record (fetch new data).
         */
        LATEST,

        /**
         * Start from the oldest available data record.
         */
        TRIM_HORIZON,

        /**
         * Start from the record at or after the specified server-side timestamp.
         */
        AT_TIMESTAMP
    }
}