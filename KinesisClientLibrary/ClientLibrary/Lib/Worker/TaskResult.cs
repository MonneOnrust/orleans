using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
 * Used to capture information from a task that we want to communicate back to the higher layer.
 * E.g. exception thrown when executing the task, if we reach end of a shard.
 */
    public class TaskResult
    {

        // Did we reach the end of the shard while processing this task.
        private bool shardEndReached;

        // Any exception caught while executing the task.
        private Exception exception;

        /**
         * @return the shardEndReached
         */
        public bool IsShardEndReached()
        {
            return shardEndReached;
        }

        /**
         * @param shardEndReached the shardEndReached to set
         */
        protected void SetShardEndReached(bool shardEndReached)
        {
            this.shardEndReached = shardEndReached;
        }

        /**
         * @return the exception
         */
        public Exception GetException()
        {
            return exception;
        }

        /**
         * @param e Any exception encountered when running the process task.
         */
        public TaskResult(Exception e) : this(e, false)
        {
        }

        /**
         * @param isShardEndReached Whether we reached the end of the shard (no more records will ever be fetched)
         */
        public TaskResult(bool isShardEndReached) : this(null, isShardEndReached)
        {
        }

        /**
         * @param e Any exception encountered when executing task.
         * @param isShardEndReached Whether we reached the end of the shard (no more records will ever be fetched)
         */
        public TaskResult(Exception e, bool isShardEndReached)
        {
            this.exception = e;
            this.shardEndReached = isShardEndReached;
        }
    }
}