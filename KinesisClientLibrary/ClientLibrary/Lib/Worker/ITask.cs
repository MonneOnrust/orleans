using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
 * Interface for shard processing tasks.
 * A task may execute an application callback (e.g. initialize, process, shutdown).
 */
    public interface ITask 
    {
        ///**
        // * Perform task logic.
        // * E.g. perform set up (e.g. fetch records) and invoke a callback (e.g. processRecords() API).
        // * 
        // * @return TaskResult (captures any exceptions encountered during execution of the task)
        // */
        TaskResult Call();

        /**
         * @return TaskType
         */
        TaskType TaskType { get; }
    }
}
