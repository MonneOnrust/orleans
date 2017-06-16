using System;
using System.Collections.Generic;
using System.Text;
using TestKinesisStreamProvider.ClientLibrary.Interfaces;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Worker
{
    /**
 * Notifies record processor of incoming shutdown request, and gives them a chance to checkpoint.
 */
    public class ShutdownNotificationTask : ITask
    {
        private readonly IRecordProcessor recordProcessor;
        private readonly IRecordProcessorCheckpointer recordProcessorCheckpointer;
        private readonly ShutdownNotification shutdownNotification;
        private readonly ShardInfo shardInfo;

        public ShutdownNotificationTask(IRecordProcessor recordProcessor, IRecordProcessorCheckpointer recordProcessorCheckpointer, ShutdownNotification shutdownNotification, ShardInfo shardInfo)
        {
            this.recordProcessor = recordProcessor;
            this.recordProcessorCheckpointer = recordProcessorCheckpointer;
            this.shutdownNotification = shutdownNotification;
            this.shardInfo = shardInfo;
        }

        public TaskResult Call()
        {
            try
            {
                if (recordProcessor is IShutdownNotificationAware)
                {
                    IShutdownNotificationAware shutdownNotificationAware = (IShutdownNotificationAware)recordProcessor;
                    try
                    {
                        shutdownNotificationAware.ShutdownRequested(recordProcessorCheckpointer);
                    }
                    catch (Exception ex)
                    {
                        return new TaskResult(ex);
                    }
                }
                return new TaskResult(null);
            }
            finally
            {
                shutdownNotification.ShutdownNotificationComplete();
            }
        }

        public TaskType TaskType
        {
            get { return TaskType.SHUTDOWN_NOTIFICATION; }
        }
    }
}
