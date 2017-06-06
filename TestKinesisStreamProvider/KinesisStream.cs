using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace TestKinesisStreamProvider
{
    /// <summary>
    /// How to use the Queue Storage Service: http://www.windowsazure.com/en-us/develop/net/how-to-guides/queue-service/
    /// Windows Azure Storage Abstractions and their Scalability Targets: http://blogs.msdn.com/b/windowsazurestorage/archive/2010/05/10/windows-azure-storage-abstractions-and-their-scalability-targets.aspx
    /// Naming Queues and Metadata: http://msdn.microsoft.com/en-us/library/windowsazure/dd179349.aspx
    /// Windows Azure Queues and Windows Azure Service Bus Queues - Compared and Contrasted: http://msdn.microsoft.com/en-us/library/hh767287(VS.103).aspx
    /// Status and Error Codes: http://msdn.microsoft.com/en-us/library/dd179382.aspx
    ///
    /// http://blogs.msdn.com/b/windowsazurestorage/archive/tags/scalability/
    /// http://blogs.msdn.com/b/windowsazurestorage/archive/2010/12/30/windows-azure-storage-architecture-overview.aspx
    /// http://blogs.msdn.com/b/windowsazurestorage/archive/2010/11/06/how-to-get-most-out-of-windows-azure-tables.aspx
    /// 
    /// </summary>
    internal static class KinesisQueueDefaultPolicies
    {
        public static int MaxQueueOperationRetries;
        public static TimeSpan PauseBetweenQueueOperationRetries;
        public static TimeSpan QueueOperationTimeout;
        public static IRetryPolicy QueueOperationRetryPolicy;
        
        static KinesisQueueDefaultPolicies()
        {
            MaxQueueOperationRetries = 5;
            PauseBetweenQueueOperationRetries = TimeSpan.FromMilliseconds(100);
            QueueOperationRetryPolicy = new LinearRetry(PauseBetweenQueueOperationRetries, MaxQueueOperationRetries); // 5 x 100ms
            QueueOperationTimeout = TimeSpan.FromMilliseconds((PauseBetweenQueueOperationRetries.TotalMilliseconds * MaxQueueOperationRetries) * 6);    // 3 sec
        }
    }

    /// <summary>
    /// Utility class to encapsulate access to Azure queue storage.
    /// </summary>
    /// <remarks>
    /// Used by Kinesis streaming provider.
    /// </remarks>
    public class KinesisStreamShard
    {
        /// <summary> Name of the table queue instance is managing. </summary>
        public string KinesisStreamName { get; private set; }

        private string connectionString { get; set; }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        private readonly Logger logger;
        private readonly TimeSpan? messageVisibilityTimeout;
        private readonly KinesisClient kinesisClient;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="streamName">Name of the stream to be connected to.</param>
        /// <param name="kinesisConnectionString">Connection string for the Azure storage account used to host this table.</param>
        /// <param name="visibilityTimeout">A TimeSpan specifying the visibility timeout interval</param>
        public KinesisStreamShard(string streamName, string kinesisConnectionString, Logger logger, TimeSpan? visibilityTimeout = null)
        {
            KinesisUtils.ValidateStreamName(streamName);

            this.logger = logger; //LogManager.GetLogger(this.GetType().Name, LoggerType.Runtime);
            KinesisStreamName = streamName;
            connectionString = kinesisConnectionString;
            messageVisibilityTimeout = visibilityTimeout;

            kinesisClient = KinesisUtils.GetKinesisClient(
                streamName,
                connectionString,
                KinesisQueueDefaultPolicies.QueueOperationRetryPolicy,
                KinesisQueueDefaultPolicies.QueueOperationTimeout,
                logger);
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="streamName">Name of the stream to be connected to.</param>
        /// <param name="deploymentId">The deployment id of the Azure service hosting this silo. It will be concatenated to the queueName.</param>
        /// <param name="storageConnectionString">Connection string for the Azure storage account used to host this table.</param>
        /// <param name="visibilityTimeout">A TimeSpan specifying the visibility timeout interval</param>
        public KinesisStreamShard(string streamName, string deploymentId, string storageConnectionString, Logger logger, TimeSpan? visibilityTimeout = null)
        {
            KinesisUtils.ValidateStreamName(streamName);

            this.logger = logger; //LogManager.GetLogger(this.GetType().Name, LoggerType.Runtime);
            KinesisStreamName = deploymentId + "-" + streamName;
            KinesisUtils.ValidateStreamName(KinesisStreamName);
            connectionString = storageConnectionString;
            messageVisibilityTimeout = visibilityTimeout;

            kinesisClient = KinesisUtils.GetKinesisClient(
                streamName,
                connectionString,
                KinesisQueueDefaultPolicies.QueueOperationRetryPolicy,
                KinesisQueueDefaultPolicies.QueueOperationTimeout,
                logger);
        }

        /// <summary>
        /// Initializes the connection to the queue.
        /// </summary>
        public async Task InitShardAsync()
        {
            var startTime = DateTime.UtcNow;

            try
            {
                // Create the queue if it doesn't already exist.
                await kinesisClient.InitStreamAsync();
                //logger.Info(/*ErrorCode.AzureQueue_01,*/ "{0} Azure storage queue {1}", (didCreate ? "Created" : "Attached to"), KinesisStreamName);
                logger.Info($"{KinesisStreamName} Kinesis stream initialized");
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "InitStreamAsync", ErrorCode.AzureQueue_02);
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "InitStreamAsync");
            }
        }

        /// <summary>
        /// Deletes the queue.
        /// </summary>
        public async Task DeleteQueue()
        {
            // TODO: check when this goes off

            var startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Deleting queue: {0}", KinesisStreamName);
            //try
            //{
            //    // that way we don't have first to create the queue to be able later to delete it.
            //    CloudQueue queueRef = queue ?? kinesisClient.GetQueueReference(KinesisStreamName);
            //    if (await queueRef.DeleteIfExistsAsync())
            //    {
            //        logger.Info(/*ErrorCode.AzureQueue_03,*/ "Deleted Azure Queue {0}", KinesisStreamName);
            //    }
            //}
            //catch (Exception exc)
            //{
            //    ReportErrorAndRethrow(exc, "DeleteQueue", ErrorCode.AzureQueue_04);
            //}
            //finally
            //{
            //    CheckAlertSlowAccess(startTime, "DeleteQueue");
            //}
        }

        /// <summary>
        /// Clears the queue.
        /// </summary>
        public async Task ClearQueue()
        {
            // TODO: check when this goes off

            var startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Clearing a queue: {0}", KinesisStreamName);
            
            //try
            //{
            //    // that way we don't have first to create the queue to be able later to delete it.
            //    CloudQueue queueRef = queue ?? kinesisClient.GetQueueReference(KinesisStreamName);
            //    await queueRef.ClearAsync();
            //    logger.Info(/*ErrorCode.AzureQueue_05,*/ "Cleared Azure Queue {0}", KinesisStreamName);
            //}
            //catch (Exception exc)
            //{
            //    ReportErrorAndRethrow(exc, "ClearQueue", ErrorCode.AzureQueue_06);
            //}
            //finally
            //{
            //    CheckAlertSlowAccess(startTime, "ClearQueue");
            //}
        }

        /// <summary>
        /// Adds a new message to the queue.
        /// </summary>
        /// <param name="message">Message to be added to the queue.</param>
        public async Task AddMessageAsync(KinesisStreamMessage message)
        {
            var startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Adding message {0} to stream: {1}", message, KinesisStreamName);
            try
            {
                await kinesisClient.PutRecordAsync(message);
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "AddStreamMessageAsync", ErrorCode.AzureQueue_07);
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "AddStreamMessageAsync");
            }
        }

        /// <summary>
        /// Peeks in the queue for latest message, without dequeueing it.
        /// </summary>
        public async Task<KinesisStreamMessage> PeekQueueMessage()
        {
            // TODO: check when this goes off

            var startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Peeking a message from queue: {0}", KinesisStreamName);

            return null;

            //try
            //{
            //    return await queue.PeekMessageAsync();

            //}
            //catch (Exception exc)
            //{
            //    ReportErrorAndRethrow(exc, "PeekQueueMessage", ErrorCode.AzureQueue_08);
            //    return null; // Dummy statement to keep compiler happy
            //}
            //finally
            //{
            //    CheckAlertSlowAccess(startTime, "PeekQueueMessage");
            //}
        }


        /// <summary>
        /// Gets a new message from the queue.
        /// </summary>
        public async Task<KinesisStreamMessage> GetQueueMessage()
        {
            var startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Getting a message from queue: {0}", KinesisStreamName);

            return null;
            //try
            //{
            //    //BeginGetMessage and EndGetMessage is not supported in netstandard, may be use GetMessageAsync
            //    // http://msdn.microsoft.com/en-us/library/ee758456.aspx
            //    // If no messages are visible in the queue, GetMessage returns null.
            //    return await queue.GetRecordAsync(messageVisibilityTimeout);
            //}
            //catch (Exception exc)
            //{
            //    ReportErrorAndRethrow(exc, "GetQueueMessage", ErrorCode.AzureQueue_09);
            //    return null; // Dummy statement to keep compiler happy
            //}
            //finally
            //{
            //    CheckAlertSlowAccess(startTime, "GetQueueMessage");
            //}
        }

        /// <summary>
        /// Gets a number of new messages from the queue.
        /// </summary>
        /// <param name="count">Number of messages to get from the queue.</param>
        public async Task<IEnumerable<KinesisStreamMessage>> GetStreamMessagesAsync(int count = -1)
        {
            var startTime = DateTime.UtcNow;
            if (count == -1)
            {
                count = KinesisStreamMessage.MaxNumberOfMessagesToPeek;
            }
            if (logger.IsVerbose2) logger.Verbose2("Getting up to {0} messages from stream: {1}", count, KinesisStreamName);
            try
            {
                return await kinesisClient.GetRecordsAsync(count, messageVisibilityTimeout);
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "GetStreamMessagesAsync", ErrorCode.AzureQueue_10);
                return null; // Dummy statement to keep compiler happy
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "GetStreamMessagesAsync");
            }
        }

        /// <summary>
        /// Deletes a messages from the queue.
        /// </summary>
        /// <param name="message">A message to be deleted from the queue.</param>
        public async Task DeleteQueueMessage(KinesisStreamMessage message)
        {
            // TODO: check when this goes off
            
            var startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Deleting a message from stream: {0}", KinesisStreamName);
            //try
            //{
            //    await queue.DeleteMessageAsync(message.Id, message.PopReceipt);

            //}
            //catch (Exception exc)
            //{
            //    ReportErrorAndRethrow(exc, "DeleteMessage", ErrorCode.AzureQueue_11);
            //}
            //finally
            //{
            //    CheckAlertSlowAccess(startTime, "DeleteQueueMessage");
            //}
        }

        internal async Task GetAndDeleteQueueMessage()
        {
            KinesisStreamMessage message = await GetQueueMessage();
            await DeleteQueueMessage(message);
        }

        /// <summary>
        /// Returns an approximate number of messages in the queue.
        /// </summary>
        public async Task<int> GetApproximateMessageCount()
        {
            // TODO: check when this goes off
            
            var startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("GetApproximateMessageCount a message from queue: {0}", KinesisStreamName);

            return 10;
            //try
            //{
            //    await queue.FetchAttributesAsync();
            //    return queue.ApproximateMessageCount.HasValue ? queue.ApproximateMessageCount.Value : 0;

            //}
            //catch (Exception exc)
            //{
            //    ReportErrorAndRethrow(exc, "FetchAttributes", ErrorCode.AzureQueue_12);
            //    return 0; // Dummy statement to keep compiler happy
            //}
            //finally
            //{
            //    CheckAlertSlowAccess(startTime, "GetApproximateMessageCount");
            //}
        }

        private void CheckAlertSlowAccess(DateTime startOperation, string operation)
        {
            var timeSpan = DateTime.UtcNow - startOperation;
            if (timeSpan > KinesisQueueDefaultPolicies.QueueOperationTimeout)
            {
                logger.Warn((int)ErrorCode.AzureQueue_13, "Slow access to Kinesis stream {0} for {1}, which took {2}.", KinesisStreamName, operation, timeSpan);
            }
        }

        private void ReportErrorAndRethrow(Exception exc, string operation, ErrorCode errorCode)
        {
            var errMsg = String.Format("Error doing {0} for Kinesis stream {1} " + Environment.NewLine + "Exception = {2}", operation, KinesisStreamName, exc);
            logger.Error((int)errorCode, errMsg, exc);
            throw new AggregateException(errMsg, exc);
        }
    }
}