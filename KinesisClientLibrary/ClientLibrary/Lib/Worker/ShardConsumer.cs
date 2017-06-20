using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using KinesisClientLibrary.ClientLibrary.Exceptions;
using KinesisClientLibrary.ClientLibrary.Interfaces;
using KinesisClientLibrary.ClientLibrary.Leases.Impl;
using KinesisClientLibrary.ClientLibrary.Leases.Interfaces;
using KinesisClientLibrary.ClientLibrary.Leases.Utils;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
 * Responsible for consuming data records of a (specified) shard.
 * The instance should be shutdown when we lose the primary responsibility for a shard.
 * A new instance should be created if the primary responsibility is reassigned back to this process.
 */
    public class ShardConsumer
    {
        //private static final Log LOG = LogFactory.getLog(ShardConsumer.class);

        public StreamConfig StreamConfig { get; private set; }
        public IRecordProcessor RecordProcessor { get; private set; }
        public RecordProcessorCheckpointer RecordProcessorCheckpointer { get; private set; }
        //private readonly ExecutorService executorService;
        public ShardInfo ShardInfo { get; private set; }
        public KinesisDataFetcher DataFetcher { get; private set; }
        //private readonly IMetricsFactory metricsFactory;
        public ILeaseManager<KinesisClientLease> LeaseManager { get; private set; }
        public ICheckpoint Checkpoint { get; private set; }
        // Backoff time when polling to check if application has finished processing parent shards
        public long ParentShardPollIntervalMillis { get; private set; }
        public bool CleanupLeasesOfCompletedShards { get; private set; }
        public long TaskBackoffTimeMillis { get; private set; }
        public bool SkipShardSyncAtWorkerInitializationIfLeasesExist { get; private set; }

        private ITask currentTask;
        private long currentTaskSubmitTime;
        private Task<TaskResult> task;

        /*
         * Tracks current state. It is only updated via the consumeStream/shutdown APIs. Therefore we don't do
         * much coordination/synchronization to handle concurrent reads/updates.
         */
        private ConsumerStates.ConsumerState currentState = ConsumerStates.INITIAL_STATE;
        /*
         * Used to track if we lost the primary responsibility. Once set to true, we will start shutting down.
         * If we regain primary responsibility before shutdown is complete, Worker should create a new ShardConsumer object.
         */
        public ShutdownReason ShutdownReason { get { return shutdownReason; } }
        private volatile ShutdownReason shutdownReason;

        public ShutdownNotification ShutdownNotification { get { return shutdownNotification; } }
        private volatile ShutdownNotification shutdownNotification;

        /**
         * @param shardInfo Shard information
         * @param streamConfig Stream configuration to use
         * @param checkpoint Checkpoint tracker
         * @param recordProcessor Record processor used to process the data records for the shard
         * @param leaseManager Used to create leases for new shards
         * @param parentShardPollIntervalMillis Wait for this long if parent shards are not done (or we get an exception)
         * @param executorService ExecutorService used to execute process tasks for this shard
         * @param metricsFactory IMetricsFactory used to construct IMetricsScopes for this shard
         * @param backoffTimeMillis backoff interval when we encounter exceptions
         */
        // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
        ShardConsumer(ShardInfo shardInfo,
                StreamConfig streamConfig,
                ICheckpoint checkpoint,
                IRecordProcessor recordProcessor,
                ILeaseManager<KinesisClientLease> leaseManager,
                long parentShardPollIntervalMillis,
                bool cleanupLeasesOfCompletedShards,
                //ExecutorService executorService,
                //IMetricsFactory metricsFactory,
                long backoffTimeMillis,
                bool skipShardSyncAtWorkerInitializationIfLeasesExist)
        {
            this.StreamConfig = streamConfig;
            this.RecordProcessor = recordProcessor;
            //this.executorService = executorService;
            this.ShardInfo = shardInfo;
            this.Checkpoint = checkpoint;
            this.RecordProcessorCheckpointer = new RecordProcessorCheckpointer(shardInfo, checkpoint,
                                new SequenceNumberValidator(streamConfig.StreamProxy, shardInfo.getShardId(), streamConfig.ValidateSequenceNumberBeforeCheckpointing));
            this.DataFetcher = new KinesisDataFetcher(streamConfig.StreamProxy, shardInfo);
            this.LeaseManager = leaseManager;
            //this.metricsFactory = metricsFactory;
            this.ParentShardPollIntervalMillis = parentShardPollIntervalMillis;
            this.CleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
            this.TaskBackoffTimeMillis = backoffTimeMillis;
            this.SkipShardSyncAtWorkerInitializationIfLeasesExist = skipShardSyncAtWorkerInitializationIfLeasesExist;
        }

        /**
         * No-op if current task is pending, otherwise submits next task for this shard.
         * This method should NOT be called if the ShardConsumer is already in SHUTDOWN_COMPLETED state.
         * 
         * @return true if a new process task was submitted, false otherwise
         */
        bool ConsumeShard()
        {
            lock (this)
            {
                return CheckAndSubmitNextTask();
            }
        }

        private bool ReadyForNextTask()
        {
            return task == null || task.IsCanceled || task.IsCompleted; // || task.IsFaulted???; 
        }

        private bool CheckAndSubmitNextTask()
        {
            lock (this)
            {
                bool submittedNewTask = false;
                if (ReadyForNextTask())
                {
                    TaskOutcome taskOutcome = TaskOutcome.NOT_COMPLETE;
                    if (task != null && task.IsCompleted)
                    {
                        taskOutcome = DetermineTaskOutcome();
                    }

                    UpdateState(taskOutcome);
                    ITask nextTask = GetNextTask();
                    if (nextTask != null)
                    {
                        currentTask = nextTask;
                        try
                        {
                            task = Task.Factory.StartNew(currentTask.Call); // executorService.submit(currentTask);
                            currentTaskSubmitTime = Time.MilliTime;
                            submittedNewTask = true;
                            Trace.WriteLine("Submitted new " + currentTask.TaskType + " task for shard " + ShardInfo.getShardId());
                        }
                        //catch (RejectedExecutionException e)
                        //{
                        //    Trace.TraceInformation(currentTask.TaskType + " task was not accepted for execution.", e);
                        //}
                        catch (RuntimeException e)
                        {
                            Trace.TraceInformation(currentTask.TaskType + " task encountered exception ", e);
                        }
                    }
                    else
                    {
                        Trace.WriteLine(String.Format("No new task to submit for shard {0}, currentState {1}", ShardInfo.getShardId(), currentState.ToString()));
                    }
                }
                else
                {
                    Trace.WriteLine("Previous " + currentTask.TaskType + " task still pending for shard " + ShardInfo.getShardId() + " since " + (Time.MilliTime - currentTaskSubmitTime) + " ms ago" + ".  Not submitting new task.");
                }

                return submittedNewTask;
            }
        }

        private enum TaskOutcome
        {
            SUCCESSFUL, END_OF_SHARD, NOT_COMPLETE, FAILURE
        }

        private TaskOutcome DetermineTaskOutcome()
        {
            try
            {
                task.Wait();
                TaskResult result = task.Result;

                if (result.GetException() == null)
                {
                    if (result.IsShardEndReached())
                    {
                        return TaskOutcome.END_OF_SHARD;
                    }
                    return TaskOutcome.SUCCESSFUL;
                }
                LogTaskException(result);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e.Message, e);
            }
            finally
            {
                // Setting future to null so we don't misinterpret task completion status in case of exceptions
                task = null;
            }
            return TaskOutcome.FAILURE;
        }

        private void LogTaskException(TaskResult taskResult)
        {
            Exception taskException = taskResult.GetException();
            if (taskException is BlockedOnParentShardException)
            {
                // No need to log the stack trace for this exception (it is very specific).
                Trace.WriteLine("Shard " + ShardInfo.getShardId() + " is blocked on completion of parent shard.");
            }
            else
            {
                Trace.WriteLine("Caught exception running " + currentTask.TaskType + " task: ", taskResult.GetException().ToString());
            }
        }

        /**
         * Requests the shutdown of the this ShardConsumer. This should give the record processor a chance to checkpoint
         * before being shutdown.
         * 
         * @param shutdownNotification used to signal that the record processor has been given the chance to shutdown.
         */
        void NotifyShutdownRequested(ShutdownNotification shutdownNotification)
        {
            this.shutdownNotification = shutdownNotification;
            MarkForShutdown(ShutdownReason.REQUESTED);
        }

        /**
         * Shutdown this ShardConsumer (including invoking the RecordProcessor shutdown API).
         * This is called by Worker when it loses responsibility for a shard.
         * 
         * @return true if shutdown is complete (false if shutdown is still in progress)
         */
        bool BeginShutdown()
        {
            lock (this)
            {
                MarkForShutdown(ShutdownReason.ZOMBIE);
                CheckAndSubmitNextTask();

                return IsShutdown();
            }
        }

        void MarkForShutdown(ShutdownReason reason)
        {
            lock (this)
            {
                // ShutdownReason.ZOMBIE takes precedence over TERMINATE (we won't be able to save checkpoint at end of shard)
                if (shutdownReason == null || shutdownReason.CanTransitionTo(reason))
                {
                    shutdownReason = reason;
                }
            }
        }

        /**
         * Used (by Worker) to check if this ShardConsumer instance has been shutdown
         * RecordProcessor shutdown() has been invoked, as appropriate.
         * 
         * @return true if shutdown is complete
         */
        bool IsShutdown()
        {
            return currentState.IsTerminal();
        }

        /**
         * Figure out next task to run based on current state, task, and shutdown context.
         * 
         * @return Return next task to run
         */
        private ITask GetNextTask()
        {
            ITask nextTask = currentState.CreateTask(this);

            if (nextTask == null)
            {
                return null;
            }
            else
            {
                return nextTask; //new MetricsCollectingTaskDecorator(nextTask, metricsFactory);
            }
        }

        /**
         * Note: This is a private/internal method with package level access solely for testing purposes.
         * Update state based on information about: task success, current state, and shutdown info.
         * 
         * @param taskOutcome The outcome of the last task
         */
        void UpdateState(TaskOutcome taskOutcome)
        {
            if (taskOutcome == TaskOutcome.END_OF_SHARD)
            {
                MarkForShutdown(ShutdownReason.TERMINATE);
            }
            if (IsShutdownRequested())
            {
                currentState = currentState.ShutdownTransition(shutdownReason);
            }
            else if (taskOutcome == TaskOutcome.SUCCESSFUL)
            {
                if (currentState.GetTaskType() == currentTask.TaskType)
                {
                    currentState = currentState.SuccessTransition();
                }
                else
                {
                    Trace.TraceError("Current State task type of '" + currentState.GetTaskType() + "' doesn't match the current tasks type of '" + currentTask.TaskType
                            + "'.  This shouldn't happen, and indicates a programming error. Unable to safely transition to the next state.");
                }
            }
            //
            // Don't change state otherwise
            //
        }

        //@VisibleForTesting
        bool IsShutdownRequested()
        {
            return shutdownReason != null;
        }

        /**
         * Private/Internal method - has package level access solely for testing purposes.
         * 
         * @return the currentState
         */
        ConsumerStates.ShardConsumerState GetCurrentState()
        {
            return currentState.GetState();
        }

        //ExecutorService GetExecutorService()
        //{
        //    return executorService;
        //}
    }
}