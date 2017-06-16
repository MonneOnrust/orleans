using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TestKinesisStreamProvider.ClientLibrary.Lib.Worker
{
    /**
 * Top level container for all the possible states a {@link ShardConsumer} can be in. The logic for creation of tasks,
 * and state transitions is contained within the {@link ConsumerState} objects.
 *
 * <h2>State Diagram</h2>
 * 
 * <pre>
 *       +-------------------+
 *       | Waiting on Parent |                               +------------------+
 *  +----+       Shard       |                               |     Shutdown     |
 *  |    |                   |          +--------------------+   Notification   |
 *  |    +----------+--------+          |  Shutdown:         |     Requested    |
 *  |               | Success           |   Requested        +-+-------+--------+
 *  |               |                   |                      |       |
 *  |        +------+-------------+     |                      |       | Shutdown:
 *  |        |    Initializing    +-----+                      |       |  Requested
 *  |        |                    |     |                      |       |
 *  |        |                    +-----+-------+              |       |
 *  |        +---------+----------+     |       | Shutdown:    | +-----+-------------+
 *  |                  | Success        |       |  Terminated  | |     Shutdown      |
 *  |                  |                |       |  Zombie      | |   Notification    +-------------+
 *  |           +------+-------------+  |       |              | |     Complete      |             |
 *  |           |     Processing     +--+       |              | ++-----------+------+             |
 *  |       +---+                    |          |              |  |           |                    |
 *  |       |   |                    +----------+              |  |           | Shutdown:          |
 *  |       |   +------+-------------+          |              \  /           |  Requested         |
 *  |       |          |                        |               \/            +--------------------+
 *  |       |          |                        |               ||
 *  |       | Success  |                        |               || Shutdown:
 *  |       +----------+                        |               ||  Terminated
 *  |                                           |               ||  Zombie
 *  |                                           |               ||
 *  |                                           |               ||
 *  |                                           |           +---++--------------+
 *  |                                           |           |   Shutting Down   |
 *  |                                           +-----------+                   |
 *  |                                                       |                   |
 *  |                                                       +--------+----------+
 *  |                                                                |
 *  |                                                                | Shutdown:
 *  |                                                                |  All Reasons
 *  |                                                                |
 *  |                                                                |
 *  |      Shutdown:                                        +--------+----------+
 *  |        All Reasons                                    |     Shutdown      |
 *  +-------------------------------------------------------+     Complete      |
 *                                                          |                   |
 *                                                          +-------------------+
 * </pre>
 */
    public class ConsumerStates
    {
        /**
         * Enumerates processing states when working on a shard.
         */
        public class ShardConsumerState
        {
            public static ShardConsumerState WAITING_ON_PARENT_SHARDS = new ShardConsumerState(new BlockedOnParentState());
            public static ShardConsumerState INITIALIZING = new ShardConsumerState(new InitializingState());
            public static ShardConsumerState PROCESSING = new ShardConsumerState(new ProcessingState());
            public static ShardConsumerState SHUTDOWN_REQUESTED = new ShardConsumerState(new ShutdownNotificationState());
            public static ShardConsumerState SHUTTING_DOWN = new ShardConsumerState(new ShuttingDownState());
            public static ShardConsumerState SHUTDOWN_COMPLETE = new ShardConsumerState(new ShutdownCompleteState());

            public ConsumerState ConsumerState { get; private set; }

            ShardConsumerState(ConsumerState consumerState)
            {
                this.ConsumerState = consumerState;
            }
        }

        /**
         * Represents a the current state of the consumer. This handles the creation of tasks for the consumer, and what to do when a transition occurs.
         */
        public interface ConsumerState
        {
            /**
             * Creates a new task for this state using the passed in consumer to build the task. If there is no task
             * required for this state it may return a null value. {@link ConsumerState}'s are allowed to modify the
             * consumer during the execution of this method.
             * 
             * @param consumer
             *            the consumer to use build the task, or execute state.
             * @return a valid task for this state or null if there is no task required.
             */
            /*I*/
            ITask CreateTask(ShardConsumer consumer);

            /**
             * Provides the next state of the consumer upon success of the task return by
             * {@link ConsumerState#createTask(ShardConsumer)}.
             * 
             * @return the next state that the consumer should transition to, this may be the same object as the current
             *         state.
             */
            ConsumerState SuccessTransition();

            /**
             * Provides the next state of the consumer when a shutdown has been requested. The returned state is dependent
             * on the current state, and the shutdown reason.
             * 
             * @param shutdownReason
             *            the reason that a shutdown was requested
             * @return the next state that the consumer should transition to, this may be the same object as the current
             *         state.
             */
            ConsumerState ShutdownTransition(ShutdownReason shutdownReason);

            /**
             * The type of task that {@link ConsumerState#createTask(ShardConsumer)} would return. This is always a valid state
             * even if createTask would return a null value.
             * 
             * @return the type of task that this state represents.
             */
            TaskType GetTaskType();

            /**
             * An enumeration represent the type of this state. Different consumer states may return the same
             * {@link ShardConsumerState}.
             * 
             * @return the type of consumer state this represents.
             */
            ShardConsumerState GetState();

            bool IsTerminal();
        }

        /**
         * The initial state that any {@link ShardConsumer} should start in.
         */
        public static readonly ConsumerState INITIAL_STATE = ShardConsumerState.WAITING_ON_PARENT_SHARDS.ConsumerState;

        private static ConsumerState ShutdownStateFor(ShutdownReason reason)
        {
            if(reason == ShutdownReason.REQUESTED)
            {
                return ShardConsumerState.SHUTDOWN_REQUESTED.ConsumerState;
            }
            else if(reason == ShutdownReason.TERMINATE || reason == ShutdownReason.ZOMBIE)
            {
                return ShardConsumerState.SHUTTING_DOWN.ConsumerState;
            }

            throw new ArgumentException("Unknown reason: " + reason);
        }

        /**
         * This is the initial state of a shard consumer. This causes the consumer to remain blocked until the all parent
         * shards have been completed.
         *
         * <h2>Valid Transitions</h2>
         * <dl>
         * <dt>Success</dt>
         * <dd>Transition to the initializing state to allow the record processor to be initialized in preparation of
         * processing.</dd>
         * <dt>Shutdown</dt>
         * <dd>
         * <dl>
         * <dt>All Reasons</dt>
         * <dd>Transitions to {@link ShutdownCompleteState}. Since the record processor was never initialized it can't be
         * informed of the shutdown.</dd>
         * </dl>
         * </dd>
         * </dl>
         */
        public class BlockedOnParentState : ConsumerState
        {
            public ITask CreateTask(ShardConsumer consumer)
            {
                return new BlockOnParentShardTask(consumer.ShardInfo, 
                                                  consumer.LeaseManager, 
                                                  consumer.ParentShardPollIntervalMillis);
            }

            public ConsumerState SuccessTransition()
            {
                return ShardConsumerState.INITIALIZING.ConsumerState;
            }

            public ConsumerState ShutdownTransition(ShutdownReason shutdownReason)
            {
                return ShardConsumerState.SHUTDOWN_COMPLETE.ConsumerState;
            }

            public TaskType GetTaskType()
            {
                return TaskType.BLOCK_ON_PARENT_SHARDS;
            }

            public ShardConsumerState GetState()
            {
                return ShardConsumerState.WAITING_ON_PARENT_SHARDS;
            }

            public bool IsTerminal()
            {
                return false;
            }
        }

        /**
         * This state is responsible for initializing the record processor with the shard information.
         * <h2>Valid Transitions</h2>
         * <dl>
         * <dt>Success</dt>
         * <dd>Transitions to the processing state which will begin to send records to the record processor</dd>
         * <dt>Shutdown</dt>
         * <dd>At this point the record processor has been initialized, but hasn't processed any records. This requires that
         * the record processor be notified of the shutdown, even though there is almost no actions the record processor
         * could take.
         * <dl>
         * <dt>{@link ShutdownReason#REQUESTED}</dt>
         * <dd>Transitions to the {@link ShutdownNotificationState}</dd>
         * <dt>{@link ShutdownReason#ZOMBIE}</dt>
         * <dd>Transitions to the {@link ShuttingDownState}</dd>
         * <dt>{@link ShutdownReason#TERMINATE}</dt>
         * <dd>
         * <p>
         * This reason should not occur, since terminate is triggered after reaching the end of a shard. Initialize never
         * makes an requests to Kinesis for records, so it can't reach the end of a shard.
         * </p>
         * <p>
         * Transitions to the {@link ShuttingDownState}
         * </p>
         * </dd>
         * </dl>
         * </dd>
         * </dl>
         */
        public class InitializingState : ConsumerState
        {
            public ITask CreateTask(ShardConsumer consumer)
            {
                return new InitializeTask(consumer.ShardInfo, 
                                          consumer.RecordProcessor, 
                                          consumer.Checkpoint,
                                          consumer.RecordProcessorCheckpointer, 
                                          consumer.DataFetcher,
                                          consumer.TaskBackoffTimeMillis, 
                                          consumer.StreamConfig);
            }

            public ConsumerState SuccessTransition()
            {
                return ShardConsumerState.PROCESSING.ConsumerState;
            }


            public ConsumerState ShutdownTransition(ShutdownReason shutdownReason)
            {
                return shutdownReason.ShutdownState;
            }


            public TaskType GetTaskType()
            {
                return TaskType.INITIALIZE;
            }


            public ShardConsumerState GetState()
            {
                return ShardConsumerState.INITIALIZING;
            }


            public bool IsTerminal()
            {
                return false;
            }
        }

        /**
         * This state is responsible for retrieving records from Kinesis, and dispatching them to the record processor.
         * While in this state the only way a transition will occur is if a shutdown has been triggered.
         * <h2>Valid Transitions</h2>
         * <dl>
         * <dt>Success</dt>
         * <dd>Doesn't actually transition, but instead returns the same state</dd>
         * <dt>Shutdown</dt>
         * <dd>At this point records are being retrieved, and processed. It's now possible for the consumer to reach the end
         * of the shard triggering a {@link ShutdownReason#TERMINATE}.
         * <dl>
         * <dt>{@link ShutdownReason#REQUESTED}</dt>
         * <dd>Transitions to the {@link ShutdownNotificationState}</dd>
         * <dt>{@link ShutdownReason#ZOMBIE}</dt>
         * <dd>Transitions to the {@link ShuttingDownState}</dd>
         * <dt>{@link ShutdownReason#TERMINATE}</dt>
         * <dd>Transitions to the {@link ShuttingDownState}</dd>
         * </dl>
         * </dd>
         * </dl>
         */
        public class ProcessingState : ConsumerState
        {
            public ITask CreateTask(ShardConsumer consumer)
            {
                return new ProcessTask(consumer.ShardInfo, 
                                       consumer.StreamConfig, 
                                       consumer.RecordProcessor,
                                       consumer.RecordProcessorCheckpointer, 
                                       consumer.DataFetcher,
                                       consumer.TaskBackoffTimeMillis, 
                                       consumer.SkipShardSyncAtWorkerInitializationIfLeasesExist);
            }

            public ConsumerState SuccessTransition()
            {
                return ShardConsumerState.PROCESSING.ConsumerState;
            }

            public ConsumerState ShutdownTransition(ShutdownReason shutdownReason)
            {
                return shutdownReason.ShutdownState;
            }

            public TaskType GetTaskType()
            {
                return TaskType.PROCESS;
            }

            public ShardConsumerState GetState()
            {
                return ShardConsumerState.PROCESSING;
            }

            public bool IsTerminal()
            {
                return false;
            }
        }

        static readonly ConsumerState SHUTDOWN_REQUEST_COMPLETION_STATE = new ShutdownNotificationCompletionState();

        /**
         * This state occurs when a shutdown has been explicitly requested. This shutdown allows the record processor a
         * chance to checkpoint and prepare to be shutdown via the normal method. This state can only be reached by a
         * shutdown on the {@link InitializingState} or {@link ProcessingState}.
         *
         * <h2>Valid Transitions</h2>
         * <dl>
         * <dt>Success</dt>
         * <dd>Success shouldn't normally be called since the {@link ShardConsumer} is marked for shutdown.</dd>
         * <dt>Shutdown</dt>
         * <dd>At this point records are being retrieved, and processed. An explicit shutdown will allow the record
         * processor one last chance to checkpoint, and then the {@link ShardConsumer} will be held in an idle state.
         * <dl>
         * <dt>{@link ShutdownReason#REQUESTED}</dt>
         * <dd>Remains in the {@link ShardConsumerState#SHUTDOWN_REQUESTED}, but the state implementation changes to
         * {@link ShutdownNotificationCompletionState}</dd>
         * <dt>{@link ShutdownReason#ZOMBIE}</dt>
         * <dd>Transitions to the {@link ShuttingDownState}</dd>
         * <dt>{@link ShutdownReason#TERMINATE}</dt>
         * <dd>Transitions to the {@link ShuttingDownState}</dd>
         * </dl>
         * </dd>
         * </dl>
         */
        public class ShutdownNotificationState : ConsumerState
        {
            public ITask CreateTask(ShardConsumer consumer)
            {
                return new ShutdownNotificationTask(consumer.RecordProcessor, 
                                                    consumer.RecordProcessorCheckpointer, 
                                                    consumer.ShutdownNotification, 
                                                    consumer.ShardInfo);
            }

            public ConsumerState SuccessTransition()
            {
                return SHUTDOWN_REQUEST_COMPLETION_STATE;
            }

            public ConsumerState ShutdownTransition(ShutdownReason shutdownReason)
            {
                if (shutdownReason == ShutdownReason.REQUESTED)
                {
                    return SHUTDOWN_REQUEST_COMPLETION_STATE;
                }
                return shutdownReason.ShutdownState;
            }

            public TaskType GetTaskType()
            {
                return TaskType.SHUTDOWN_NOTIFICATION;
            }

            public ShardConsumerState GetState()
            {
                return ShardConsumerState.SHUTDOWN_REQUESTED;
            }

            public bool IsTerminal()
            {
                return false;
            }
        }

        /**
         * Once the {@link ShutdownNotificationState} has been completed the {@link ShardConsumer} must not re-enter any of the
         * processing states. This state idles the {@link ShardConsumer} until the worker triggers the final shutdown state.
         *
         * <h2>Valid Transitions</h2>
         * <dl>
         * <dt>Success</dt>
         * <dd>
         * <p>
         * Success shouldn't normally be called since the {@link ShardConsumer} is marked for shutdown.
         * </p>
         * <p>
         * Remains in the {@link ShutdownNotificationCompletionState}
         * </p>
         * </dd>
         * <dt>Shutdown</dt>
         * <dd>At this point the {@link ShardConsumer} has notified the record processor of the impending shutdown, and is
         * waiting that notification. While waiting for the notification no further processing should occur on the
         * {@link ShardConsumer}.
         * <dl>
         * <dt>{@link ShutdownReason#REQUESTED}</dt>
         * <dd>Remains in the {@link ShardConsumerState#SHUTDOWN_REQUESTED}, and the state implementation remains
         * {@link ShutdownNotificationCompletionState}</dd>
         * <dt>{@link ShutdownReason#ZOMBIE}</dt>
         * <dd>Transitions to the {@link ShuttingDownState}</dd>
         * <dt>{@link ShutdownReason#TERMINATE}</dt>
         * <dd>Transitions to the {@link ShuttingDownState}</dd>
         * </dl>
         * </dd>
         * </dl>
         */
        public class ShutdownNotificationCompletionState : ConsumerState
        {
            public ITask CreateTask(ShardConsumer consumer)
            {
                return null;
            }

            public ConsumerState SuccessTransition()
            {
                return this;
            }

            public ConsumerState ShutdownTransition(ShutdownReason shutdownReason)
            {
                if (shutdownReason != ShutdownReason.REQUESTED)
                {
                    return shutdownReason.ShutdownState;
                }
                return this;
            }

            public TaskType GetTaskType()
            {
                return TaskType.SHUTDOWN_NOTIFICATION;
            }

            public ShardConsumerState GetState()
            {
                return ShardConsumerState.SHUTDOWN_REQUESTED;
            }

            public bool IsTerminal()
            {
                return false;
            }
        }

        /**
         * This state is entered if the {@link ShardConsumer} loses its lease, or reaches the end of the shard.
         *
         * <h2>Valid Transitions</h2>
         * <dl>
         * <dt>Success</dt>
         * <dd>
         * <p>
         * Success shouldn't normally be called since the {@link ShardConsumer} is marked for shutdown.
         * </p>
         * <p>
         * Transitions to the {@link ShutdownCompleteState}
         * </p>
         * </dd>
         * <dt>Shutdown</dt>
         * <dd>At this point the record processor has processed the final shutdown indication, and depending on the shutdown
         * reason taken the correct course of action. From this point on there should be no more interactions with the
         * record processor or {@link ShardConsumer}.
         * <dl>
         * <dt>{@link ShutdownReason#REQUESTED}</dt>
         * <dd>
         * <p>
         * This should not occur as all other {@link ShutdownReason}s take priority over it.
         * </p>
         * <p>
         * Transitions to {@link ShutdownCompleteState}
         * </p>
         * </dd>
         * <dt>{@link ShutdownReason#ZOMBIE}</dt>
         * <dd>Transitions to the {@link ShutdownCompleteState}</dd>
         * <dt>{@link ShutdownReason#TERMINATE}</dt>
         * <dd>Transitions to the {@link ShutdownCompleteState}</dd>
         * </dl>
         * </dd>
         * </dl>
         */
        public class ShuttingDownState : ConsumerState
        {
            public ITask CreateTask(ShardConsumer consumer)
            {
                return new ShutdownTask(consumer.ShardInfo, 
                                        consumer.RecordProcessor,
                                        consumer.RecordProcessorCheckpointer, 
                                        consumer.ShutdownReason,
                                        consumer.StreamConfig.StreamProxy,
                                        consumer.StreamConfig.InitialPositionInStream,
                                        consumer.CleanupLeasesOfCompletedShards, 
                                        consumer.LeaseManager,
                                        consumer.TaskBackoffTimeMillis);
            }

            public ConsumerState SuccessTransition()
            {
                return ShardConsumerState.SHUTDOWN_COMPLETE.ConsumerState;
            }

            public ConsumerState ShutdownTransition(ShutdownReason shutdownReason)
            {
                return ShardConsumerState.SHUTDOWN_COMPLETE.ConsumerState;
            }

            public TaskType GetTaskType()
            {
                return TaskType.SHUTDOWN;
            }

            public ShardConsumerState GetState()
            {
                return ShardConsumerState.SHUTTING_DOWN;
            }

            public bool IsTerminal()
            {
                return false;
            }
        }

        /**
         * This is the final state for the {@link ShardConsumer}. This occurs once all shutdown activities are completed.
         *
         * <h2>Valid Transitions</h2>
         * <dl>
         * <dt>Success</dt>
         * <dd>
         * <p>
         * Success shouldn't normally be called since the {@link ShardConsumer} is marked for shutdown.
         * </p>
         * <p>
         * Remains in the {@link ShutdownCompleteState}
         * </p>
         * </dd>
         * <dt>Shutdown</dt>
         * <dd>At this point the all shutdown activites are completed, and the {@link ShardConsumer} should not take any
         * further actions.
         * <dl>
         * <dt>{@link ShutdownReason#REQUESTED}</dt>
         * <dd>
         * <p>
         * This should not occur as all other {@link ShutdownReason}s take priority over it.
         * </p>
         * <p>
         * Remains in {@link ShutdownCompleteState}
         * </p>
         * </dd>
         * <dt>{@link ShutdownReason#ZOMBIE}</dt>
         * <dd>Remains in {@link ShutdownCompleteState}</dd>
         * <dt>{@link ShutdownReason#TERMINATE}</dt>
         * <dd>Remains in {@link ShutdownCompleteState}</dd>
         * </dl>
         * </dd>
         * </dl>
         */
        public class ShutdownCompleteState : ConsumerState
        {
            public ITask CreateTask(ShardConsumer consumer)
            {
                if (consumer.ShutdownNotification != null)
                {
                    consumer.ShutdownNotification.ShutdownComplete();
                }
                return null;
            }

            public ConsumerState SuccessTransition()
            {
                return this;
            }

            public ConsumerState ShutdownTransition(ShutdownReason shutdownReason)
            {
                return this;
            }


            public TaskType GetTaskType()
            {
                return TaskType.SHUTDOWN_COMPLETE;
            }


            public ShardConsumerState GetState()
            {
                return ShardConsumerState.SHUTDOWN_COMPLETE;
            }


            public bool IsTerminal()
            {
                return true;
            }
        }
    }
}