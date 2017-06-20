using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KinesisClientLibrary.ClientLibrary.Leases.Exceptions;
using KinesisClientLibrary.ClientLibrary.Leases.Interfaces;
using KinesisClientLibrary.ClientLibrary.Leases.Utils;

namespace KinesisClientLibrary.ClientLibrary.Leases.Impl
{
    /**
 * An implementation of ILeaseManager that uses DynamoDB.
 */
    public class LeaseManager<T> : ILeaseManager<T> where T : Lease
    {
        protected String table;
        protected IAmazonDynamoDB dynamoDBClient;
        protected ILeaseSerializer<T> serializer;
        protected bool consistentReads;

        /**
         * Constructor.
         * 
         * @param table leases table
         * @param dynamoDBClient DynamoDB client to use
         * @param serializer LeaseSerializer to use to convert to/from DynamoDB objects.
         */
        public LeaseManager(String table, IAmazonDynamoDB dynamoDBClient, ILeaseSerializer<T> serializer) : this(table, dynamoDBClient, serializer, false)
        {
        }

        /**
         * Constructor for test cases - allows control of consistent reads. Consistent reads should only be used for testing
         * - our code is meant to be resilient to inconsistent reads. Using consistent reads during testing speeds up
         * execution of simple tests (you don't have to wait out the consistency window). Test cases that want to experience
         * eventual consistency should not set consistentReads=true.
         * 
         * @param table leases table
         * @param dynamoDBClient DynamoDB client to use
         * @param serializer lease serializer to use
         * @param consistentReads true if we want consistent reads for testing purposes.
         */
        public LeaseManager(String table, IAmazonDynamoDB dynamoDBClient, ILeaseSerializer<T> serializer, bool consistentReads)
        {
            VerifyNotNull(table, "Table name cannot be null");
            VerifyNotNull(dynamoDBClient, "dynamoDBClient cannot be null");
            VerifyNotNull(serializer, "ILeaseSerializer cannot be null");

            this.table = table;
            this.dynamoDBClient = dynamoDBClient;
            this.consistentReads = consistentReads;
            this.serializer = serializer;
        }

        /**
         * {@inheritDoc}
         */
        public bool CreateLeaseTableIfNotExists(long readCapacity, long writeCapacity)
        {
            VerifyNotNull(readCapacity, "readCapacity cannot be null");
            VerifyNotNull(writeCapacity, "writeCapacity cannot be null");

            try
            {
                if (tableStatus() != null)
                {
                    return false;
                }
            }
            catch (DependencyException de)
            {
                //
                // Something went wrong with DynamoDB
                //
                Trace.TraceError("Failed to get table status for " + table, de.ToString());
            }
            CreateTableRequest request = new CreateTableRequest();
            request.TableName = table;
            request.KeySchema = serializer.GetKeySchema();
            request.AttributeDefinitions.AddRange(serializer.GetAttributeDefinitions());

            ProvisionedThroughput throughput = new ProvisionedThroughput();
            throughput.ReadCapacityUnits = readCapacity;
            throughput.WriteCapacityUnits = writeCapacity;
            request.ProvisionedThroughput = throughput;

            try
            {
                var task = dynamoDBClient.CreateTableAsync(request);
                task.Wait();
            }
            catch (ResourceInUseException)
            {
                Trace.TraceInformation("Table " + table + " already exists.");
                return false;
            }
            catch (LimitExceededException e)
            {
                throw new ProvisionedThroughputException("Capacity exceeded when creating table " + table, e);
            }
            catch (AmazonClientException e)
            {
                throw new DependencyException(e.Message, e);
            }
            return true;
        }

        /**
         * {@inheritDoc}
         */
        public bool LeaseTableExists()
        {
            return TableStatus.ACTIVE == tableStatus();
        }

        private TableStatus tableStatus()
        {
            DescribeTableRequest request = new DescribeTableRequest();

            request.TableName = table;

            DescribeTableResponse result;
            try
            {
                var task = dynamoDBClient.DescribeTableAsync(request);
                task.Wait();
                result = task.Result;
            }
            catch (ResourceNotFoundException)
            {
                Trace.WriteLine(String.Format("Got ResourceNotFoundException for table {0} in leaseTableExists, returning false.", table));
                return null;
            }
            catch (AmazonClientException e)
            {
                throw new DependencyException(e.Message, e);
            }

            TableStatus tableStatus = new TableStatus(result.Table.TableStatus);//.fromValue(result.getTable().getTableStatus());
            Trace.WriteLine("Lease table exists and is in status " + tableStatus);

            return tableStatus;
        }

        public bool WaitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds)
        {
            long sleepTimeRemaining = timeoutSeconds * 1000;

            while (!LeaseTableExists())
            {
                if (sleepTimeRemaining <= 0)
                {
                    return false;
                }

                long timeToSleepMillis = Math.Min(secondsBetweenPolls * 1000, sleepTimeRemaining);

                sleepTimeRemaining -= Sleep((int)timeToSleepMillis);
            }

            return true;
        }

        /**
         * Exposed for testing purposes.
         * 
         * @param timeToSleepMillis time to sleep in milliseconds
         * 
         * @return actual time slept in millis
         */
        private int Sleep(int timeToSleepMillis)
        {
            var startTime = Time.MilliTime;

            //try
            //{
            Thread.Sleep(timeToSleepMillis);
            //}
            //catch (InterruptedException e)
            //{
            //    Trace.WriteLine("Interrupted while sleeping");
            //}

            return (int)(Time.MilliTime - startTime);
        }

        /**
         * {@inheritDoc}
         */
        public List<T> ListLeases()
        {
            return List(0);
        }

        /**
         * {@inheritDoc}
         */
        public bool IsLeaseTableEmpty()
        {
            return List(1).Count == 0;
        }

        /**
         * List with the given page size. Package access for integration testing.
         * 
         * @param limit number of items to consider at a time - used by integration tests to force paging.
         * @return list of leases
         * @throws InvalidStateException if table does not exist
         * @throws DependencyException if DynamoDB scan fail in an unexpected way
         * @throws ProvisionedThroughputException if DynamoDB scan fail due to exceeded capacity
         */
        private List<T> List(int limit)
        {
            Trace.WriteLine("Listing leases from table " + table);

            ScanRequest scanRequest = new ScanRequest();
            scanRequest.TableName = table;
            if (limit != 0)
            {
                scanRequest.Limit = limit;
            }

            try
            {
                var task = dynamoDBClient.ScanAsync(scanRequest);
                task.Wait();
                ScanResponse scanResult = task.Result;
                List<T> result = new List<T>();

                while (scanResult != null)
                {
                    foreach (Dictionary<String, AttributeValue> item in scanResult.Items)
                    {
                        Trace.WriteLine("Got item " + item.ToString() + " from DynamoDB.");

                        result.Add(serializer.FromDynamoRecord(item));
                    }

                    Dictionary<String, AttributeValue> lastEvaluatedKey = scanResult.LastEvaluatedKey;
                    if (lastEvaluatedKey == null)
                    {
                        // Signify that we're done.
                        scanResult = null;
                        Trace.WriteLine("lastEvaluatedKey was null - scan finished.");
                    }
                    else
                    {
                        // Make another request, picking up where we left off.
                        scanRequest.ExclusiveStartKey = lastEvaluatedKey;

                        Trace.WriteLine("lastEvaluatedKey was " + lastEvaluatedKey + ", continuing scan.");

                        task = dynamoDBClient.ScanAsync(scanRequest);
                        task.Wait();
                        scanResult = task.Result;
                    }
                }

                Trace.WriteLine("Listed " + result.Count + " leases from table " + table);

                return result;
            }
            catch (ResourceNotFoundException e)
            {
                throw new InvalidStateException("Cannot scan lease table " + table + " because it does not exist.", e);
            }
            catch (ProvisionedThroughputExceededException e)
            {
                throw new ProvisionedThroughputException(e.Message, e);
            }
            catch (AmazonClientException e)
            {
                throw new DependencyException(e.Message, e);
            }
        }

        /**
         * {@inheritDoc}
         */
        public bool CreateLeaseIfNotExists(T lease)
        {
            VerifyNotNull(lease, "lease cannot be null");

            Trace.WriteLine("Creating lease " + lease);

            PutItemRequest request = new PutItemRequest();
            request.TableName = table;
            request.Item = serializer.ToDynamoRecord(lease);
            request.Expected = serializer.GetDynamoNonexistantExpectation();

            try
            {
                var task = dynamoDBClient.PutItemAsync(request);
                task.Wait();
            }
            catch (ConditionalCheckFailedException)
            {
                Trace.WriteLine("Did not create lease " + lease + " because it already existed");

                return false;
            }
            catch (AmazonClientException e)
            {
                throw ConvertAndRethrowExceptions("create", lease.LeaseKey, e);
            }

            return true;
        }

        /**
         * {@inheritDoc}
         */
        public T GetLease(String leaseKey)
        {
            VerifyNotNull(leaseKey, "leaseKey cannot be null");

            Trace.WriteLine("Getting lease with key " + leaseKey);

            GetItemRequest request = new GetItemRequest();
            request.TableName = (table);
            request.Key = (serializer.GetDynamoHashKey(leaseKey));
            request.ConsistentRead = (consistentReads);

            try
            {
                var task = dynamoDBClient.GetItemAsync(request);
                task.Wait();
                GetItemResponse result = task.Result;

                Dictionary<String, AttributeValue> dynamoRecord = result.Item;
                if (dynamoRecord == null)
                {
                    Trace.WriteLine("No lease found with key " + leaseKey + ", returning null.");

                    return null;
                }
                else
                {
                    T lease = serializer.FromDynamoRecord(dynamoRecord);
                    Trace.WriteLine("Got lease " + lease);

                    return lease;
                }
            }
            catch (AmazonClientException e)
            {
                throw ConvertAndRethrowExceptions("get", leaseKey, e);
            }
        }

        /**
         * {@inheritDoc}
         */
        public bool RenewLease(T lease)
        {
            VerifyNotNull(lease, "lease cannot be null");

            Trace.WriteLine("Renewing lease with key " + lease.LeaseKey);

            UpdateItemRequest request = new UpdateItemRequest();
            request.TableName = (table);
            request.Key = (serializer.GetDynamoHashKey(lease));
            request.Expected = (serializer.GetDynamoLeaseCounterExpectation(lease));
            request.AttributeUpdates = (serializer.GetDynamoLeaseCounterUpdate(lease));

            try
            {
                var task = dynamoDBClient.UpdateItemAsync(request);
                task.Wait();
            }
            catch (ConditionalCheckFailedException)
            {
                Trace.WriteLine("Lease renewal failed for lease with key " + lease.LeaseKey + " because the lease counter was not " + lease.LeaseCounter);

                return false;
            }
            catch (AmazonClientException e)
            {
                throw ConvertAndRethrowExceptions("renew", lease.LeaseKey, e);
            }

            lease.LeaseCounter = lease.LeaseCounter + 1;
            return true;
        }

        /**
         * {@inheritDoc}
         */
        public virtual bool TakeLease(T lease, String owner)
        {
            VerifyNotNull(lease, "lease cannot be null");
            VerifyNotNull(owner, "owner cannot be null");

            Trace.WriteLine(String.Format("Taking lease with leaseKey {0} from {1} to {2}", lease.LeaseKey, lease.LeaseOwner == null ? "nobody" : lease.LeaseOwner, owner));

            UpdateItemRequest request = new UpdateItemRequest();
            request.TableName = (table);
            request.Key = (serializer.GetDynamoHashKey(lease));
            request.Expected = (serializer.GetDynamoLeaseCounterExpectation(lease));

            Dictionary<String, AttributeValueUpdate> updates = serializer.GetDynamoLeaseCounterUpdate(lease);
            serializer.GetDynamoTakeLeaseUpdate(lease, owner).ToList().ForEach(de => updates.Add(de.Key, de.Value));
            request.AttributeUpdates = updates;

            try
            {
                var task = dynamoDBClient.UpdateItemAsync(request);
                task.Wait();
            }
            catch (ConditionalCheckFailedException)
            {
                Trace.WriteLine("Lease renewal failed for lease with key " + lease.LeaseKey + " because the lease counter was not " + lease.LeaseCounter);

                return false;
            }
            catch (AmazonClientException e)
            {
                throw ConvertAndRethrowExceptions("take", lease.LeaseKey, e);
            }

            lease.LeaseCounter++;
            lease.LeaseOwner = owner;

            return true;
        }

        /**
         * {@inheritDoc}
         */
        public bool EvictLease(T lease)
        {
            VerifyNotNull(lease, "lease cannot be null");

            Trace.WriteLine(String.Format("Evicting lease with leaseKey {0} owned by {1}", lease.LeaseKey, lease.LeaseOwner));

            UpdateItemRequest request = new UpdateItemRequest();
            request.TableName = (table);
            request.Key = (serializer.GetDynamoHashKey(lease));
            request.Expected = (serializer.GetDynamoLeaseOwnerExpectation(lease));

            Dictionary<String, AttributeValueUpdate> updates = serializer.GetDynamoLeaseCounterUpdate(lease);
            serializer.GetDynamoEvictLeaseUpdate(lease).ToList().ForEach(de => updates.Add(de.Key, de.Value));
            request.AttributeUpdates = (updates);

            try
            {
                var task = dynamoDBClient.UpdateItemAsync(request);
                task.Wait();
            }
            catch (ConditionalCheckFailedException)
            {
                Trace.WriteLine("Lease eviction failed for lease with key " + lease.LeaseKey + " because the lease owner was not " + lease.LeaseOwner);

                return false;
            }
            catch (AmazonClientException e)
            {
                throw ConvertAndRethrowExceptions("evict", lease.LeaseKey, e);
            }

            lease.LeaseOwner = (null);
            lease.LeaseCounter++;
            return true;
        }

        /**
         * {@inheritDoc}
         */
        public void DeleteAll()
        {
            List<T> allLeases = ListLeases();

            Trace.TraceWarning("Deleting " + allLeases.Count() + " items from table " + table);

            foreach (T lease in allLeases)
            {
                DeleteItemRequest deleteRequest = new DeleteItemRequest();
                deleteRequest.TableName = (table);
                deleteRequest.Key = (serializer.GetDynamoHashKey(lease));

                var task = dynamoDBClient.DeleteItemAsync(deleteRequest);
                task.Wait();
            }
        }

        /**
         * {@inheritDoc}
         */
        public void DeleteLease(T lease)
        {
            VerifyNotNull(lease, "lease cannot be null");

            Trace.WriteLine(String.Format("Deleting lease with leaseKey {0}", lease.LeaseKey));

            DeleteItemRequest deleteRequest = new DeleteItemRequest();
            deleteRequest.TableName = (table);
            deleteRequest.Key = (serializer.GetDynamoHashKey(lease));

            try
            {
                var task = dynamoDBClient.DeleteItemAsync(deleteRequest);
                task.Wait();
            }
            catch (AmazonClientException e)
            {
                throw ConvertAndRethrowExceptions("delete", lease.LeaseKey, e);
            }
        }

        /**
         * {@inheritDoc}
         */
        public bool UpdateLease(T lease)
        {
            VerifyNotNull(lease, "lease cannot be null");

            Trace.WriteLine(String.Format("Updating lease {0}", lease));

            UpdateItemRequest request = new UpdateItemRequest();
            request.TableName = (table);
            request.Key = (serializer.GetDynamoHashKey(lease));
            request.Expected = (serializer.GetDynamoLeaseCounterExpectation(lease));

            Dictionary<String, AttributeValueUpdate> updates = serializer.GetDynamoLeaseCounterUpdate(lease);
            serializer.GetDynamoUpdateLeaseUpdate(lease).ToList().ForEach(de => updates.Add(de.Key, de.Value));
            request.AttributeUpdates = (updates);

            try
            {
                var task = dynamoDBClient.UpdateItemAsync(request);
                task.Wait();
            }
            catch (ConditionalCheckFailedException)
            {
                Trace.WriteLine("Lease update failed for lease with key " + lease.LeaseKey + " because the lease counter was not " + lease.LeaseCounter);

                return false;
            }
            catch (AmazonClientException e)
            {
                throw ConvertAndRethrowExceptions("update", lease.LeaseKey, e);
            }

            lease.LeaseCounter++;
            return true;
        }

        /*
         * This method contains boilerplate exception handling - it throws or returns something to be thrown. The
         * inconsistency there exists to satisfy the compiler when this method is used at the end of non-void methods.
         */
        protected DependencyException ConvertAndRethrowExceptions(String operation, String leaseKey, AmazonClientException e)
        {
            if (e.GetType() == typeof(ProvisionedThroughputExceededException))
            {
                throw new ProvisionedThroughputException(e.Message, e);
            }
            else if (e.GetType() == typeof(ResourceNotFoundException))
            {
                // @formatter:on
                throw new InvalidStateException(String.Format("Cannot {0} lease with key {1} because table {2} does not exist.", operation, leaseKey, table), e);
                //@formatter:off
            }
            else
            {
                return new DependencyException(e.Message, e);
            }
        }

        private void VerifyNotNull(Object @object, String message)
        {
            if (@object == null)
            {
                throw new ArgumentException(message);
            }
        }
    }
}
