using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TestKinesisStreamProvider.ClientLibrary.Leases.Impl;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Interfaces
{
    public interface ILeaseManager<T> where T : Lease
    {
        /**
         * Creates the table that will store leases. Succeeds if table already exists.
         * 
         * @param readCapacity
         * @param writeCapacity
         * 
         * @return true if we created a new table (table didn't exist before)
         * 
         * @throws ProvisionedThroughputException if we cannot create the lease table due to per-AWS-account capacity
         *         restrictions.
         * @throws DependencyException if DynamoDB createTable fails in an unexpected way
         */
        bool CreateLeaseTableIfNotExists(long readCapacity, long writeCapacity);

        /**
         * @return true if the lease table already exists.
         * 
         * @throws DependencyException if DynamoDB describeTable fails in an unexpected way
         */
        bool LeaseTableExists();

        /**
         * Blocks until the lease table exists by polling leaseTableExists.
         * 
         * @param secondsBetweenPolls time to wait between polls in seconds
         * @param timeoutSeconds total time to wait in seconds
         * 
         * @return true if table exists, false if timeout was reached
         * 
         * @throws DependencyException if DynamoDB describeTable fails in an unexpected way
         */
        bool WaitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds);

        /**
         * List all objects in table synchronously.
         * 
         * @throws DependencyException if DynamoDB scan fails in an unexpected way
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
         * 
         * @return list of leases
         */
        List<T> ListLeases();

        /**
         * Create a new lease. Conditional on a lease not already existing with this shardId.
         * 
         * @param lease the lease to create
         * 
         * @return true if lease was created, false if lease already exists
         * 
         * @throws DependencyException if DynamoDB put fails in an unexpected way
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB put fails due to lack of capacity
         */
        bool CreateLeaseIfNotExists(T lease);

        /**
         * @param shardId Get the lease for this shardId
         * 
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB get fails due to lack of capacity
         * @throws DependencyException if DynamoDB get fails in an unexpected way
         * 
         * @return lease for the specified shardId, or null if one doesn't exist
         */
        T GetLease(String shardId);

        /**
         * Renew a lease by incrementing the lease counter. Conditional on the leaseCounter in DynamoDB matching the leaseCounter
         * of the input. Mutates the leaseCounter of the passed-in lease object after updating the record in DynamoDB.
         * 
         * @param lease the lease to renew
         * 
         * @return true if renewal succeeded, false otherwise
         * 
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
         * @throws DependencyException if DynamoDB update fails in an unexpected way
         */
        bool RenewLease(T lease);

        /**
         * Take a lease for the given owner by incrementing its leaseCounter and setting its owner field. Conditional on
         * the leaseCounter in DynamoDB matching the leaseCounter of the input. Mutates the leaseCounter and owner of the
         * passed-in lease object after updating DynamoDB.
         * 
         * @param lease the lease to take
         * @param owner the new owner
         * 
         * @return true if lease was successfully taken, false otherwise
         * 
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
         * @throws DependencyException if DynamoDB update fails in an unexpected way
         */
        bool TakeLease(T lease, String owner);

        /**
         * Evict the current owner of lease by setting owner to null. Conditional on the owner in DynamoDB matching the owner of
         * the input. Mutates the lease counter and owner of the passed-in lease object after updating the record in DynamoDB.
         * 
         * @param lease the lease to void
         * 
         * @return true if eviction succeeded, false otherwise
         * 
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
         * @throws DependencyException if DynamoDB update fails in an unexpected way
         */
        bool EvictLease(T lease);

        /**
         * Delete the given lease from DynamoDB. Does nothing when passed a lease that does not exist in DynamoDB.
         * 
         * @param lease the lease to delete
         * 
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB delete fails due to lack of capacity
         * @throws DependencyException if DynamoDB delete fails in an unexpected way
         */
        void DeleteLease(T lease);

        /**
         * Delete all leases from DynamoDB. Useful for tools/utils and testing.
         * 
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB scan or delete fail due to lack of capacity
         * @throws DependencyException if DynamoDB scan or delete fail in an unexpected way
         */
        void DeleteAll();

        /**
         * Update application-specific fields of the given lease in DynamoDB. Does not update fields managed by the leasing
         * library such as leaseCounter, leaseOwner, or leaseKey. Conditional on the leaseCounter in DynamoDB matching the
         * leaseCounter of the input. Increments the lease counter in DynamoDB so that updates can be contingent on other
         * updates. Mutates the lease counter of the passed-in lease object.
         * 
         * @return true if update succeeded, false otherwise
         * 
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
         * @throws DependencyException if DynamoDB update fails in an unexpected way
         */
        bool UpdateLease(T lease);

        /**
         * Check (synchronously) if there are any leases in the lease table.
         * 
         * @return true if there are no leases in the lease table
         * 
         * @throws DependencyException if DynamoDB scan fails in an unexpected way
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB scan fails due to lack of capacity
         */
        bool IsLeaseTableEmpty();
    }
}
