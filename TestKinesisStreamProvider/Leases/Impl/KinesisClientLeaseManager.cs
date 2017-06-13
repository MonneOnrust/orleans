using Amazon.DynamoDBv2;
using System;
using System.Collections.Generic;
using System.Text;
using TestKinesisStreamProvider.ClientLibrary.Leases.Interfaces;
using TestKinesisStreamProvider.ClientLibrary.Types;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Impl
{
    /**
 * An implementation of LeaseManager for the KinesisClientLibrary - takeLease updates the ownerSwitchesSinceCheckpoint field.
 */
    public class KinesisClientLeaseManager : LeaseManager<KinesisClientLease>, IKinesisClientLeaseManager
    {
        /**
         * Constructor.
         * 
         * @param table Leases table
         * @param dynamoDBClient DynamoDB client to use
         */
        public KinesisClientLeaseManager(String table, IAmazonDynamoDB dynamoDBClient) : this(table, dynamoDBClient, false)
        {

        }

        /**
         * Constructor for integration tests - see comment on superclass for documentation on setting the consistentReads
         * flag.
         * 
         * @param table leases table
         * @param dynamoDBClient DynamoDB client to use
         * @param consistentReads true if we want consistent reads for testing purposes.
         */
        public KinesisClientLeaseManager(String table, IAmazonDynamoDB dynamoDBClient, bool consistentReads) : base(table, dynamoDBClient, new KinesisClientLeaseSerializer(), consistentReads)
        {
        }

        /**
         * {@inheritDoc}
         */
        public override bool TakeLease(KinesisClientLease lease, String newOwner)
        {
            String oldOwner = lease.LeaseOwner;

            bool result = base.TakeLease(lease, newOwner);

            if (oldOwner != null && !oldOwner.Equals(newOwner))
            {
                lease.SetOwnerSwitchesSinceCheckpoint(lease.OwnerSwitchesSinceCheckpoint + 1);
            }

            return result;
        }

        /**
         * {@inheritDoc}
         */
        public ExtendedSequenceNumber GetCheckpoint(string shardId)
        {
            ExtendedSequenceNumber checkpoint = null;
            KinesisClientLease lease = GetLease(shardId);
            if (lease != null)
            {
                checkpoint = lease.Checkpoint;
            }
            return checkpoint;
        }
    }
}