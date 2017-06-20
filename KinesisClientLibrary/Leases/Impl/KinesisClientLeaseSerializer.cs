using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Leases.Interfaces;
using KinesisClientLibrary.ClientLibrary.Leases.Utils;
using KinesisClientLibrary.ClientLibrary.Types;

namespace KinesisClientLibrary.ClientLibrary.Leases.Impl
{
    /**
 * An implementation of ILeaseSerializer for KinesisClientLease objects.
 */
    public class KinesisClientLeaseSerializer : ILeaseSerializer<KinesisClientLease>
    {
        private static readonly String OWNER_SWITCHES_KEY = "ownerSwitchesSinceCheckpoint";
        private static readonly String CHECKPOINT_SEQUENCE_NUMBER_KEY = "checkpoint";
        private static readonly String CHECKPOINT_SUBSEQUENCE_NUMBER_KEY = "checkpointSubSequenceNumber";
        public readonly String PARENT_SHARD_ID_KEY = "parentShardId";

        private readonly LeaseSerializer<KinesisClientLease> baseSerializer = new LeaseSerializer<KinesisClientLease>();

        public Dictionary<String, AttributeValue> ToDynamoRecord(KinesisClientLease lease)
        {
            Dictionary<String, AttributeValue> result = baseSerializer.ToDynamoRecord(lease);

            result[OWNER_SWITCHES_KEY] = DynamoUtils.createAttributeValue(lease.OwnerSwitchesSinceCheckpoint);
            result[CHECKPOINT_SEQUENCE_NUMBER_KEY] = DynamoUtils.createAttributeValue(lease.Checkpoint.SequenceNumber);
            result[CHECKPOINT_SUBSEQUENCE_NUMBER_KEY] = DynamoUtils.createAttributeValue(lease.Checkpoint.SubSequenceNumber);
            if (lease.GetParentShardIds() != null && lease.GetParentShardIds().Count > 0)
            {
                result[PARENT_SHARD_ID_KEY] = DynamoUtils.createAttributeValue(lease.GetParentShardIds());
            }

            return result;
        }

        public KinesisClientLease FromDynamoRecord(Dictionary<String, AttributeValue> dynamoRecord)
        {
            KinesisClientLease result = (KinesisClientLease)baseSerializer.FromDynamoRecord(dynamoRecord);

            result.SetOwnerSwitchesSinceCheckpoint(DynamoUtils.SafeGetLong(dynamoRecord, OWNER_SWITCHES_KEY));
            result.Checkpoint = new ExtendedSequenceNumber(DynamoUtils.SafeGetString(dynamoRecord, CHECKPOINT_SEQUENCE_NUMBER_KEY),
                                                          DynamoUtils.SafeGetLong(dynamoRecord, CHECKPOINT_SUBSEQUENCE_NUMBER_KEY));
            result.SetParentShardIds(DynamoUtils.SafeGetSS(dynamoRecord, PARENT_SHARD_ID_KEY));

            return result;
        }

        public Dictionary<string, AttributeValue> GetDynamoHashKey(KinesisClientLease lease)
        {
            return baseSerializer.GetDynamoHashKey(lease);
        }

        public Dictionary<String, AttributeValue> GetDynamoHashKey(String shardId)
        {
            return baseSerializer.GetDynamoHashKey(shardId);
        }

        public Dictionary<String, ExpectedAttributeValue> GetDynamoLeaseCounterExpectation(KinesisClientLease lease)
        {
            return baseSerializer.GetDynamoLeaseCounterExpectation(lease);
        }

        public Dictionary<String, ExpectedAttributeValue> GetDynamoLeaseOwnerExpectation(KinesisClientLease lease)
        {
            return baseSerializer.GetDynamoLeaseOwnerExpectation(lease);
        }

        public Dictionary<String, ExpectedAttributeValue> GetDynamoNonexistantExpectation()
        {
            return baseSerializer.GetDynamoNonexistantExpectation();
        }

        public Dictionary<String, AttributeValueUpdate> GetDynamoLeaseCounterUpdate(KinesisClientLease lease)
        {
            return baseSerializer.GetDynamoLeaseCounterUpdate(lease);
        }

        public Dictionary<String, AttributeValueUpdate> GetDynamoTakeLeaseUpdate(KinesisClientLease lease, String newOwner)
        {
            Dictionary<String, AttributeValueUpdate> result = baseSerializer.GetDynamoTakeLeaseUpdate(lease, newOwner);

            String oldOwner = lease.LeaseOwner;
            if (oldOwner != null && !oldOwner.Equals(newOwner))
            {
                result[OWNER_SWITCHES_KEY] = new AttributeValueUpdate(DynamoUtils.createAttributeValue(1L), AttributeAction.ADD);
            }

            return result;
        }

        public Dictionary<String, AttributeValueUpdate> GetDynamoEvictLeaseUpdate(KinesisClientLease lease)
        {
            return baseSerializer.GetDynamoEvictLeaseUpdate(lease);
        }

        public Dictionary<String, AttributeValueUpdate> GetDynamoUpdateLeaseUpdate(KinesisClientLease lease)
        {
            Dictionary<String, AttributeValueUpdate> result = baseSerializer.GetDynamoUpdateLeaseUpdate(lease);

            result[CHECKPOINT_SEQUENCE_NUMBER_KEY] = new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.Checkpoint.SequenceNumber), AttributeAction.PUT);
            result[CHECKPOINT_SUBSEQUENCE_NUMBER_KEY] = new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.Checkpoint.SubSequenceNumber), AttributeAction.PUT);
            result[OWNER_SWITCHES_KEY] = new AttributeValueUpdate(DynamoUtils.createAttributeValue(lease.OwnerSwitchesSinceCheckpoint), AttributeAction.PUT); ;

            return result;
        }

        public List<KeySchemaElement> GetKeySchema()
        {
            return baseSerializer.GetKeySchema();
        }

        public List<AttributeDefinition> GetAttributeDefinitions()
        {
            return baseSerializer.GetAttributeDefinitions();
        }
    }
}
