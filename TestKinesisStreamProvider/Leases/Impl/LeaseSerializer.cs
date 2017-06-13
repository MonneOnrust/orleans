using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Text;
using TestKinesisStreamProvider.ClientLibrary.Leases.Interfaces;
using TestKinesisStreamProvider.ClientLibrary.Leases.Utils;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Impl
{
    /// <summary>
    /// An implementation of ILeaseSerializer for basic Lease objects. Can also instantiate subclasses of Lease so that
    /// LeaseSerializer can be decorated by other classes if you need to add fields to leases.
    /// </summary>
    public class LeaseSerializer<T> : ILeaseSerializer<T> where T : Lease, new()
    {
        public readonly String LEASE_KEY_KEY = "leaseKey";
        public readonly String LEASE_OWNER_KEY = "leaseOwner";
        public readonly String LEASE_COUNTER_KEY = "leaseCounter";

        public LeaseSerializer()
        {
        }

        public Dictionary<String, AttributeValue> ToDynamoRecord(T lease)
        {
            Dictionary<String, AttributeValue> result = new Dictionary<String, AttributeValue>();

            result[LEASE_KEY_KEY] = DynamoUtils.createAttributeValue(lease.LeaseKey);
            result[LEASE_COUNTER_KEY] = DynamoUtils.createAttributeValue(lease.LeaseCounter);

            if (lease.LeaseOwner != null)
            {
                result[LEASE_OWNER_KEY] = DynamoUtils.createAttributeValue(lease.LeaseOwner);
            }

            return result;
        }


        public T FromDynamoRecord(Dictionary<String, AttributeValue> dynamoRecord)
        {
            T result;
            //try
            //{
                result = new T();
            //}
            //catch (InstantiationException e)
            //{
            //    throw new RuntimeException(e);
            //}
            //catch (IllegalAccessException e)
            //{
            //    throw new RuntimeException(e);
            //}

            result.LeaseKey = DynamoUtils.SafeGetString(dynamoRecord, LEASE_KEY_KEY);
            result.LeaseOwner = DynamoUtils.SafeGetString(dynamoRecord, LEASE_OWNER_KEY);
            result.LeaseCounter = DynamoUtils.SafeGetLong(dynamoRecord, LEASE_COUNTER_KEY);

            return result;
        }

        public Dictionary<String, AttributeValue> GetDynamoHashKey(String leaseKey)
        {
            Dictionary<String, AttributeValue> result = new Dictionary<String, AttributeValue>();

            result[LEASE_KEY_KEY] = DynamoUtils.createAttributeValue(leaseKey);

            return result;
        }


        public Dictionary<String, AttributeValue> GetDynamoHashKey(T lease)
        {
            return GetDynamoHashKey(lease.LeaseKey);
        }


        public Dictionary<String, ExpectedAttributeValue> GetDynamoLeaseCounterExpectation(T lease)
        {
            return GetDynamoLeaseCounterExpectation(lease.LeaseCounter);
        }

        public Dictionary<String, ExpectedAttributeValue> GetDynamoLeaseCounterExpectation(long leaseCounter)
        {
            Dictionary<String, ExpectedAttributeValue> result = new Dictionary<String, ExpectedAttributeValue>();

            ExpectedAttributeValue eav = new ExpectedAttributeValue(DynamoUtils.createAttributeValue(leaseCounter));
            result[LEASE_COUNTER_KEY] = eav;

            return result;
        }


        public Dictionary<String, ExpectedAttributeValue> GetDynamoLeaseOwnerExpectation(T lease)
        {
            Dictionary<String, ExpectedAttributeValue> result = new Dictionary<String, ExpectedAttributeValue>();

            ExpectedAttributeValue eav = null;

            if (lease.LeaseOwner == null)
            {
                eav = new ExpectedAttributeValue(false);
            }
            else
            {
                eav = new ExpectedAttributeValue(DynamoUtils.createAttributeValue(lease.LeaseOwner));
            }

            result[LEASE_OWNER_KEY] = eav;

            return result;
        }


        public Dictionary<String, ExpectedAttributeValue> GetDynamoNonexistantExpectation()
        {
            Dictionary<String, ExpectedAttributeValue> result = new Dictionary<String, ExpectedAttributeValue>();

            ExpectedAttributeValue expectedAV = new ExpectedAttributeValue(false);
            result[LEASE_KEY_KEY] = expectedAV;

            return result;
        }


        public Dictionary<String, AttributeValueUpdate> GetDynamoLeaseCounterUpdate(T lease)
        {
            return GetDynamoLeaseCounterUpdate(lease.LeaseCounter);
        }

        public Dictionary<String, AttributeValueUpdate> GetDynamoLeaseCounterUpdate(long leaseCounter)
        {
            Dictionary<String, AttributeValueUpdate> result = new Dictionary<String, AttributeValueUpdate>();

            AttributeValueUpdate avu = new AttributeValueUpdate(DynamoUtils.createAttributeValue(leaseCounter + 1), AttributeAction.PUT);
            result[LEASE_COUNTER_KEY] = avu;

            return result;
        }


        public Dictionary<String, AttributeValueUpdate> GetDynamoTakeLeaseUpdate(T lease, String owner)
        {
            Dictionary<String, AttributeValueUpdate> result = new Dictionary<String, AttributeValueUpdate>();

            result[LEASE_OWNER_KEY] = new AttributeValueUpdate(DynamoUtils.createAttributeValue(owner), AttributeAction.PUT);

            return result;
        }


        public Dictionary<String, AttributeValueUpdate> GetDynamoEvictLeaseUpdate(T lease)
        {
            Dictionary<String, AttributeValueUpdate> result = new Dictionary<String, AttributeValueUpdate>();

            result[LEASE_OWNER_KEY] = new AttributeValueUpdate(null, AttributeAction.DELETE);

            return result;
        }


        public Dictionary<String, AttributeValueUpdate> GetDynamoUpdateLeaseUpdate(T lease)
        {
            // There is no application-specific data in Lease - just return a Dictionary that increments the counter.
            return new Dictionary<String, AttributeValueUpdate>();
        }


        public List<KeySchemaElement> GetKeySchema()
        {
            List<KeySchemaElement> keySchema = new List<KeySchemaElement>();
            keySchema.Add(new KeySchemaElement { AttributeName = LEASE_KEY_KEY, KeyType = KeyType.HASH });

            return keySchema;
        }


        public List<AttributeDefinition> GetAttributeDefinitions()
        {
            List<AttributeDefinition> definitions = new List<AttributeDefinition>();
            definitions.Add(new AttributeDefinition { AttributeName = LEASE_KEY_KEY, AttributeType = ScalarAttributeType.S });

            return definitions;
        }
    }
}