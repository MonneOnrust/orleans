using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Leases.Impl;

namespace KinesisClientLibrary.ClientLibrary.Leases.Interfaces
{
    /// <summary>
    /// Utility class that manages the Dictionaryping of Lease objects/operations to records in DynamoDB.
    /// </summary>
    /// <typeparam name="T">Lease subclass, possibly Lease itself</typeparam>
    public interface ILeaseSerializer<T> where T : Lease
    {
        /**
         * Construct a DynamoDB record out of a Lease object
         * 
         * @param lease lease object to serialize
         * @return an attribute value Dictionary representing the lease object
         */
        Dictionary<String, AttributeValue> ToDynamoRecord(T lease);

        /**
         * Construct a Lease object out of a DynamoDB record.
         * 
         * @param dynamoRecord attribute value Dictionary from DynamoDB
         * @return a deserialized lease object representing the attribute value Dictionary
         */
        T FromDynamoRecord(Dictionary<String, AttributeValue> dynamoRecord);

        /**
         * @param lease
         * @return the attribute value Dictionary representing a Lease's hash key given a Lease object.
         */
        Dictionary<String, AttributeValue> GetDynamoHashKey(T lease);

        /**
         * Special getDynamoHashKey implementation used by ILeaseManager.getLease().
         * 
         * @param leaseKey
         * @return the attribute value Dictionary representing a Lease's hash key given a string.
         */
        Dictionary<String, AttributeValue> GetDynamoHashKey(String leaseKey);

        /**
         * @param lease
         * @return the attribute value Dictionary asserting that a lease counter is what we expect.
         */
        Dictionary<String, ExpectedAttributeValue> GetDynamoLeaseCounterExpectation(T lease);

        /**
         * @param lease
         * @return the attribute value Dictionary asserting that the lease owner is what we expect.
         */
        Dictionary<String, ExpectedAttributeValue> GetDynamoLeaseOwnerExpectation(T lease);

        /**
         * @return the attribute value Dictionary asserting that a lease does not exist.
         */
        Dictionary<String, ExpectedAttributeValue> GetDynamoNonexistantExpectation();

        /**
         * @param lease
         * @return the attribute value Dictionary that increments a lease counter
         */
        Dictionary<String, AttributeValueUpdate> GetDynamoLeaseCounterUpdate(T lease);

        /**
         * @param lease
         * @param newOwner
         * @return the attribute value Dictionary that takes a lease for a new owner
         */
        Dictionary<String, AttributeValueUpdate> GetDynamoTakeLeaseUpdate(T lease, String newOwner);

        /**
         * @param lease
         * @return the attribute value Dictionary that voids a lease
         */
        Dictionary<String, AttributeValueUpdate> GetDynamoEvictLeaseUpdate(T lease);

        /**
         * @param lease
         * @return the attribute value Dictionary that updates application-specific data for a lease and increments the lease
         *         counter
         */
        Dictionary<String, AttributeValueUpdate> GetDynamoUpdateLeaseUpdate(T lease);

        /**
         * @return the key schema for creating a DynamoDB table to store leases
         */
        List<KeySchemaElement> GetKeySchema();

        /**
         * @return attribute definitions for creating a DynamoDB table to store leases
         */
        List<AttributeDefinition> GetAttributeDefinitions();
    }

}
