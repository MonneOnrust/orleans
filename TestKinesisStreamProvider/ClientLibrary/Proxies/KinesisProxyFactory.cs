using Amazon.Kinesis;
using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Proxies
{
    /**
 * Factory used for instantiating KinesisProxy objects (to fetch data from Kinesis).
 */
    public class KinesisProxyFactory : IKinesisProxyFactory
    {
        private readonly AWSCredentials credentialProvider;
        private static String defaultServiceName = "kinesis";
        private static String defaultRegionId = "us-east-1";
        private static readonly long DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS = 1000L;
        private static readonly int DEFAULT_DESCRIBE_STREAM_RETRY_TIMES = 50;
        private readonly IAmazonKinesis kinesisClient;
        private readonly long describeStreamBackoffTimeInMillis;
        private readonly int maxDescribeStreamRetryAttempts;

        /**
         * Constructor for creating a KinesisProxy factory, using the specified credentials provider and endpoint.
         *
         * @param credentialProvider credentials provider used to sign requests
         * @param endpoint Amazon Kinesis endpoint to use
         */
        public KinesisProxyFactory(AWSCredentials credentialProvider, String endpoint)
            : this(credentialProvider, new AmazonKinesisConfig(), endpoint, defaultServiceName, defaultRegionId, DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES)
        { }

        /**
         * Constructor for KinesisProxy factory using the client configuration to use when interacting with Kinesis.
         *
         * @param credentialProvider credentials provider used to sign requests
         * @param clientConfig Client Configuration used when instantiating an AmazonKinesisClient
         * @param endpoint Amazon Kinesis endpoint to use
         */
        public KinesisProxyFactory(AWSCredentials credentialProvider, AmazonKinesisConfig clientConfig, String endpoint)
                : this(credentialProvider, clientConfig, endpoint, defaultServiceName, defaultRegionId, DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES)
        { }

        /**
         * This constructor may be used to specify the AmazonKinesisClient to use.
         * 
         * @param credentialProvider credentials provider used to sign requests
         * @param client AmazonKinesisClient used to fetch data from Kinesis
         */
        public KinesisProxyFactory(AWSCredentials credentialProvider, IAmazonKinesis client)
            : this(credentialProvider, client, DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES)
        { }

        /**
         * Used internally and for development/testing.
         * 
         * @param credentialProvider credentials provider used to sign requests
         * @param clientConfig Client Configuration used when instantiating an AmazonKinesisClient
         * @param endpoint Amazon Kinesis endpoint to use
         * @param serviceName service name
         * @param regionId region id
         * @param describeStreamBackoffTimeInMillis backoff time for describing stream in millis
         * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
         */
        KinesisProxyFactory(AWSCredentials credentialProvider,
                            AmazonKinesisConfig clientConfig,
                            String endpoint,
                            String serviceName,
                            String regionId,
                            long describeStreamBackoffTimeInMillis,
                            int maxDescribeStreamRetryAttempts)
        :
            this(credentialProvider, 
                 BuildClientSettingEndpoint(credentialProvider, clientConfig, endpoint, serviceName, regionId),
                 describeStreamBackoffTimeInMillis, maxDescribeStreamRetryAttempts)
        { }

        /**
         * Used internally in the class (and for development/testing).
         * 
         * @param credentialProvider credentials provider used to sign requests
         * @param client AmazonKinesisClient used to fetch data from Kinesis
         * @param describeStreamBackoffTimeInMillis backoff time for describing stream in millis
         * @param maxDescribeStreamRetryAttempts Number of retry attempts for DescribeStream calls
         */
        private KinesisProxyFactory(AWSCredentials credentialProvider, IAmazonKinesis client, long describeStreamBackoffTimeInMillis, int maxDescribeStreamRetryAttempts)
            : base()
        {
            this.kinesisClient = client;
            this.credentialProvider = credentialProvider;
            this.describeStreamBackoffTimeInMillis = describeStreamBackoffTimeInMillis;
            this.maxDescribeStreamRetryAttempts = maxDescribeStreamRetryAttempts;
        }

        /**
         * {@inheritDoc}
         */
        public IKinesisProxy GetProxy(String streamName)
        {
            return new KinesisProxy(streamName, credentialProvider, kinesisClient, describeStreamBackoffTimeInMillis, maxDescribeStreamRetryAttempts);
        }

        private static AmazonKinesisClient BuildClientSettingEndpoint(AWSCredentials credentialProvider,
                                                                    AmazonKinesisConfig clientConfig,
                                                                    String endpoint,
                                                                    String serviceName,
                                                                    String regionId)
        {
            clientConfig.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(regionId);
            clientConfig.AuthenticationRegion = regionId;

            AmazonKinesisClient client = new AmazonKinesisClient(credentialProvider, clientConfig);
            //client.Endpoint = endpoint;
            //client.SignerRegionOverride = regionId;
            return client;
        }
    }
}