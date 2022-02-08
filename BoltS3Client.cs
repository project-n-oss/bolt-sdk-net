using System;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Internal.Auth;
using Amazon.S3;
using Amazon.Util;

namespace ProjectN.Bolt
{
    /// <summary>
    /// Implementation for accessing S3 via Bolt.
    ///
    /// Provides the same constructors as AmazonS3Client, but always resolves to the Bolt service URL
    /// as specified in environment variable.
    ///
    /// </summary>
    public class BoltS3Client : AmazonS3Client
    {
        private static string Region()
        {
            return EC2InstanceMetadata.Region?.SystemName
                ?? Environment.GetEnvironmentVariable("AWS_REGION");
        }

        /// <summary>
        /// Gets or sets the url of the bolt service endpoint. The value defaults
        /// to the BOLT_URL environmental variable.
        /// </summary>
        public static string BoltServiceUrl { get; set; } 
            = Environment.GetEnvironmentVariable("BOLT_URL")?.Replace("{region}", Region());

        private static AmazonS3Config BoltConfig => new AmazonS3Config
        {
            ServiceURL = BoltServiceUrl ?? throw new InvalidOperationException("BOLT_URL not defined in environment"),
            ForcePathStyle = true,
        };

        /// <summary>
        /// Constructs AmazonS3Client with the credentials loaded from the application's
        /// default configuration, and if unsuccessful from the Instance Profile service on an EC2 instance.
        /// </summary>
        public BoltS3Client() : base(BoltConfig)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with the credentials loaded from the application's
        /// default configuration, and if unsuccessful from the Instance Profile service on an EC2 instance.
        /// </summary>
        /// <param name="region">The region to connect.</param>
        public BoltS3Client(RegionEndpoint region) : base(BoltConfig)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with the credentials loaded from the application's
        /// default configuration, and if unsuccessful from the Instance Profile service on an EC2 instance.
        /// </summary>
        /// <param name="config">The AmazonS3Client Configuration Object</param>
        public BoltS3Client(AmazonS3Config config) : base(config)
        {
            FixConfig(config);
        }

        /// <summary>Constructs AmazonS3Client with AWS Credentials</summary>
        /// <param name="credentials">AWS Credentials</param>
        public BoltS3Client(AWSCredentials credentials) : base(credentials, BoltConfig)
        {
        }

        /// <summary>Constructs AmazonS3Client with AWS Credentials</summary>
        /// <param name="credentials">AWS Credentials</param>
        /// <param name="region">The region to connect.</param>
        public BoltS3Client(AWSCredentials credentials, RegionEndpoint region) : base(credentials, BoltConfig)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Credentials and an
        /// AmazonS3Client Configuration object.
        /// </summary>
        /// <param name="credentials">AWS Credentials</param>
        /// <param name="clientConfig">The AmazonS3Client Configuration Object</param>
        public BoltS3Client(AWSCredentials credentials, AmazonS3Config clientConfig) : base(credentials, clientConfig)
        {
            FixConfig(clientConfig);
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID and AWS Secret Key
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        public BoltS3Client(string awsAccessKeyId, string awsSecretAccessKey) : base(awsAccessKeyId, awsSecretAccessKey,
            BoltConfig)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID and AWS Secret Key
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        /// <param name="region">The region to connect.</param>
        public BoltS3Client(string awsAccessKeyId, string awsSecretAccessKey, RegionEndpoint region) : base(
            awsAccessKeyId, awsSecretAccessKey, BoltConfig)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID, AWS Secret Key and an
        /// AmazonS3Client Configuration object.
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        /// <param name="clientConfig">The AmazonS3Client Configuration Object</param>
        public BoltS3Client(string awsAccessKeyId, string awsSecretAccessKey, AmazonS3Config clientConfig) : base(
            awsAccessKeyId, awsSecretAccessKey, clientConfig)
        {
            FixConfig(clientConfig);
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID and AWS Secret Key
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        /// <param name="awsSessionToken">AWS Session Token</param>
        public BoltS3Client(string awsAccessKeyId, string awsSecretAccessKey, string awsSessionToken) : base(
            awsAccessKeyId, awsSecretAccessKey, awsSessionToken, BoltConfig)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID and AWS Secret Key
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        /// <param name="awsSessionToken">AWS Session Token</param>
        /// <param name="region">The region to connect.</param>
        public BoltS3Client(string awsAccessKeyId, string awsSecretAccessKey, string awsSessionToken,
            RegionEndpoint region) : base(awsAccessKeyId, awsSecretAccessKey, awsSessionToken, BoltConfig)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID, AWS Secret Key and an
        /// AmazonS3Client Configuration object.
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        /// <param name="awsSessionToken">AWS Session Token</param>
        /// <param name="clientConfig">The AmazonS3Client Configuration Object</param>
        public BoltS3Client(string awsAccessKeyId, string awsSecretAccessKey, string awsSessionToken,
            AmazonS3Config clientConfig) : base(awsAccessKeyId, awsSecretAccessKey, awsSessionToken, clientConfig)
        {
            FixConfig(clientConfig);
        }

        /// <summary>Creates the signer for the service.</summary>
        protected override AbstractAWSSigner CreateSigner()
        {
            return new BoltSigner();
        }

        private static void FixConfig(AmazonS3Config clientConfig, string boltUrl = null)
        {
            clientConfig.ServiceURL = boltUrl ?? BoltServiceUrl
                ?? throw new InvalidOperationException("BOLT_URL not defined in environment");
            clientConfig.ForcePathStyle = true;
        }
    }
}