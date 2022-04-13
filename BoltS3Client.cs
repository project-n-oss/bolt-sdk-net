using System;
using System.Net.Http;

using System.Collections.Generic;

using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.Runtime.Internal.Auth;
using Amazon.S3;
using Amazon.Util;

using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Threading;

namespace ProjectN.Bolt
{
    public static class BoltConfiguration
    {
        public static string Region = Environment.GetEnvironmentVariable("AWS_REGION")
                   ?? EC2InstanceMetadata.Region?.SystemName;
        public static string ZoneId = Environment.GetEnvironmentVariable("AWS_ZONE_ID")
                    ?? EC2InstanceMetadata.GetData("/placement/availability-zone-id");
        public static string CustomDomain = Environment.GetEnvironmentVariable("BOLT_CUSTOM_DOMAIN");
        public static string AuthBucket = Environment.GetEnvironmentVariable("BOLT_AUTH_BUCKET");
    }

    /// <summary>
    /// Implementation for accessing S3 via Bolt.
    ///
    /// Provides the same constructors as AmazonS3Client, but always resolves to the Bolt service URL
    /// as specified in app settings.
    ///
    /// Example App.config:
    /// <code>
    /// &lt;?xml version="1.0" encoding="utf-8" ?&gt;
    /// &lt;configuration&gt;
    ///     &lt;appSettings&gt;
    ///         &lt;add key="BoltURL" value="http://bolt.project.n"/&gt;
    ///     &lt;/appSettings&gt;
    /// &lt;/configuration&
    /// </code>
    /// </summary>
    public class BoltS3Client : AmazonS3Client
    {
        public static string Region;

        private static string ZoneId;
        private static string CustomDomain;

        public static string BoltHostname;

        private static string QuicksilverUrl;

        public static string AuthBucket;

        private static readonly List<string> ReadOrderEndpoints = new List<string> { "main_read_endpoints", "main_write_endpoints", "failover_read_endpoints", "failover_write_endpoints" };
        private static readonly List<string> WriteOrderEndpoints = new List<string> { "main_write_endpoints", "failover_write_endpoints" };
        private static readonly List<string> HttpReadMethodTypes = new List<string> { "GET", "HEAD" }; // S3 operations get converted to one of the standard HTTP request methods https://docs.aws.amazon.com/apigateway/latest/developerguide/integrating-api-with-aws-services-s3.html
        private static readonly Random RandGenerator = new Random();
        private static readonly object syncLock = new object();
        public static int Rand(int min, int max)
        {
            lock (syncLock)
            {
                return RandGenerator.Next(min, max);
            }
        }

        private static readonly HttpClient qsClient = new HttpClient();
        private static ReaderWriterLockSlim endpointCacheLock = new ReaderWriterLockSlim();
        private static DateTime RefreshTime = DateTime.UtcNow.AddSeconds(120);

        private static Dictionary<string, List<string>> BoltEndPoints = null;
        private static async Task<Dictionary<string, List<string>>> GetBoltEndPoints(string errIp)
        {
            // lazy load config
            if (QuicksilverUrl == null)
                UseBoltConfiguration();

            var requestUrl = errIp.Length > 0 ? $"{QuicksilverUrl}&err={errIp}" : QuicksilverUrl;
            try
            {
                using (var result = await qsClient.GetAsync(requestUrl))
                {
                    var responseString = await result.Content.ReadAsStringAsync();
                    return JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(responseString);
                }
            }
            catch (Exception ex)
            {
                if ((BoltEndPoints?.Count ?? 0) > 0)
                {
                    return BoltEndPoints;
                }

                throw new Exception($"Quicksilver url: {requestUrl}, Message: {ex.Message}, StackTrace: {ex.StackTrace}");
            }
        }

        public static void RefreshBoltEndpoints(string errIp)
        {
            if (errIp.Length > 0)
            {
                // If this is a refresh on error, take write lock on cache before performing request to prevent more clients from erroring
                endpointCacheLock.EnterWriteLock();
                try
                {
                    BoltEndPoints = GetBoltEndPoints(errIp).Result;
                }
                finally
                {
                    endpointCacheLock.ExitWriteLock();
                }
            }
            else
            {
                // If this is a normal refresh, take an upgradeable read lock and only upgrade to write if endpoints have changed
                endpointCacheLock.EnterUpgradeableReadLock();
                try
                {

                    var result = GetBoltEndPoints(errIp).Result;
                    // TODO: only need to take write lock if endpoints are actually different
                    //if (EndpointsChanged(result))
                    //{
                    endpointCacheLock.EnterWriteLock();
                    try
                    {
                        BoltEndPoints = result;
                    }
                    finally
                    {
                        endpointCacheLock.ExitWriteLock();
                    }
                    //}
                }
                finally
                {
                    endpointCacheLock.ExitUpgradeableReadLock();
                }
            }
            RefreshTime = DateTime.UtcNow.AddSeconds(Rand(60, 180));
        }

        public static Uri SelectBoltEndPoint(string httpRequestMethod)
        {
            if (DateTime.UtcNow > RefreshTime || BoltEndPoints is null)
                RefreshBoltEndpoints("");

            var preferredOrder = HttpReadMethodTypes.Contains(httpRequestMethod) ? ReadOrderEndpoints : WriteOrderEndpoints;
            endpointCacheLock.EnterReadLock();
            try
            {
                foreach (var endPointsKey in preferredOrder)
                    if (BoltEndPoints.ContainsKey(endPointsKey) && BoltEndPoints[endPointsKey].Count > 0)
                    {
                        var selectedEndpoint = BoltEndPoints[endPointsKey][Rand(0, BoltEndPoints[endPointsKey].Count)];
                        return new Uri($"https://{selectedEndpoint}");
                    }
            }
            finally
            {
                endpointCacheLock.ExitReadLock();
            }
            throw new Exception($"No bolt api endpoints are available. Region: {Region}, ZoneId: {ZoneId}, UrlToFetchLatestBoltEndPoints: {QuicksilverUrl}");
        }
        private static readonly TimeSpan defaultTimeout = TimeSpan.FromSeconds(100);
        private static readonly TimeSpan defaultReadWriteTimeout = TimeSpan.FromSeconds(300);
        private static readonly TimeSpan maxTimeout = TimeSpan.FromSeconds(300);

        private static readonly AmazonS3Config BoltS3Config = new AmazonS3Config
        {
            ForcePathStyle = true,
            // Explicitly set default timeout values
            Timeout = defaultTimeout,
            // NOTE: The following property is obsolete for
            //       versions of the AWS SDK for .NET that target .NET Core.
            ReadWriteTimeout = defaultReadWriteTimeout,
        };

        private static void UseBoltConfiguration()
        {
            Region = BoltConfiguration.Region ?? throw new InvalidOperationException("AWS_REGION is not defined through BoltConfiguration or in evironment. And also Region info not available in EC2InstanceMetadata.");

            ZoneId = BoltConfiguration.ZoneId ?? throw new InvalidOperationException("AWS_ZONE_ID not defined through BoltConfiguration or in evironment. And also AvailabilityZoneId info not available in EC2InstanceMetadata.");

            CustomDomain = BoltConfiguration.CustomDomain ?? throw new InvalidOperationException("BOLT_CUSTOM_DOMAIN not defined through BoltConfiguration or in evironment.");

            AuthBucket = BoltConfiguration.AuthBucket; // allow this to be unset - will use source bucket resolution if not provided

            BoltHostname = $"bolt.{Region}.{CustomDomain}";
            QuicksilverUrl = $"https://quicksilver.{Region}.{CustomDomain}/services/bolt?az={ZoneId}";
        }

        /// <summary>
        /// Constructs AmazonS3Client with the credentials loaded from the application's
        /// default configuration, and if unsuccessful from the Instance Profile service on an EC2 instance.
        ///
        /// Example App.config with credentials set.
        /// <code>
        /// &lt;?xml version="1.0" encoding="utf-8" ?&gt;
        /// &lt;configuration&gt;
        ///     &lt;appSettings&gt;
        ///         &lt;add key="AWSProfileName" value="AWS Default"/&gt;
        ///     &lt;/appSettings&gt;
        /// &lt;/configuration&gt;
        /// </code>
        ///
        /// </summary>
        public BoltS3Client() : base(BoltS3Config)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with the credentials loaded from the application's
        /// default configuration, and if unsuccessful from the Instance Profile service on an EC2 instance.
        ///
        /// Example App.config with credentials set.
        /// <code>
        /// &lt;?xml version="1.0" encoding="utf-8" ?&gt;
        /// &lt;configuration&gt;
        ///     &lt;appSettings&gt;
        ///         &lt;add key="AWSProfileName" value="AWS Default"/&gt;
        ///     &lt;/appSettings&gt;
        /// &lt;/configuration&gt;
        /// </code>
        ///
        /// </summary>
        /// <param name="region">The region to connect.</param>
        public BoltS3Client(RegionEndpoint region) : base(BoltS3Config)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with the credentials loaded from the application's
        /// default configuration, and if unsuccessful from the Instance Profile service on an EC2 instance.
        ///
        /// Example App.config with credentials set.
        /// <code>
        /// &lt;?xml version="1.0" encoding="utf-8" ?&gt;
        /// &lt;configuration&gt;
        ///     &lt;appSettings&gt;
        ///         &lt;add key="AWSProfileName" value="AWS Default"/&gt;
        ///     &lt;/appSettings&gt;
        /// &lt;/configuration&gt;
        /// </code>
        ///
        /// </summary>
        /// <param name="clientConfig">The AmazonS3Client Configuration Object</param>
        public BoltS3Client(AmazonS3Config clientConfig) : base(clientConfig)
        {
            clientConfig.ForcePathStyle = true;
            // set timeouts to defaults if not set, or ensure they're not greater than maxTimeout
            if (clientConfig.Timeout == null)
            {
                clientConfig.Timeout = defaultTimeout;
            }
            else if (clientConfig.Timeout > maxTimeout)
            {
                clientConfig.Timeout = maxTimeout;
            }
            if (clientConfig.ReadWriteTimeout == null)
            {
                clientConfig.ReadWriteTimeout = defaultReadWriteTimeout;
            }
            else if (clientConfig.ReadWriteTimeout > maxTimeout)
            {
                clientConfig.ReadWriteTimeout = maxTimeout;
            }
        }

        /// <summary>Constructs AmazonS3Client with AWS Credentials</summary>
        /// <param name="credentials">AWS Credentials</param>
        public BoltS3Client(AWSCredentials credentials) : base(credentials, BoltS3Config)
        {
        }

        /// <summary>Constructs AmazonS3Client with AWS Credentials</summary>
        /// <param name="credentials">AWS Credentials</param>
        /// <param name="region">The region to connect.</param>
        public BoltS3Client(AWSCredentials credentials, RegionEndpoint region) : base(credentials, BoltS3Config)
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
            clientConfig.ForcePathStyle = true;
            if (clientConfig.Timeout == null)
            {
                clientConfig.Timeout = defaultTimeout;
            }
            else if (clientConfig.Timeout > maxTimeout)
            {
                clientConfig.Timeout = maxTimeout;
            }
            if (clientConfig.ReadWriteTimeout == null)
            {
                clientConfig.ReadWriteTimeout = defaultReadWriteTimeout;
            }
            else if (clientConfig.ReadWriteTimeout > maxTimeout)
            {
                clientConfig.ReadWriteTimeout = maxTimeout;
            }
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID and AWS Secret Key
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        public BoltS3Client(string awsAccessKeyId, string awsSecretAccessKey) : base(awsAccessKeyId, awsSecretAccessKey,
            BoltS3Config)
        {
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID and AWS Secret Key
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        /// <param name="region">The region to connect.</param>
        public BoltS3Client(string awsAccessKeyId, string awsSecretAccessKey, RegionEndpoint region) : base(
            awsAccessKeyId, awsSecretAccessKey, BoltS3Config)
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
            clientConfig.ForcePathStyle = true;
            if (clientConfig.Timeout == null)
            {
                clientConfig.Timeout = defaultTimeout;
            }
            else if (clientConfig.Timeout > maxTimeout)
            {
                clientConfig.Timeout = maxTimeout;
            }
            if (clientConfig.ReadWriteTimeout == null)
            {
                clientConfig.ReadWriteTimeout = defaultReadWriteTimeout;
            }
            else if (clientConfig.ReadWriteTimeout > maxTimeout)
            {
                clientConfig.ReadWriteTimeout = maxTimeout;
            }
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID and AWS Secret Key
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        /// <param name="awsSessionToken">AWS Session Token</param>
        public BoltS3Client(string awsAccessKeyId, string awsSecretAccessKey, string awsSessionToken) : base(
            awsAccessKeyId, awsSecretAccessKey, awsSessionToken, BoltS3Config)
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
            RegionEndpoint region) : base(awsAccessKeyId, awsSecretAccessKey, awsSessionToken, BoltS3Config)
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
            clientConfig.ForcePathStyle = true;
            if (clientConfig.Timeout == null)
            {
                clientConfig.Timeout = defaultTimeout;
            }
            else if (clientConfig.Timeout > maxTimeout)
            {
                clientConfig.Timeout = maxTimeout;
            }
            if (clientConfig.ReadWriteTimeout == null)
            {
                clientConfig.ReadWriteTimeout = defaultReadWriteTimeout;
            }
            else if (clientConfig.ReadWriteTimeout > maxTimeout)
            {
                clientConfig.ReadWriteTimeout = maxTimeout;
            }
        }

        /// <summary>Creates the signer for the service.</summary>
        protected override AbstractAWSSigner CreateSigner()
        {
            return new BoltSigner();
        }
        /// <summary>
        /// Adds custom retry handler to client to refresh bolt endpoints on error
        /// </summary>
        /// <param name="pipeline"></param>
        protected override void CustomizeRuntimePipeline(RuntimePipeline pipeline)
        {
            base.CustomizeRuntimePipeline(pipeline);
            RetryPolicy retryPolicy;
            switch (Config.RetryMode)
            {
                case RequestRetryMode.Legacy:
                    retryPolicy = new BoltRetryPolicy(Config);
                    break;
                case RequestRetryMode.Adaptive:
                    retryPolicy = new BoltAdaptiveRetryPolicy(Config);
                    break;
                case RequestRetryMode.Standard:
                default:
                    retryPolicy = new BoltStandardRetryPolicy(Config);
                    break;
            }
            pipeline.ReplaceHandler<RetryHandler>(new RetryHandler(retryPolicy));
        }
    }
}
