using System;
using System.IO;
using System.Net;
using System.Configuration;

using System.Linq;
using System.Collections.Generic;

using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Internal.Auth;
using Amazon.S3;
using Amazon.Util;

using Newtonsoft.Json;

namespace ProjectN.Bolt
{
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
        private static string Region = Environment.GetEnvironmentVariable("AWS_REGION")
                   ?? EC2InstanceMetadata.Region?.SystemName
                   ?? throw new InvalidOperationException("Region not available in EC2InstanceMetadata, and also not defined in environment.");

        private static string AvailabilityZoneId = Environment.GetEnvironmentVariable("AWS_ZONE_ID")
                    ?? EC2InstanceMetadata.GetData("/placement/availability-zone-id")
                    ?? throw new InvalidOperationException("AvailabilityZoneId not available in EC2InstanceMetadata, and also not defined in environment.");

        public static string BoltHostname = (ConfigurationManager.AppSettings["BOLT_HOSTNAME"] ?? Environment.GetEnvironmentVariable("BOLT_HOSTNAME"))
                    ?.Replace("{region}", Region)
                    ?? throw new InvalidOperationException("BOLT_HOSTNAME not defined in app config or evironment.");

        private static string UrlToFetchLatestBoltEndPoints = new Func<string>(() =>
        {
            var baseServiceUrl = (ConfigurationManager.AppSettings["SERVICE_URL"] ?? Environment.GetEnvironmentVariable("SERVICE_URL"))
                    ?.Replace("{region}", Region)
                    ?? throw new InvalidOperationException("SERVICE_URL not defined in app config or evironment.");

            return $"{baseServiceUrl}/services/bolt?az={AvailabilityZoneId}";
        })();

        private static Dictionary<string, List<string>> GetBoltEndPoints()
        {
            var ServiceURLRequest = WebRequest.Create(UrlToFetchLatestBoltEndPoints);
            var httpResponse = ServiceURLRequest.GetResponse();
            using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
            {
                var responseString = streamReader.ReadToEnd();
                return JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(responseString);
            }
        }

        private static List<string> ReadOrderEndpoints = new List<string> { "main_read_endpoints", "main_write_endpoints", "failover_read_endpoints", "failover_write_endpoints" };
        private static List<string> WriteOrderEndpoints = new List<string> { "main_write_endpoints", "failover_write_endpoints" };
        private static List<string> HttpReadMethodTypes = new List<string> { "GET", "HEAD" };

        private static DateTime LastRefreshedTimeinUtc = DateTime.UtcNow;
        private static Dictionary<string, List<string>> BoltEndPoints = null; // TODO: Move static class level to instance level so as to make thread safe

        private static int RefreshTime = new Random().Next(60, 180);

        public static string SelectBoltEndPoint(string httpRequestMethod)
        {
            if ((DateTime.UtcNow - LastRefreshedTimeinUtc).TotalSeconds > RefreshTime || BoltEndPoints is null)
            {
                BoltEndPoints = GetBoltEndPoints();
                LastRefreshedTimeinUtc = DateTime.UtcNow;
            }

            var preferredOrder = HttpReadMethodTypes.Contains(httpRequestMethod) ? ReadOrderEndpoints : WriteOrderEndpoints;
            var random = new Random();
            foreach (var endPointsKey in preferredOrder)
            {
                if (BoltEndPoints.ContainsKey(endPointsKey) && BoltEndPoints[endPointsKey].Count > 0)
                {
                    var methodRelatedEndPoints = BoltEndPoints[endPointsKey].Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();
                    var randomIndex = random.Next(methodRelatedEndPoints.Count);
                    return methodRelatedEndPoints[randomIndex];
                }
            }

            throw new Exception($"No bolt api endpoints are available. Region: {Region}, AvailabilityZoneId: {AvailabilityZoneId}, UrlToFetchLatestBoltEndPoints: {UrlToFetchLatestBoltEndPoints}");


        }

        private static readonly AmazonS3Config BoltConfig = new AmazonS3Config
        {
            // The UrlToFetchLatestBoltEndPoints will be replaced with dynamic bolt api endpoint based on the http method type of S3 operation's request. You can check the related code in BoltSigner.cs
            ServiceURL = UrlToFetchLatestBoltEndPoints,
            ForcePathStyle = true,
        };

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
        public BoltS3Client() : base(BoltConfig)
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
        public BoltS3Client(RegionEndpoint region) : base(BoltConfig)
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
        /// <param name="config">The AmazonS3Client Configuration Object</param>
        public BoltS3Client(AmazonS3Config config) : base(config)
        {
            config.ServiceURL = UrlToFetchLatestBoltEndPoints;
            config.ForcePathStyle = true;
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
            clientConfig.ServiceURL = UrlToFetchLatestBoltEndPoints;
            clientConfig.ForcePathStyle = true;
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
            clientConfig.ServiceURL = UrlToFetchLatestBoltEndPoints;
            clientConfig.ForcePathStyle = true;
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
            clientConfig.ForcePathStyle = true;
            clientConfig.ServiceURL = UrlToFetchLatestBoltEndPoints;
        }

        /// <summary>Creates the signer for the service.</summary>
        protected override AbstractAWSSigner CreateSigner()
        {
            return new BoltSigner();
        }
    }
}
