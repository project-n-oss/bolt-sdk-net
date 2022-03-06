using System;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Internal.Auth;
using Amazon.S3;
using Amazon.Util;

using System.IO;
using System.Net;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;

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
        private static string Region()
        {
            if (EC2InstanceMetadata.Region is null)
            {
                return Environment.GetEnvironmentVariable("AWS_REGION");
            }
            else
            {
                return EC2InstanceMetadata.Region.SystemName;
            };
        }

        private static string AvailabilityZone()
        {
            if (EC2InstanceMetadata.AvailabilityZone is null)
            {
                return Environment.GetEnvironmentVariable("AWS_ZONE_ID");
            }
            else
            {
                return EC2InstanceMetadata.AvailabilityZone;
            };
        }

        private static string BoltURL = Environment.GetEnvironmentVariable("BOLT_URL")?.Replace("{region}", Region());

        private static string ServiceURL = Environment.GetEnvironmentVariable("SERVICE_URL")?.Replace("{region}", Region());

        public static bool isItBasedOnDynamicBoltEndPoints = (ServiceURL ?? "").Length > 0 ? true : false;

        public static string BoltApiUrl = isItBasedOnDynamicBoltEndPoints ? ServiceURL : BoltURL; // This ServiceURL will be replaced with dynamic Bolt API Endpoint based on S3 operation request, Check the related code in BoltSigner.cs

        private static bool AcceptAllCertifications(object sender, System.Security.Cryptography.X509Certificates.X509Certificate certification, System.Security.Cryptography.X509Certificates.X509Chain chain, System.Net.Security.SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }
        private static Dictionary<string, List<string>> GetBoltEndPoints(string boltServiceListURL)
        {
            Console.WriteLine($"boltServiceListURL: {boltServiceListURL}");
            //ServicePointManager.Expect100Continue = true;
            //ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.ServerCertificateValidationCallback = new System.Net.Security.RemoteCertificateValidationCallback(AcceptAllCertifications);
            var ServiceURLRequest = WebRequest.Create(boltServiceListURL);
            Console.WriteLine($"After WebRequest.Create");
            ServiceURLRequest.Method = "GET";
            Console.WriteLine($"Before ServiceURLRequest.GetResponse");
            var httpResponse = ServiceURLRequest.GetResponse();
            Console.WriteLine($"After ServiceURLRequest.GetResponse");
            using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
            {
                var responseString = streamReader.ReadToEnd();
                Console.WriteLine($"responseString : {responseString}");
                return JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(responseString);
            }
        }

        private static DateTime lastRefreshedTime = DateTime.Now;
        private static Dictionary<string, List<string>> BoltEndPoints = null; // TODO: Move static class level to instance level so as to make thread safe
        public static string SelectBoltEndPoint(string method)
        {
            if ((DateTime.Now - lastRefreshedTime).Seconds > 120 || BoltEndPoints is null)
            {
                BoltEndPoints = GetBoltEndPoints(ServiceURL + "/services/bolt?az=" + AvailabilityZone());
                lastRefreshedTime = DateTime.Now;
            }
            string[] readOrder = { "main_read_endpoints", "main_write_endpoints", "failover_read_endpoints", "failover_write_endpoints" };
            string[] writeOrder = { "main_write_endpoints", "failover_write_endpoints" };
            string[] methodSetTypes = { "GET", "HEAD" };
            var methodSet = new HashSet<string>(methodSetTypes);
            string[] preferredOrder = methodSet.Contains(method) ? readOrder : writeOrder;
            foreach (var endPointsKey in preferredOrder)
            {
                if (BoltEndPoints.ContainsKey(endPointsKey) && BoltEndPoints[endPointsKey].Count > 0)
                {
                    var methodRelatedEndPoints = BoltEndPoints[endPointsKey].Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();
                    var random = new Random();
                    var randomIndex = random.Next(methodRelatedEndPoints.Count);
                    return methodRelatedEndPoints[randomIndex];
                }
            }
            // if we reach this point, no endpoints are available
            throw new Exception($"no endpoints are available... service_name: bolt, region_name: {Region()}");
        }
        private static readonly AmazonS3Config BoltConfig = new AmazonS3Config
        {
            ServiceURL = BoltApiUrl,
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
            config.ServiceURL = BoltApiUrl;
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
            clientConfig.ServiceURL = BoltApiUrl;
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
            clientConfig.ServiceURL = BoltApiUrl;
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
            clientConfig.ServiceURL = BoltApiUrl;
        }


        /// <summary>Creates the signer for the service.</summary>
        protected override AbstractAWSSigner CreateSigner()
        {
            return new BoltSigner();
        }
    }
}
