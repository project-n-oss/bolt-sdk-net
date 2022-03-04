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
  /// &lt;/configuration&gt;
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
        return EC2InstanceMetadata.AvailabilityZone.SystemName;
      };
    }

    private static readonly string BoltServiceUrl = Environment.GetEnvironmentVariable("BOLT_URL").Replace("{region}", Region());

    private static getBoltEndPoints()
    {
      try
      {
        ServiceURL = BoltServiceUrl + "/services/bolt?az=" + AvailabilityZone();
        var ServiceURLRequest = WebRequest.Create(ServiceURL);
        ServiceURLRequest.Method = "GET";
        var httpResponse = ServiceURLRequest.GetResponse();
        using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
        {
          var responseString = streamReader.ReadToEnd();
          return JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(responseString);
        }
      }
      catch (Exception e)
      {
        throw new Exception();
      }
    }

    private static selectBoltEndPoint(string method)
    {
      var boltEndPoints = getBoltEndPoints();
      string[] readOrder = { "main_read_endpoints", "main_write_endpoints", "failover_read_endpoints", "failover_write_endpoints" };
      string[] writeOrder = { "main_write_endpoints", "failover_write_endpoints" };
      string[] preferredOrder = null;
      string[] methodSetTypes = { "GET", "HEAD" };
      var methodSet = new HashSet<string>(methodSetTypes);
      if (methodSet.Contains(method))
        preferredOrder = readOrder;
      else
        preferredOrder = writeOrder;
      foreach (var endPoint in preferredOrder)
      {
        if (boltEndPoints[endPoint].Count > 0)
        {
          var random = new Random();
          var randomIndex = random.Next(boltEndPoints[endPoint].Count);
          return boltEndPoints[endPoint][randomIndex];
        }
      }
      // if we reach this point, no endpoints are available
      throw new Exception($"no endpoints are available... service_name: bolt, region_name: ${self._get_region()}");
    }
    private static readonly AmazonS3Config BoltConfig = new AmazonS3Config
    {
      ServiceURL = BoltServiceUrl,
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
      config.ServiceURL = BoltServiceUrl;
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
      clientConfig.ServiceURL = BoltServiceUrl;
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
      clientConfig.ServiceURL = BoltServiceUrl;
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
      clientConfig.ServiceURL = BoltServiceUrl;
    }

    /// <summary>Creates the signer for the service.</summary>
    protected override AbstractAWSSigner CreateSigner()
    {
      return new BoltSigner();
    }
  }
}
