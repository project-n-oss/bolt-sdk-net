using System;
using Amazon;
using Amazon.SecurityToken.Model;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.Runtime.Internal.Auth;
using Amazon.Runtime.Internal.Util;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model.Internal.MarshallTransformations;

namespace ProjectN.Bolt
{
    /// <summary>
    /// AWS4 protocol signer for Bolt.
    /// Rather than directly signing S3 requests, we instead use credentials sent into the S3 request,
    /// but proxy the signature to a canonical STS GetCallerIdentity API call.
    /// </summary>
    public class BoltSigner : AWS4Signer
    {
        private static readonly Uri StsEndpoint = new Uri("https://sts.amazonaws.com/");

        private static readonly IClientConfig StsConfig = new AmazonSecurityTokenServiceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1
        };

        /// <summary>
        /// Calculates and signs the specified request using the AWS4 signing protocol by using the
        /// AWS account credentials given in the method parameters. The resulting signature is added
        /// to the request headers as 'Authorization'. Parameters supplied in the request, either in
        /// the resource path as a query string or in the Parameters collection must not have been
        /// uri encoded. If they have, use the SignRequest method to obtain a signature.
        /// </summary>
        /// <param name="request">
        /// The request to compute the signature for. Additional headers mandated by the AWS4 protocol
        /// ('host' and 'x-amz-date') will be added to the request before signing.
        /// </param>
        /// <param name="clientConfig">
        /// Client configuration data encompassing the service call (notably authentication
        /// region, endpoint and service name).
        /// </param>
        /// <param name="metrics">Metrics for the request</param>
        /// <param name="awsAccessKeyId">
        /// The AWS public key for the account making the service call.
        /// </param>
        /// <param name="awsSecretAccessKey">
        /// The AWS secret key for the account making the call, in clear text.
        /// </param>
        /// <exception cref="T:Amazon.Runtime.SignatureException">
        /// If any problems are encountered while signing the request.
        /// </exception>
        public override void Sign(IRequest request, IClientConfig clientConfig, RequestMetrics metrics,
            string awsAccessKeyId, string awsSecretAccessKey)
        {
            request.Endpoint = new Uri($"https://{BoltS3Client.SelectBoltEndPoint(request.HttpMethod)}");

            // Create the canonical STS request to get caller identity, with session token if appropriate.
            var iamRequest = GetCallerIdentityRequestMarshaller.Instance.Marshall(new GetCallerIdentityRequest());
            iamRequest.Headers["User-Agent"] = request.Headers["User-Agent"];
            iamRequest.Endpoint = StsEndpoint;
            if (request.Headers.TryGetValue("X-Amz-Security-Token", out var sessionToken))
            {
                iamRequest.Headers["X-Amz-Security-Token"] = sessionToken;
            }

            AWS4SigningResult awS4SigningResult =
                SignRequest(iamRequest, StsConfig, metrics, awsAccessKeyId,
                    awsSecretAccessKey);
            // On the receiving end, Bolt should forward these request headers to the STS GetCallerIdentity API.
            request.Headers["X-Amz-Content-SHA256"] = iamRequest.Headers["X-Amz-Content-SHA256"];
            request.Headers["X-Amz-Date"] = iamRequest.Headers["X-Amz-Date"];
            request.Headers["Authorization"] = awS4SigningResult.ForAuthorizationHeader;

            // Use bolt hostname as the Host in the request
            // SSL certs are validated based on the Host
            request.Headers["Host"] = BoltS3Client.BoltHostname;
        }
    }
}
