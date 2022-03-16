using System;
using Amazon;
using Amazon.SecurityToken.Model;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.Runtime.Internal.Auth;
using Amazon.Runtime.Internal.Util;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model.Internal.MarshallTransformations;
using System.Collections.Generic;

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

        private static readonly int roundToSeconds = 600;
        private static TimeSpan roundTo = TimeSpan.FromSeconds(roundToSeconds);
        private static TimeSpan offset = TimeSpan.FromSeconds(new Random().Next(0, roundToSeconds));

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
            request.Endpoint = BoltS3Client.SelectBoltEndPoint(request.HttpMethod);

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

        /// <summary>
        /// Calculates and signs the specified request using the AWS4 signing protocol by using the
        /// AWS account credentials given in the method parameters.
        /// </summary>
        /// <param name="request">
        /// The request to compute the signature for. Additional headers mandated by the AWS4 protocol 
        /// ('host' and 'x-amz-date') will be added to the request before signing.
        /// </param>
        /// <param name="clientConfig">
        /// Client configuration data encompassing the service call (notably authentication
        /// region, endpoint and service name).
        /// </param>
        /// <param name="metrics">
        /// Metrics for the request.
        /// </param>
        /// <param name="awsAccessKeyId">
        /// The AWS public key for the account making the service call.
        /// </param>
        /// <param name="awsSecretAccessKey">
        /// The AWS secret key for the account making the call, in clear text.
        /// </param>
        /// <exception cref="Amazon.Runtime.SignatureException">
        /// If any problems are encountered while signing the request.
        /// </exception>
        /// <remarks>
        /// Parameters passed as part of the resource path should be uri-encoded prior to
        /// entry to the signer. Parameters passed in the request.Parameters collection should
        /// be not be encoded; encoding will be done for these parameters as part of the 
        /// construction of the canonical request.
        /// </remarks>
        public new AWS4SigningResult SignRequest(IRequest request,
                                             IClientConfig clientConfig,
                                             RequestMetrics metrics,
                                             string awsAccessKeyId,
                                             string awsSecretAccessKey)
        {
            var signedAt = InitializeRoundedHeaders(request.Headers, request.Endpoint);
            var service = "sts"; // we always sign requests for sts, and the equivalent code from the base class uses an internal method to determine the service

            // the rest of the code should be identical to the implementation in the base class
            var region = DetermineSigningRegion(clientConfig, service, request.AlternateEndpoint, request);

            var parametersToCanonicalize = GetParametersToCanonicalize(request);
            var canonicalParameters = CanonicalizeQueryParameters(parametersToCanonicalize);
            var bodyHash = SetRequestBodyHash(request, SignPayload);
            var sortedHeaders = SortAndPruneHeaders(request.Headers);

            var canonicalRequest = CanonicalizeRequest(request.Endpoint,
                                                       request.ResourcePath,
                                                       request.HttpMethod,
                                                       sortedHeaders,
                                                       canonicalParameters,
                                                       bodyHash);
            if (metrics != null)
                metrics.AddProperty(Metric.CanonicalRequest, canonicalRequest);

            return ComputeSignature(awsAccessKeyId,
                                    awsSecretAccessKey,
                                    region,
                                    signedAt,
                                    service,
                                    CanonicalizeHeaderNames(sortedHeaders),
                                    canonicalRequest,
                                    metrics);
        }


        /// <summary>
        /// Sets the AWS4 mandated 'host' and 'x-amz-date' headers, returning the date/time that will
        /// be used throughout the signing process in various elements and formats.
        ///
        /// This new version rounds the current time to the nearest 10 minutes, to take advantage of signed
        /// credential cache reuse from Bolt
        /// </summary>
        /// <param name="headers">The current set of headers</param>
        /// <param name="requestEndpoint"></param>
        /// <returns>Date and time used for x-amz-date, in UTC</returns>
        public static DateTime InitializeRoundedHeaders(IDictionary<string, string> headers, Uri requestEndpoint)
        {
            return InitializeHeaders(headers, requestEndpoint, RoundTime(CorrectClockSkew.GetCorrectedUtcNowForEndpoint(requestEndpoint.ToString())));
        }

        private static DateTime RoundTime(DateTime baseDateTime)
        {
            var offsetTime = baseDateTime - offset;
            long ticks = (offsetTime.Ticks + (roundTo.Ticks / 2) + 1) / roundTo.Ticks;
            var newTime = new DateTime(ticks * roundTo.Ticks, baseDateTime.Kind);
            newTime += offset;

            return newTime;
        }

    }
}
