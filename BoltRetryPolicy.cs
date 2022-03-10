using Amazon.Runtime;
using Amazon.Runtime.Internal;
using System;
using System.Net;
using Amazon.S3.Internal;
using Amazon.Runtime.Internal.Util;
using System.Threading.Tasks;

#pragma warning disable 1998

namespace ProjectN.Bolt

{
    public partial class BoltRetryPolicy : AmazonS3RetryPolicy
    {
         /// <summary>
        /// Constructor for BoltRetryPolicy.
        /// </summary>
        /// <param name="config">The IClientConfig object</param>
        public BoltRetryPolicy(IClientConfig config) : base(config)
        {  
        }

        /// <summary>
        /// Perform the processor-bound portion of the RetryForException logic.
        /// This is shared by the sync, async, and APM versions of the RetryForException method.
        /// </summary>
        /// <param name="executionContext"></param>
        /// <param name="exception"></param>
        /// <returns>a value if it can be determined, or null if the IO-bound calculations need to be done</returns>
        public new bool? RetryForExceptionSync(IExecutionContext executionContext, Exception exception)
        {
            return SharedRetryForExceptionSync(executionContext, exception, Logger, base.RetryForException);            
        }
        internal static bool? SharedRetryForExceptionSync(IExecutionContext executionContext, Exception exception, 
            ILogger logger,
            Func<IExecutionContext, Exception, bool> baseRetryForException)
        {
            var serviceException = exception as AmazonServiceException;
            if (serviceException != null && serviceException.StatusCode >= HttpStatusCode.InternalServerError)
            {
                BoltS3Client.RefreshBoltEndpoints();
                executionContext.RequestContext.Request.Endpoint = new Uri($"https://{BoltS3Client.SelectBoltEndPoint(executionContext.RequestContext.Request.HttpMethod)}");
                return true;
            }

            return baseRetryForException(executionContext, exception);
        }

         /// <summary>
        /// Return true if the request should be retried. Implements additional checks 
        /// specific to S3 on top of the checks in DefaultRetryPolicy.
        /// </summary>
        /// <param name="executionContext">Request context containing the state of the request.</param>
        /// <param name="exception">The exception thrown by the previous request.</param>
        /// <returns>Return true if the request should be retried.</returns>
        public override async Task<bool> RetryForExceptionAsync(IExecutionContext executionContext, Exception exception)
        {
            return await SharedRetryForExceptionAsync(executionContext, exception, RetryForExceptionSync, base.RetryForException).ConfigureAwait(false);
        }
        
        internal static async Task<bool> SharedRetryForExceptionAsync(IExecutionContext executionContext, Exception exception,
            Func<IExecutionContext, Exception, bool?> retryForExceptionSync,
            Func<IExecutionContext, Exception, bool> baseRetryForException)
        {
            var syncResult = retryForExceptionSync(executionContext, exception);
            if (syncResult.HasValue)
            {
                return syncResult.Value;
            }
            else
            {
               return baseRetryForException(executionContext, exception);
            }
        }
    }


    public partial class BoltStandardRetryPolicy : StandardRetryPolicy
    {
        /// <summary>
        /// Constructor for BoltStandardRetryPolicy.
        /// </summary>
        /// <param name="config">The IClientConfig object</param>
        public BoltStandardRetryPolicy(IClientConfig config) :
            base(config)
        {
        }

        /// <summary>
        /// Perform the processor-bound portion of the RetryForException logic.
        /// This is shared by the sync, async, and APM versions of the RetryForException method.
        /// </summary>
        /// <param name="executionContext"></param>
        /// <param name="exception"></param>
        /// <returns>a value if it can be determined, or null if the IO-bound calculations need to be done</returns>
        public bool? RetryForExceptionSync(IExecutionContext executionContext, Exception exception)
        {
            return BoltRetryPolicy.SharedRetryForExceptionSync(executionContext, exception, Logger, base.RetryForException);
        }

        /// <summary>
        /// Return true if the request should be retried. Implements additional checks 
        /// specific to S3 on top of the checks in StandardRetryPolicy.
        /// </summary>
        /// <param name="executionContext">Request context containing the state of the request.</param>
        /// <param name="exception">The exception thrown by the previous request.</param>
        /// <returns>Return true if the request should be retried.</returns>
        public override async Task<bool> RetryForExceptionAsync(IExecutionContext executionContext, Exception exception)
        {
            return await BoltRetryPolicy.SharedRetryForExceptionAsync(executionContext, exception, RetryForExceptionSync, base.RetryForException).ConfigureAwait(false);            
        }
    }


    public partial class BoltAdaptiveRetryPolicy : AdaptiveRetryPolicy
    {
        /// <summary>
        /// Constructor for BoltAdaptiveRetryPolicy.
        /// </summary>
        /// <param name="config">The IClientConfig object</param>
        public BoltAdaptiveRetryPolicy(IClientConfig config) :
            base(config)
        {
        }

        /// <summary>
        /// Perform the processor-bound portion of the RetryForException logic.
        /// This is shared by the sync, async, and APM versions of the RetryForException method.
        /// </summary>
        /// <param name="executionContext"></param>
        /// <param name="exception"></param>
        /// <returns>a value if it can be determined, or null if the IO-bound calculations need to be done</returns>
        public bool? RetryForExceptionSync(IExecutionContext executionContext, Exception exception)
        {
            return BoltRetryPolicy.SharedRetryForExceptionSync(executionContext, exception, Logger, base.RetryForException);
        }


        /// <summary>
        /// Return true if the request should be retried. Implements additional checks 
        /// specific to S3 on top of the checks in AdaptiveRetryPolicy.
        /// </summary>
        /// <param name="executionContext">Request context containing the state of the request.</param>
        /// <param name="exception">The exception thrown by the previous request.</param>
        /// <returns>Return true if the request should be retried.</returns>
        public override async Task<bool> RetryForExceptionAsync(IExecutionContext executionContext, Exception exception)
        {
            return await BoltRetryPolicy.SharedRetryForExceptionAsync(executionContext, exception, RetryForExceptionSync, base.RetryForException).ConfigureAwait(false);
        }
    }
}