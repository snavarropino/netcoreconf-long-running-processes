using System;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.WebJobs;

namespace ContinuousWebJob
{
    public class Functions
    {
        [return: Queue("output-queue")]
        public static string ProcessQueueMessage([QueueTrigger("input-queue")] string message, ILogger logger)
        {
            logger.LogInformation($"Hi {message}");
            //Stuff
            
            //throw new Exception("error"); //Uncomment to force errors
            
            return $"processed {message}";
        }
    }
}