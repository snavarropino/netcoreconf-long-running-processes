using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace DurableFunction
{
    public static class Function1
    {
        [FunctionName("QueueTrigger")]
        public static async Task QueueTrigger([QueueTrigger("input-durable")] string myQueueItem, 
                                              [DurableClient] IDurableOrchestrationClient starter, ILogger log)
        {
            await starter.StartNewAsync("ParallelOrchestrator", null, myQueueItem);
        }

        [FunctionName("ParallelOrchestrator")]
        public static async Task<List<string>> RunOrchestrator([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            var rounds = int.Parse(context.GetInput<string>());
            var parallelTasks = new List<Task<string>>();

            for (int round = 1; round <= rounds; round++)
            {
                var task = context.CallActivityAsync<string>("CalculatePiNumber", round);
                parallelTasks.Add(task);
            }

            await Task.WhenAll(parallelTasks);

            // Aggregate all N outputs
            var results = parallelTasks.Select(t => t.Result).ToList();
            log.LogInformation("Results:");
            results.ForEach(i => log.LogInformation(i.ToString()));

            return results;
        }

        [FunctionName("CalculatePiNumber")]
        public static string CalculatePiNumber([ActivityTrigger] int rounds, ILogger log)
        {
            log.LogInformation($"Calculating in {rounds} rounds");
            var piNumber = "3,";
            var dividedBy = 11080585;
            var divisor = 78256779;

            for (int i = 0; i < rounds; i++)
            {
                if (dividedBy < divisor)
                    dividedBy *= 10;

                var result = dividedBy / divisor;

                var resultString = result.ToString();
                piNumber += resultString;

                dividedBy -= divisor * result;
            }

            return piNumber;
        }
    }
}