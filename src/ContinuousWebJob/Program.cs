using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ContinuousWebJob
{
    static class Program
    {
        public static async Task Main()
        {
            var host = CreateHostBuilder().Build();
            await host.RunAsync();
        }

        private static IHostBuilder CreateHostBuilder()
        {
            var builder = new HostBuilder();
            builder.ConfigureLogging((context, b) => { b.AddConsole(); });
            builder.ConfigureWebJobs(b =>
            {
                b.AddAzureStorageCoreServices();
                b.AddAzureStorage(options =>
                {
                    options.MaxDequeueCount = 2;
                    options.VisibilityTimeout  = TimeSpan.FromSeconds(10);
                });
            });

            return builder;
        }
    }
}
