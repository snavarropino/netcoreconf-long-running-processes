using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace HostedService.HostedServices
{
    public class GracePeriodManagerService : BackgroundService
    {
        private readonly ILogger<GracePeriodManagerService> _logger;
        private int GracePeriodManagerServiceDelay { get; }
        
        public GracePeriodManagerService(ILogger<GracePeriodManagerService> logger, IConfiguration configuration)
        {
            GracePeriodManagerServiceDelay = int.Parse(configuration["GracePeriodManagerServiceDelay"]);
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("GracePeriodManagerService is starting.");

            stoppingToken.Register(() => _logger.LogInformation(" GracePeriod background task is stopping."));

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("GracePeriod task doing background work.");

                // Do stuff

                await Task.Delay(GracePeriodManagerServiceDelay, stoppingToken);
            }
        }
    }
}
