using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Ktm.Api.IntegrationTests;

public sealed class ApiIsolationTests
{
    [Fact]
    public void PublicApiStartsWithoutBackgroundProcessing()
    {
        using var factory = new WebApplicationFactory<Program>();
        using var client = factory.CreateClient();
        var names = factory.Services.GetServices<IHostedService>().Select(service => service.GetType().Name).ToArray();
        Assert.DoesNotContain("DataSaverWorker", names);
        Assert.DoesNotContain("BindingService", names);
        Assert.DoesNotContain("InterpretationService", names);
    }
}
