using Ktm.Binding.Infrastructure;
using Ktm.HostedServices;
using Ktm.HostedServices.Admin;
using Ktm.HostedServices.Health;
using Ktm.Interpretation.Infrastructure;
using Microsoft.Extensions.Options;

try
{
    var probe = Host.CreateApplicationBuilder(args);
    bool dataSaver = probe.Configuration.GetValue<bool?>("HostedServices:DataSaver:Enabled") ?? true;
    if (dataSaver)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddDataSaver(builder.Configuration);
        AddCoreWorkers(builder.Services, builder.Configuration);
        var app = builder.Build();
        app.Services.GetRequiredService<IStartupValidator>().Validate();
        app.MapDataSaverHealth();
        app.MapAdmin();
        app.MapAdminUi();
        await app.RunAsync();
    }
    else
    {
        probe.Services.AddSingleton(TimeProvider.System);
        AddCoreWorkers(probe.Services, probe.Configuration);
        using var host = probe.Build();
        host.Services.GetService<IStartupValidator>()?.Validate();
        await host.RunAsync();
    }
    return Environment.ExitCode;
}
catch (Exception ex) when (StartupErrors.TryDescribe(ex, out var description))
{
    await Console.Error.WriteLineAsync(description);
    return StartupErrors.ConfigExitCode;
}

static void AddCoreWorkers(IServiceCollection services, IConfiguration configuration)
{
    if (configuration.GetValue<bool>("HostedServices:Binding:Enabled"))
    {
        services.AddModuleDatabase<BindingDbContext>(configuration, "Ktm:Binding:Postgres");
        services.AddHostedService<BindingService>();
    }
    if (configuration.GetValue<bool>("HostedServices:Interpretation:Enabled"))
    {
        services.AddModuleDatabase<InterpretationDbContext>(configuration, "Ktm:Interpretation:Postgres");
        services.AddHostedService<InterpretationService>();
    }
}

namespace Ktm.HostedServices
{
    public partial class Program;
}
