using Ktm.Binding.Infrastructure;
using Ktm.HostedServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Ktm.DataSaver.Tests.Host;

public sealed class CorePostgresConfigurationTests
{
    private const string Section = "Ktm:Binding:Postgres";

    [Fact]
    public void ValidFileSecretIsUsedByScopedContext()
    {
        string path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, "secret-from-file\n");
            var settings = ValidSettings();
            settings[$"{Section}:Password"] = null;
            settings[$"{Section}:Password_FILE"] = path;
            using var provider = Services(settings);
            provider.GetRequiredService<IStartupValidator>().Validate();
            using var scope = provider.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<BindingDbContext>();
            Assert.Contains("secret-from-file", context.Database.GetConnectionString());
            Assert.Same(context, scope.ServiceProvider.GetRequiredService<BindingDbContext>());
        }
        finally { File.Delete(path); }
    }

    [Theory]
    [InlineData(null, "ktm_app")]
    [InlineData("explicit_user", "explicit_user")]
    public void DatabaseUsernameDefaultsToAppAndAllowsExplicitOverride(string? configuredUsername, string expectedUsername)
    {
        var settings = ValidSettings();
        if (configuredUsername is null)
        {
            settings.Remove($"{Section}:Username");
        }
        else
        {
            settings[$"{Section}:Username"] = configuredUsername;
        }

        using var provider = Services(settings);
        provider.GetRequiredService<IStartupValidator>().Validate();
        using var scope = provider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<BindingDbContext>();
        var connection = new NpgsqlConnectionStringBuilder(context.Database.GetConnectionString());
        Assert.Equal(expectedUsername, connection.Username);
    }

    [Fact]
    public void BothSecretSourcesFailValidationWithoutLeakingValue()
    {
        var settings = ValidSettings();
        settings[$"{Section}:Password_FILE"] = "unused-file";
        using var provider = Services(settings);
        var error = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IStartupValidator>().Validate());
        Assert.DoesNotContain("secret-value", error.Message);
        Assert.Contains(Section, error.Message);
    }

    [Fact]
    public void UnreadableFileFailsValidation()
    {
        var settings = ValidSettings();
        settings[$"{Section}:Password"] = null;
        settings[$"{Section}:Password_FILE"] = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        using var provider = Services(settings);
        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IStartupValidator>().Validate());
    }

    private static Dictionary<string, string?> ValidSettings() => new()
    {
        [$"{Section}:Host"] = "localhost", [$"{Section}:Database"] = "ktm",
        [$"{Section}:Username"] = "binding", [$"{Section}:Password"] = "secret-value"
    };

    private static ServiceProvider Services(Dictionary<string, string?> settings)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddModuleDatabase<BindingDbContext>(configuration, Section);
        return services.BuildServiceProvider();
    }
}
