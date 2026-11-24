using Ktm.DataSaver.Configuration;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Rules;
using Ktm.DataSaver.Health;
using Ktm.DataSaver.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace Ktm.DataSaver.Tests.Host;

/// <summary>Настройки <c>DataSaver__Rules__*</c>, аварийное отключение и готовность по первой загрузке правил (ADR-0005 п. 2.10).</summary>
public class RulesHostTests
{
    private static Dictionary<string, string?> Valid() => new()
    {
        ["DataSaver:Kafka:BootstrapServers"] = "kafka:9092",
        ["DataSaver:Postgres:Host"] = "db",
        ["DataSaver:Postgres:Database"] = "ktm",
        ["DataSaver:Postgres:Username"] = "ktm_app",
        ["DataSaver:Postgres:Password"] = "pw",
    };

    private static ServiceProvider Build(Dictionary<string, string?> values)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDataSaver(configuration);
        return services.BuildServiceProvider();
    }

    /// <summary>Полный host (без запуска): в нём есть метрики, время жизни приложения и прочие службы, нужные worker-у.</summary>
    private static IHost BuildHost(Dictionary<string, string?> values)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(values);
        builder.Services.AddDataSaver(builder.Configuration);
        return builder.Build();
    }

    private static string[] StartupFailures(Dictionary<string, string?> values)
    {
        using var provider = Build(values);
        var ex = Assert.ThrowsAny<Exception>(() => provider.GetRequiredService<IStartupValidator>().Validate());
        var failures = ex is AggregateException aggregate ? aggregate.InnerExceptions : [ex];
        return failures.Select(f => f.Message).ToArray();
    }

    [Theory]
    [InlineData("RefreshIntervalSeconds", "4")]
    [InlineData("RefreshIntervalSeconds", "3601")]
    [InlineData("RefreshIntervalSeconds", "0")]
    [InlineData("MaxRules", "0")]
    [InlineData("RegexTimeoutMs", "0")]
    [InlineData("RegexTimeoutMs", "1001")]
    [InlineData("MaxRegexNodes", "0")]
    [InlineData("MaxRegexNodes", "1001")]
    public void RulesSettingOutOfRange_FailsStartupNamingTheSetting(string setting, string value)
    {
        var values = Valid();
        values[$"DataSaver:Rules:{setting}"] = value;

        var failures = StartupFailures(values);

        Assert.Contains(failures, f => f.Contains($"DataSaver__Rules__{setting}", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("5")]
    [InlineData("60")]
    [InlineData("3600")]
    public void RefreshIntervalWithinBounds_PassesStartup(string seconds)
    {
        var values = Valid();
        values["DataSaver:Rules:RefreshIntervalSeconds"] = seconds;
        using var provider = Build(values);

        provider.GetRequiredService<IStartupValidator>().Validate();

        Assert.Equal(int.Parse(seconds), provider.GetRequiredService<IOptions<RulesOptions>>().Value.RefreshIntervalSeconds);
    }

    [Theory]
    [InlineData("RegexTimeoutMs", "1")]
    [InlineData("RegexTimeoutMs", "1000")]
    [InlineData("MaxRegexNodes", "1")]
    [InlineData("MaxRegexNodes", "1000")]
    public void RegexSettingsWithinBounds_PassStartup_AndReachThePolicyAndTheBuilder(string setting, string value)
    {
        var values = Valid();
        values[$"DataSaver:Rules:{setting}"] = value;
        using var provider = Build(values);

        provider.GetRequiredService<IStartupValidator>().Validate();

        var options = provider.GetRequiredService<IOptions<RulesOptions>>().Value;
        Assert.Equal(setting == "RegexTimeoutMs" ? int.Parse(value) : 200, (int)provider.GetRequiredService<RegexPolicy>().MatchTimeout.TotalMilliseconds);
        Assert.Equal(setting == "MaxRegexNodes" ? int.Parse(value) : 64, options.MaxRegexNodes);
    }

    [Fact]
    public void RegexSettings_DefaultTo200MsAnd64Nodes()
    {
        using var provider = Build(Valid());

        provider.GetRequiredService<IStartupValidator>().Validate();

        var options = provider.GetRequiredService<IOptions<RulesOptions>>().Value;
        Assert.Equal((200, 64), (options.RegexTimeoutMs, options.MaxRegexNodes));
        Assert.Equal(TimeSpan.FromMilliseconds(200), provider.GetRequiredService<RegexPolicy>().MatchTimeout);
    }

    [Fact]
    public void RulesDisabled_RegexSettingsAreNotValidated_AndThePolicyFallsBackToDefaults()
    {
        var values = Valid();
        values["DataSaver:Rules:Enabled"] = "false";
        values["DataSaver:Rules:RegexTimeoutMs"] = "0";
        values["DataSaver:Rules:MaxRegexNodes"] = "5000";
        using var provider = Build(values);

        provider.GetRequiredService<IStartupValidator>().Validate();

        Assert.Equal(RegexPolicy.Default, provider.GetRequiredService<RegexPolicy>());
    }

    [Fact]
    public void RefreshIntervalNotConfigured_DefaultsTo60Seconds_AndPassesStartup()
    {
        var values = Valid();
        Assert.DoesNotContain("DataSaver:Rules:RefreshIntervalSeconds", values.Keys);
        using var provider = Build(values);

        provider.GetRequiredService<IStartupValidator>().Validate();

        Assert.Equal(60, provider.GetRequiredService<IOptions<RulesOptions>>().Value.RefreshIntervalSeconds);
    }

    [Fact]
    public void RulesEnabledByDefault_StageFollowsC5_AndLoaderStartsBeforeIngestion()
    {
        using var provider = BuildHost(Valid());

        var stages = provider.Services.GetServices<IIngestionStage>().Select(s => s.GetType()).ToList();
        var hosted = provider.Services.GetServices<IHostedService>().Select(s => s.GetType()).ToList();

        Assert.Equal([typeof(EventValidationStage), typeof(RuleRejectionStage)], stages);
        Assert.True(hosted.IndexOf(typeof(RuleRefreshService)) is var loader and >= 0 && loader < hosted.IndexOf(typeof(DataSaverWorker)));
    }

    [Fact]
    public void RulesDisabled_NoStageNoLoaderNoSnapshot_SoTheTableIsNeverRead()
    {
        var values = Valid();
        values["DataSaver:Rules:Enabled"] = "false";
        using var host = BuildHost(values);
        var provider = host.Services;

        provider.GetRequiredService<IStartupValidator>().Validate();

        Assert.Equal([typeof(EventValidationStage)], provider.GetServices<IIngestionStage>().Select(s => s.GetType()));
        Assert.DoesNotContain(typeof(RuleRefreshService), provider.GetServices<IHostedService>().Select(s => s.GetType()));
        Assert.Null(provider.GetService<RuleSnapshotHolder>());
        Assert.Null(provider.GetService<IRuleSource>());
    }

    private sealed class Signals(DateTimeOffset heartbeat) : IHealthSignals
    {
        public DateTimeOffset LastHeartbeat { get; } = heartbeat;
        public IngestionState State => IngestionState.Running;
        public bool IsConsumerFatal => false;
        public bool HasAssignment => true;

        public ValueTask<bool> IsBrokerAvailableAsync(CancellationToken cancellationToken) => ValueTask.FromResult(true);

        public ValueTask<bool> IsDatabaseAvailableAsync(CancellationToken cancellationToken) => ValueTask.FromResult(true);
    }

    [Fact]
    public async Task Readiness_RequiresFirstRuleLoad_EmptyTableCounts_AndLivenessSurvivesTheWait()
    {
        var time = new FakeTimeProvider();
        var holder = new RuleSnapshotHolder();
        var health = new HealthEvaluator(new Signals(time.GetUtcNow()), Options.Create(new HealthOptions { LiveStaleSeconds = 60 }), time, holder);

        Assert.Equal("rules_not_loaded", (await health.ReadyAsync(default)).Reason);
        // Цикл приёма ждёт загрузку и не отмечается: это не зависание, перезапуск не нужен.
        time.Advance(TimeSpan.FromMinutes(10));
        Assert.True(health.Live().IsHealthy);

        // Пустая таблица - успешная загрузка.
        holder.Replace(new RuleSnapshotBuilder(new ConditionCompiler()).Build("empty", [], []), time.GetUtcNow());

        Assert.True((await health.ReadyAsync(default)).IsHealthy);
        Assert.Equal("loop_stalled", health.Live().Reason);
    }
}
