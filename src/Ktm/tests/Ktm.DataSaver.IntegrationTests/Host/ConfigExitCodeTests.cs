using System.Diagnostics;

namespace Ktm.DataSaver.IntegrationTests.Host;

public sealed class ConfigExitCodeTests
{
    private const string Secret = "top-secret-pw";
    private const string DbSecret = "db-secret-pw";

    private static Dictionary<string, string> ValidBase() => new()
    {
        ["DataSaver__Kafka__BootstrapServers"] = "127.0.0.1:1",
        ["DataSaver__Postgres__Host"] = "127.0.0.1",
        ["DataSaver__Postgres__Database"] = "ktm",
        ["DataSaver__Postgres__Username"] = "ktm_app",
        ["DataSaver__Postgres__Password"] = DbSecret,
    };

    /// <summary>Запуск службы отдельным процессом: из окружения убираются все DataSaver__*, нужные задаются явно.</summary>
    internal static async Task<(int ExitCode, string Stderr, string Stdout)> RunAsync(Dictionary<string, string> environment)
    {
        var info = new ProcessStartInfo("dotnet")
        {
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        info.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "Ktm.DataSaver.dll"));
        foreach (var key in info.Environment.Keys.Where(k => k.StartsWith("DataSaver__", StringComparison.OrdinalIgnoreCase)).ToList())
        {
            info.Environment.Remove(key);
        }

        foreach (var (key, value) in environment)
        {
            info.Environment[key] = value;
        }

        info.Environment["ASPNETCORE_URLS"] = "http://127.0.0.1:0";

        using var process = Process.Start(info)!;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        try
        {
            // Потоки читаются параллельно: иначе заполненный буфер одного блокирует процесс.
            var stderr = process.StandardError.ReadToEndAsync(timeout.Token);
            var stdout = process.StandardOutput.ReadToEndAsync(timeout.Token);
            await Task.WhenAll(stderr, stdout);
            await process.WaitForExitAsync(timeout.Token);
            return (process.ExitCode, stderr.Result, stdout.Result);
        }
        finally
        {
            if (!process.HasExited)
            {
                // Таймаут: процесс службы не должен остаться висеть после теста.
                process.Kill(entireProcessTree: true);
            }
        }
    }

    private static void AssertOneLineWithoutStackOrSecrets((int ExitCode, string Stderr, string Stdout) result, params string[] secrets)
    {
        Assert.Equal(78, result.ExitCode);
        // Пароль БД задан в каждом сценарии и не должен попасть в вывод ни в одном из них.
        secrets = [.. secrets, DbSecret];
        Assert.Single(result.Stderr.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
        Assert.DoesNotContain("   at ", result.Stderr + result.Stdout, StringComparison.Ordinal);
        foreach (var secret in secrets)
        {
            Assert.DoesNotContain(secret, result.Stderr + result.Stdout, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task InvalidConfiguration_ExitsWith78_AndOneLineWithSettingName_NoStackTraceNoSecrets()
    {
        var environment = ValidBase();
        environment["DataSaver__Batch__MaxSize"] = "0";
        environment["DataSaver__Kafka__SaslPassword"] = Secret;

        var result = await RunAsync(environment);

        // Хост сообщает о первой невалидной секции; важно, что в строке есть имя настройки.
        Assert.Contains("DataSaver__", result.Stderr, StringComparison.Ordinal);
        AssertOneLineWithoutStackOrSecrets(result, Secret);
    }

    [Fact]
    public async Task SaslPasswordAndFileTogether_ExitsWith78_OneLineNamingBothSettings()
    {
        var file = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(file, "pw-from-file");
            var environment = ValidBase();
            environment["DataSaver__Kafka__SaslPassword"] = Secret;
            environment["DataSaver__Kafka__SaslPasswordFile"] = file;

            var result = await RunAsync(environment);

            Assert.Contains("DataSaver__Kafka__SaslPassword", result.Stderr, StringComparison.Ordinal);
            Assert.Contains("DataSaver__Kafka__SaslPasswordFile", result.Stderr, StringComparison.Ordinal);
            AssertOneLineWithoutStackOrSecrets(result, Secret, "pw-from-file");
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task UnreadableSaslPasswordFile_ExitsWith78_OneLineNamingSettingAndPath()
    {
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".missing");
        var environment = ValidBase();
        environment["DataSaver__Kafka__SaslPasswordFile"] = path;

        var result = await RunAsync(environment);

        Assert.Contains("DataSaver__Kafka__SaslPasswordFile", result.Stderr, StringComparison.Ordinal);
        Assert.Contains(path, result.Stderr, StringComparison.Ordinal);
        AssertOneLineWithoutStackOrSecrets(result);
    }

    [Fact]
    public async Task PollIntervalNotBelowLiveStale_ExitsWith78_OneLineNamingBothSettings()
    {
        var environment = ValidBase();
        environment["DataSaver__PollIntervalMs"] = "90000";

        var result = await RunAsync(environment);

        Assert.Contains("DataSaver__PollIntervalMs", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("DataSaver__Health__LiveStaleSeconds", result.Stderr, StringComparison.Ordinal);
        // Проверяется только подставленный секрет: пароль БД из ValidBase в сообщение не входит по построению.
        AssertOneLineWithoutStackOrSecrets(result);
    }

    [Theory]
    [InlineData("DataSaver__Batch__MaxSize")]
    [InlineData("DataSaver__Health__Port")]
    [InlineData("DataSaver__Trust__Default")]
    public async Task NonNumericSetting_ExitsWith78_OneLineNamingSettingWithoutItsValue(string setting)
    {
        const string badValue = "abc";
        var environment = ValidBase();
        environment[setting] = badValue;

        var result = await RunAsync(environment);

        Assert.Contains(setting, result.Stderr, StringComparison.Ordinal);
        AssertOneLineWithoutStackOrSecrets(result, badValue);
    }
}
