using System.Diagnostics;

namespace Ktm.DataSaver.IntegrationTests.Host;

public sealed class ConfigExitCodeTests
{
    [Fact]
    public async Task InvalidConfiguration_ExitsWith78_AndOneLineWithSettingName_NoStackTraceNoSecrets()
    {
        var dll = Path.Combine(AppContext.BaseDirectory, "Ktm.DataSaver.dll");
        var info = new ProcessStartInfo("dotnet")
        {
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        info.ArgumentList.Add(dll);
        info.Environment["DataSaver__Batch__MaxSize"] = "0";
        info.Environment["DataSaver__Kafka__SaslPassword"] = "top-secret-pw";
        info.Environment["ASPNETCORE_URLS"] = "http://127.0.0.1:0";

        using var process = Process.Start(info)!;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var stderr = await process.StandardError.ReadToEndAsync(timeout.Token);
        var stdout = await process.StandardOutput.ReadToEndAsync(timeout.Token);
        await process.WaitForExitAsync(timeout.Token);

        Assert.Equal(78, process.ExitCode);
        // Хост сообщает о первой невалидной секции; важно, что в строке есть имя настройки.
        Assert.Contains("DataSaver__", stderr, StringComparison.Ordinal);
        Assert.Single(stderr.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
        Assert.DoesNotContain("   at ", stderr + stdout, StringComparison.Ordinal);
        Assert.DoesNotContain("top-secret-pw", stderr + stdout, StringComparison.Ordinal);
    }
}
