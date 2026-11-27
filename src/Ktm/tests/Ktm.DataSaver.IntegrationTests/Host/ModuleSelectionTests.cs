using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace Ktm.DataSaver.IntegrationTests.Host;

public sealed class ModuleSelectionTests
{
    [Fact]
    public async Task WorkersStartWithoutKafkaOrHttpListener()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var info = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        info.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "Ktm.HostedServices.dll"));
        info.Environment["ASPNETCORE_URLS"] = $"http://127.0.0.1:{port}";
        info.Environment["HostedServices__DataSaver__Enabled"] = "false";
        info.Environment["HostedServices__Binding__Enabled"] = "true";
        info.Environment["HostedServices__Interpretation__Enabled"] = "true";
        foreach (string module in new[] { "Binding", "Interpretation" })
        {
            string section = $"Ktm__{module}__Postgres__";
            info.Environment[section + "Host"] = "localhost";
            info.Environment[section + "Database"] = "ktm";
            info.Environment[section + "Username"] = module;
            info.Environment[section + "Password"] = "secret";
        }
        using var process = Process.Start(info)!;
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var lines = new List<string>();
            while (true)
            {
                string? line = await process.StandardOutput.ReadLineAsync(timeout.Token);
                Assert.NotNull(line);
                lines.Add(line);
                if (lines.Any(item => item.Contains("Application started", StringComparison.Ordinal))
                    && lines.Any(item => item.Contains("Binding worker registered", StringComparison.Ordinal))
                    && lines.Any(item => item.Contains("Interpretation worker registered", StringComparison.Ordinal))) break;
            }
            Assert.False(process.HasExited);
            Assert.Contains(lines, line => line.Contains("Binding worker registered", StringComparison.Ordinal));
            Assert.Contains(lines, line => line.Contains("Interpretation worker registered", StringComparison.Ordinal));
        }
        finally
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync();
        }
    }
}
