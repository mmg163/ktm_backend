using Ktm.DataSaver;
using Ktm.DataSaver.Health;
using Microsoft.Extensions.Options;

try
{
    var builder = WebApplication.CreateBuilder(args);
    builder.Services.AddDataSaver(builder.Configuration);

    var app = builder.Build();
    // Проверка настроек до старта host: иначе host сам пишет в лог стек-трейс "Hosting failed to start".
    app.Services.GetRequiredService<IStartupValidator>().Validate();
    app.MapDataSaverHealth();
    await app.RunAsync();
    return 0;
}
catch (Exception ex) when (StartupErrors.TryDescribe(ex, out var description))
{
    // Ошибка конфигурации: одна понятная строка с именами настроек вместо стек-трейса; рестарты её не лечат.
    await Console.Error.WriteLineAsync(description);
    return StartupErrors.ConfigExitCode;
}

/// <summary>Точка входа для WebApplicationFactory в интеграционных тестах.</summary>
public partial class Program;
