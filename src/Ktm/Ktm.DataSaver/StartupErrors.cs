using Microsoft.Extensions.Options;

namespace Ktm.DataSaver;

/// <summary>Сообщение и код выхода при некорректной конфигурации (EX_CONFIG, sysexits.h).</summary>
internal static class StartupErrors
{
    public const int ConfigExitCode = 78;

    /// <summary>
    /// Ошибка валидации настроек (одна или несколько в <see cref="AggregateException"/>) -> одна строка с перечнем
    /// нарушений. Сообщения валидаторов называют настройку и не содержат её значений. Прочие исключения - false.
    /// </summary>
    public static bool TryDescribe(Exception exception, out string description)
    {
        var failures = new List<string>();
        if (!Collect(exception, failures))
        {
            description = string.Empty;
            return false;
        }

        description = $"Invalid configuration, the service is not started: {string.Join(" ", failures)}";
        return true;
    }

    private static bool Collect(Exception exception, List<string> failures)
    {
        switch (exception)
        {
            case OptionsValidationException validation:
                var lines = validation.Failures.Select(f => f.ReplaceLineEndings(" ").Trim()).Where(f => f.Length > 0).ToArray();
                failures.AddRange(lines.Length > 0 ? lines : [validation.OptionsType.Name]);
                return true;
            case AggregateException aggregate when aggregate.InnerExceptions.Count > 0:
                return aggregate.InnerExceptions.All(inner => Collect(inner, failures));
            default:
                return false;
        }
    }
}
