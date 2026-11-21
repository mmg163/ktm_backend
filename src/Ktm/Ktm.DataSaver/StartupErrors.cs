using System.Text.RegularExpressions;
using Ktm.DataSaver.Infrastructure.Postgres;
using Microsoft.Extensions.Options;

namespace Ktm.DataSaver;

/// <summary>Сообщение и код выхода при некорректной конфигурации (EX_CONFIG, sysexits.h).</summary>
internal static partial class StartupErrors
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
            case InvalidSettingException invalid:
                failures.Add($"{invalid.Setting} has an invalid value.");
                return true;
            case InvalidOperationException binder when BinderPath().Match(binder.Message) is { Success: true } match:
                // Биндер не смог привести значение к типу: в тексте только путь настройки, значение берётся из внутреннего исключения.
                failures.Add($"{match.Groups[1].Value.Replace(":", "__", StringComparison.Ordinal)} has an invalid value (wrong type).");
                return true;
            default:
                return false;
        }
    }

    [GeneratedRegex(@"(?s)^Failed to convert configuration value .*at '([^']+)' to type")]
    private static partial Regex BinderPath();
}
