using System.Globalization;
using Ktm.DataSaver.Core.Ingestion;

namespace Ktm.DataSaver.Infrastructure.Postgres;

/// <summary>Настройка задана недопустимым значением; <see cref="Setting"/> - имя настройки, значение в сообщение не входит.</summary>
public sealed class InvalidSettingException(string setting, string message) : InvalidOperationException(message)
{
    public string Setting { get; } = setting;
}

/// <summary>
/// Временный источник коэффициентов доверия (ADR-0002, G-3): <c>DataSaver__Trust__Default</c> (по умолчанию 100) и
/// переопределения <c>DataSaver__Trust__Source__&lt;source&gt;</c>. Все значения проверяются в конструкторе (отказ запуска);
/// <see cref="GetTrust"/> при записи не бросает. Заменяется хранилищем настроек.
/// </summary>
public sealed class EnvironmentSourceTrustProvider : ISourceTrustProvider
{
    public const string DefaultVariable = "DataSaver__Trust__Default";
    public const string SourcePrefix = "DataSaver__Trust__Source__";
    public const short DefaultTrust = 100;

    private readonly short _default;
    private readonly Dictionary<string, short> _bySource = new(StringComparer.Ordinal);

    public EnvironmentSourceTrustProvider(IReadOnlyDictionary<string, string?> variables)
    {
        ArgumentNullException.ThrowIfNull(variables);
        _default = variables.TryGetValue(DefaultVariable, out var raw) ? Parse(DefaultVariable, raw) ?? DefaultTrust : DefaultTrust;

        foreach (var (name, value) in variables)
        {
            if (!name.StartsWith(SourcePrefix, StringComparison.Ordinal) || name.Length == SourcePrefix.Length) continue;
            if (Parse(name, value) is { } trust) _bySource[name[SourcePrefix.Length..]] = trust;
        }
    }

    public short GetTrust(string source)
    {
        ArgumentException.ThrowIfNullOrEmpty(source);
        return _bySource.TryGetValue(source, out var trust) ? trust : _default;
    }

    private static short? Parse(string name, string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        if (!short.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var trust) || trust > 100)
        {
            throw new InvalidSettingException(name, $"{name} must be an integer in range 0..100.");
        }

        return trust;
    }
}
