using System.Globalization;
using Ktm.DataSaver.Core.Ingestion;

namespace Ktm.DataSaver.Infrastructure.Postgres;

/// <summary>
/// Временный источник коэффициентов доверия (ADR-0002, G-3): <c>DataSaver__Trust__Default</c> (по умолчанию 100) и
/// переопределения <c>DataSaver__Trust__Source__&lt;source&gt;</c>. Заменяется хранилищем настроек.
/// </summary>
public sealed class EnvironmentSourceTrustProvider : ISourceTrustProvider
{
    public const string DefaultVariable = "DataSaver__Trust__Default";
    public const string SourcePrefix = "DataSaver__Trust__Source__";
    public const short DefaultTrust = 100;

    private readonly Func<string, string?> _getVariable;
    private readonly short _default;

    public EnvironmentSourceTrustProvider(Func<string, string?>? getVariable = null)
    {
        _getVariable = getVariable ?? Environment.GetEnvironmentVariable;
        _default = Parse(DefaultVariable, _getVariable(DefaultVariable)) ?? DefaultTrust;
    }

    public short GetTrust(string source)
    {
        ArgumentException.ThrowIfNullOrEmpty(source);
        var name = SourcePrefix + source;
        return Parse(name, _getVariable(name)) ?? _default;
    }

    private static short? Parse(string name, string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        if (!short.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var trust) || trust > 100)
        {
            throw new InvalidOperationException($"{name} must be an integer in range 0..100.");
        }

        return trust;
    }
}
