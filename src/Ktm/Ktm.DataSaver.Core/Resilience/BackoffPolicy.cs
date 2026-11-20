namespace Ktm.DataSaver.Core.Resilience;

/// <summary>
/// Пауза между повторами записи: min(Initial * Multiplier^n, Max) с джиттером +-JitterFraction.
/// Источник случайности внедряется (возвращает значение в [0, 1)). Не потокобезопасен.
/// </summary>
public sealed class BackoffPolicy
{
    private readonly TimeSpan _initial;
    private readonly TimeSpan _max;
    private readonly double _multiplier;
    private readonly double _jitterFraction;
    private readonly Func<double> _nextRandom;
    private int _attempt;

    public BackoffPolicy(TimeSpan initial, TimeSpan max, double multiplier, double jitterFraction, Func<double> nextRandom)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(initial, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(max, initial);
        ArgumentOutOfRangeException.ThrowIfLessThan(multiplier, 1.0);
        ArgumentOutOfRangeException.ThrowIfNegative(jitterFraction);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(jitterFraction, 1.0);
        ArgumentNullException.ThrowIfNull(nextRandom);
        _initial = initial;
        _max = max;
        _multiplier = multiplier;
        _jitterFraction = jitterFraction;
        _nextRandom = nextRandom;
    }

    public int Attempt => _attempt;

    /// <summary>Следующая пауза; увеличивает счётчик попыток (до достижения потолка).</summary>
    public TimeSpan NextDelay()
    {
        var baseMs = Math.Min(_initial.TotalMilliseconds * Math.Pow(_multiplier, _attempt), _max.TotalMilliseconds);
        if (baseMs < _max.TotalMilliseconds)
        {
            _attempt++;
        }

        var jitter = 1.0 + (_nextRandom() * 2.0 - 1.0) * _jitterFraction;
        return TimeSpan.FromMilliseconds(baseMs * jitter);
    }

    /// <summary>Сброс после успешной записи.</summary>
    public void Reset() => _attempt = 0;
}
