namespace Ktm.DataSaver.Application.Resilience;

/// <summary>
/// Политика ухода в dlq (ADR-0006 п. 2.3): <c>MaxAttempts</c> - число неудачных попыток по ошибке, привязанной к сообщению,
/// до изоляции и dlq; <c>BreakerThreshold</c> - сколько сообщений подряд без успешной записи считается системной проблемой.
/// <c>Enabled = false</c> - прежнее поведение: повтор без предела.
/// </summary>
public sealed record DlqPolicy(bool Enabled, int MaxAttempts, int BreakerThreshold)
{
    public const int DefaultMaxAttempts = 3;
    public const int DefaultBreakerThreshold = 20;

    public static DlqPolicy Default { get; } = new(true, DefaultMaxAttempts, DefaultBreakerThreshold);
}
