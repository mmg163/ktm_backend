namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>Ожидание условий вместо фиксированных пауз: опрос с коротким шагом и понятной причиной таймаута.</summary>
internal static class Wait
{
    public static readonly TimeSpan Default = TimeSpan.FromSeconds(90);

    public static async Task UntilAsync(Func<Task<bool>> condition, string what, TimeSpan? timeout = null, Func<string>? lastObservation = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? Default);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(100);
        }

        throw new TimeoutException($"Timed out waiting for: {what}" + (lastObservation is null ? string.Empty : $" (last observed: {lastObservation()})"));
    }

    public static Task UntilAsync(Func<bool> condition, string what, TimeSpan? timeout = null, Func<string>? lastObservation = null) =>
        UntilAsync(() => Task.FromResult(condition()), what, timeout, lastObservation);
}
