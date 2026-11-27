using System.Globalization;
using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Application.Rules;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;

namespace Ktm.HostedServices.Hosting;

/// <summary>
/// Загрузка и перезагрузка правил отбраковки (ADR-0005 п. 2.10). Первая загрузка выполняется до старта цикла приёма:
/// <see cref="DataSaverWorker"/> ждёт <see cref="WaitForFirstLoadAsync"/>, Kafka до этого не читается. БД недоступна -
/// повтор с backoff; нет таблицы или прав (<see cref="RuleSourceFailureKind.Configuration"/>) - код выхода 78.
/// Затем таймер (и необязательный <see cref="IRuleChangeSignal"/>): сбой чтения оставляет прежний снимок, Warning и
/// <c>reloads{result=failed}</c>. Снимок заменяет <see cref="RuleSnapshotRefresher"/> атомарно, сервис отвечает за лог и метрики.
/// </summary>
public sealed class RuleRefreshService : BackgroundService
{
    public const int LoopFailureExitCode = 1;

    /// <summary>Не чаще раза за интервал на причину сбоя чтения (ADR п. 2.10).</summary>
    public static readonly TimeSpan FailureLogInterval = TimeSpan.FromMinutes(10);

    /// <summary>Не чаще раза за интервал на <c>(rule_id, version, причина)</c> ошибочного правила (ADR п. 2.10).</summary>
    public static readonly TimeSpan RuleErrorLogInterval = TimeSpan.FromHours(1);

    private const int RevisionPrefixLength = 12;

    private readonly RuleSnapshotRefresher _refresher;
    private readonly RuleSnapshotHolder _holder;
    private readonly TimeSpan _interval;
    private readonly BackoffPolicy _backoff;
    private readonly TimeProvider _time;
    private readonly DataSaverMetrics _metrics;
    private readonly IProcessTerminator _terminator;
    private readonly IRuleChangeSignal? _signal;
    private readonly ILogger<RuleRefreshService> _logger;
    private readonly LogThrottle _failureThrottle;
    private readonly LogThrottle _ruleErrorThrottle;
    private readonly TaskCompletionSource _firstLoad = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public RuleRefreshService(
        RuleSnapshotRefresher refresher,
        RuleSnapshotHolder holder,
        TimeSpan interval,
        BackoffPolicy backoff,
        TimeProvider time,
        DataSaverMetrics metrics,
        IProcessTerminator terminator,
        ILogger<RuleRefreshService> logger,
        IRuleChangeSignal? signal = null)
    {
        _refresher = refresher;
        _holder = holder;
        _interval = interval;
        _backoff = backoff;
        _time = time;
        _metrics = metrics;
        _terminator = terminator;
        _logger = logger;
        _signal = signal;
        _failureThrottle = new LogThrottle(time, FailureLogInterval);
        _ruleErrorThrottle = new LogThrottle(time, RuleErrorLogInterval);
    }

    /// <summary>Завершается после первой успешной загрузки (пустая таблица - тоже успешная). При остановке host бросает отмену.</summary>
    public Task WaitForFirstLoadAsync(CancellationToken cancellationToken) => _firstLoad.Task.WaitAsync(cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Загрузка может быть синхронной: уходим с потока запуска host, чтобы не задерживать старт остальных служб.
        await Task.Yield();
        try
        {
            if (!await LoadFirstAsync(stoppingToken))
            {
                return;
            }

            _firstLoad.TrySetResult();
            await foreach (var trigger in RuleRefreshTriggers.WatchAsync(_interval, _time, _signal, stoppingToken))
            {
                await ReloadAsync(trigger == RuleRefreshTrigger.Timer ? "timer" : "signal", stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Штатная остановка по токену host.
        }
        catch (Exception ex)
        {
            // Дефект загрузчика или сбой сигнала: молча остановленное обновление оставило бы правила устаревшими.
            _logger.LogCritical(ex, "Rule refresh loop failed; the process is terminating");
            _terminator.Terminate(LoopFailureExitCode, "Rule refresh loop failed: " + ex.GetType().Name);
        }
    }

    /// <returns><c>false</c> - загрузка невозможна из-за конфигурации, процесс завершается.</returns>
    private async Task<bool> LoadFirstAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                var result = await _refresher.RefreshAsync(cancellationToken);
                _backoff.Reset();
                Applied(result, "startup");
                return true;
            }
            catch (RuleSourceException ex) when (ex.Kind == RuleSourceFailureKind.Configuration)
            {
                _metrics.RulesReloaded("failed", "startup");
                _logger.LogCritical("Rules cannot be loaded, the service is not started: {Reason}", ex.Message);
                _terminator.Terminate(StartupErrors.ConfigExitCode, "Rule table is not readable: " + ex.Message);
                return false;
            }
            catch (RuleSourceException ex)
            {
                _metrics.RulesReloaded("failed", "startup");
                var delay = _backoff.NextDelay();
                Failed(ex, startup: true, delay);
                await Task.Delay(delay, _time, cancellationToken);
            }
        }
    }

    private async Task ReloadAsync(string trigger, CancellationToken cancellationToken)
    {
        try
        {
            Applied(await _refresher.RefreshAsync(cancellationToken), trigger);
        }
        catch (RuleSourceException ex)
        {
            // Любой сбой источника после первой загрузки, включая 42P01/42501, - недоступность: действует прежний снимок.
            _metrics.RulesReloaded("failed", trigger);
            Failed(ex, startup: false, _interval);
        }
    }

    private void Failed(RuleSourceException exception, bool startup, TimeSpan next)
    {
        if (!_failureThrottle.TryPass($"{exception.Kind}:{exception.Message}", out var suppressed))
        {
            return;
        }

        if (startup)
        {
            _logger.LogWarning(
                "Rules are not loaded yet, ingestion waits for the first load; retry in {Delay}. Reason: {Reason}; repeats suppressed: {Suppressed}",
                next, exception.Message, suppressed);
        }
        else
        {
            _logger.LogWarning(
                "Rule reload failed, the previous rule set stays in effect; next attempt in {Delay}. Reason: {Reason}; repeats suppressed: {Suppressed}",
                next, exception.Message, suppressed);
        }
    }

    private void Applied(RuleRefreshResult result, string trigger)
    {
        var snapshot = result.Snapshot;
        if (result.Outcome == RuleRefreshOutcome.Changed)
        {
            _metrics.RulesReloaded("changed", trigger);
            _metrics.SetRulesState(snapshot.Rules.Count, snapshot.Errors.Count);
            LogChanged(snapshot);
        }
        else
        {
            _metrics.RulesReloaded("unchanged", trigger);
        }

        // Напоминание об ошибочных правилах не чаще раза в час, пока они остаются в таблице (при unchanged набор не пересобирается).
        foreach (var error in snapshot.Errors)
        {
            if (_ruleErrorThrottle.TryPass($"{error.RuleId}|{error.RuleVersion}|{error.Reason}", out _))
            {
                _logger.LogError(
                    "Rejection rule is not applied: RuleId {RuleId}, RuleVersion {RuleVersion}, reason: {Reason}",
                    error.RuleId, error.RuleVersion?.ToString(CultureInfo.InvariantCulture) ?? "unknown", error.Reason);
            }
        }
    }

    private void LogChanged(RuleSnapshot snapshot)
    {
        var revision = snapshot.Revision is { Length: > RevisionPrefixLength } r ? r[..RevisionPrefixLength] : snapshot.Revision;
        var changes = snapshot.Changes;
        if (snapshot.Rules.Count == 0)
        {
            _logger.LogInformation(
                "Rejection rule set applied: revision {Revision}, no enabled rules, {InvalidCount} invalid; added [{Added}], changed [{Changed}], removed [{Removed}]",
                revision, snapshot.Errors.Count, Format(changes.Added), Format(changes.Changed), Format(changes.Removed));
        }
        else
        {
            _logger.LogInformation(
                "Rejection rule set applied: revision {Revision}, {RuleCount} active rules, {InvalidCount} invalid; added [{Added}], changed [{Changed}], removed [{Removed}]",
                revision, snapshot.Rules.Count, snapshot.Errors.Count, Format(changes.Added), Format(changes.Changed), Format(changes.Removed));
        }

        foreach (var rule in changes.ChangedWithoutVersionIncrease)
        {
            _logger.LogWarning("Rejection rule {RuleId} was changed without increasing version (version {RuleVersion})", rule.Id, rule.Version);
        }
    }

    private static string Format(IReadOnlyList<RuleRef> rules) => string.Join(", ", rules.Select(r => $"{r.Id}@{r.Version}"));
}
