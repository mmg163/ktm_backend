using System.Diagnostics.Metrics;
using Ktm.DataSaver.Core.Ingestion;

namespace Ktm.DataSaver.Infrastructure.Observability;

/// <summary>
/// Метрики Data Saver (System.Diagnostics.Metrics, инструменты с префиксом <c>ktm.datasaver.</c>). Теги не содержат
/// персональных данных: только причина отбраковки и стадия повтора.
/// </summary>
public sealed class DataSaverMetrics
{
    public const string MeterName = "Ktm.DataSaver";

    private readonly Counter<long> _read;
    private readonly Counter<long> _written;
    private readonly Counter<long> _duplicates;
    private readonly Counter<long> _corrections;
    private readonly Counter<long> _rejected;
    private readonly Counter<long> _retries;
    private readonly Counter<long> _adminNotifications;
    private readonly Histogram<double> _batchDuration;
    private readonly Histogram<int> _batchSize;

    private readonly Counter<long> _ruleRejected;
    private readonly Counter<long> _ruleReloads;
    private readonly TimeProvider _time;
    private int _activeRules;
    private int _invalidRules;
    private long _lastCheckUnixMs = long.MinValue;

    public DataSaverMetrics(Meter meter, TimeProvider? time = null)
    {
        Meter = meter;
        _time = time ?? TimeProvider.System;
        _ruleRejected = meter.CreateCounter<long>("ktm.datasaver.rules.rejected", "{message}", "Messages rejected by a rejection rule, by source and rule id");
        _ruleReloads = meter.CreateCounter<long>("ktm.datasaver.rules.reloads", "{attempt}", "Rule set load attempts, by result and trigger");
        meter.CreateObservableGauge("ktm.datasaver.rules.active", () => Volatile.Read(ref _activeRules), "{rule}", "Enabled rules in the current snapshot");
        meter.CreateObservableGauge("ktm.datasaver.rules.invalid", () => Volatile.Read(ref _invalidRules), "{rule}", "Enabled rules not applied because of an error");
        meter.CreateObservableGauge("ktm.datasaver.rules.snapshot_age", SnapshotAge, "s", "Seconds since the last successful check of the rule table");
        _read = meter.CreateCounter<long>("ktm.datasaver.messages.read", "{message}", "Messages read from the raw topic");
        _written = meter.CreateCounter<long>("ktm.datasaver.events.written", "{event}", "Events newly inserted into the journal");
        _duplicates = meter.CreateCounter<long>("ktm.datasaver.events.duplicates", "{event}", "Events skipped as already stored (idempotent redelivery)");
        _corrections = meter.CreateCounter<long>("ktm.datasaver.corrections.written", "{correction}", "Correction links newly inserted");
        _rejected = meter.CreateCounter<long>("ktm.datasaver.messages.rejected", "{message}", "Messages published to the rejected topic, by reason");
        _retries = meter.CreateCounter<long>("ktm.datasaver.retries", "{attempt}", "Failed write and rejected-publish attempts, by stage (the batch is then retried or handed to the failure handler)");
        _adminNotifications = meter.CreateCounter<long>("ktm.datasaver.admin_notifications", "{notification}", "Administrator notifications (stub)");
        _batchDuration = meter.CreateHistogram<double>("ktm.datasaver.batch.duration", "s", "Duration of a successful batch write");
        _batchSize = meter.CreateHistogram<int>("ktm.datasaver.batch.size", "{event}", "Number of events in a written batch");
    }

    public Meter Meter { get; }

    public void MessageRead() => _read.Add(1);

    public void BatchWritten(int batchSize, int inserted, int corrections, TimeSpan duration)
    {
        _written.Add(inserted);
        _duplicates.Add(batchSize - inserted);
        _corrections.Add(corrections);
        _batchSize.Record(batchSize);
        _batchDuration.Record(duration.TotalSeconds);
    }

    public void MessageRejected(string reason) =>
        _rejected.Add(1, new KeyValuePair<string, object?>("reason", reason));

    /// <summary>Отбраковка правилом; <c>ruleId</c> берётся только из загруженного снимка, не из данных сообщения.</summary>
    public void RuleRejected(string source, string ruleId) =>
        _ruleRejected.Add(1, new KeyValuePair<string, object?>("source", source), new KeyValuePair<string, object?>("rule_id", ruleId));

    /// <summary>
    /// Попытка загрузки набора правил: <c>result</c> - changed/unchanged/failed, <c>trigger</c> - timer/startup.
    /// Для changed и unchanged (успешная проверка таблицы) сбрасывает возраст снимка; failed его не трогает.
    /// </summary>
    public void RulesReloaded(string result, string trigger)
    {
        _ruleReloads.Add(1, new KeyValuePair<string, object?>("result", result), new KeyValuePair<string, object?>("trigger", trigger));
        if (result is "changed" or "unchanged")
        {
            Interlocked.Exchange(ref _lastCheckUnixMs, _time.GetUtcNow().ToUnixTimeMilliseconds());
        }
    }

    /// <summary>Состояние снимка после загрузки: включённых правил и не применённых из-за ошибки.</summary>
    public void SetRulesState(int active, int invalid)
    {
        Volatile.Write(ref _activeRules, active);
        Volatile.Write(ref _invalidRules, invalid);
    }

    private IEnumerable<Measurement<double>> SnapshotAge()
    {
        var last = Interlocked.Read(ref _lastCheckUnixMs);
        if (last != long.MinValue)
        {
            yield return new Measurement<double>(Math.Max(0, (_time.GetUtcNow().ToUnixTimeMilliseconds() - last) / 1000.0));
        }
    }

    public void Retry(string stage) =>
        _retries.Add(1, new KeyValuePair<string, object?>("stage", stage));

    public void AdminNotified() => _adminNotifications.Add(1);

    /// <summary>Состояние цикла (0 - Running, 1 - Degraded) и число повторов текущей пачки как наблюдаемые измерения.</summary>
    public void ObserveLoop(IngestionLoop loop)
    {
        Meter.CreateObservableGauge("ktm.datasaver.loop.state", () => (int)loop.State, description: "Ingestion loop state: 0 Running, 1 Degraded");
        Meter.CreateObservableGauge("ktm.datasaver.loop.retry_streak", () => loop.RetryCount, "{attempt}", "Failed attempts of the current batch");
    }
}
