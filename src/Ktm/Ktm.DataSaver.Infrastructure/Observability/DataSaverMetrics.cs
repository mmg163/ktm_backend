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

    public DataSaverMetrics(Meter meter)
    {
        Meter = meter;
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
