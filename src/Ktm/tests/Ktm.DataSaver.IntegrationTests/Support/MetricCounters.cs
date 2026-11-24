using System.Diagnostics.Metrics;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>Сумма измерений счётчиков одного <see cref="Meter"/>; ключ - имя инструмента, для тегированных - <c>имя[значение первого тега]</c>.</summary>
internal sealed class MetricCounters : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly Dictionary<string, long> _values = [];
    private readonly Dictionary<string, int> _gauges = [];

    public MetricCounters(Meter meter)
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (ReferenceEquals(instrument.Meter, meter)) listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var key = tags.Length == 0 ? instrument.Name : $"{instrument.Name}[{tags[0].Value}]";
            lock (_values) _values[key] = _values.GetValueOrDefault(key) + value;
        });
        _listener.SetMeasurementEventCallback<int>((instrument, value, _, _) =>
        {
            lock (_values) _gauges[instrument.Name] = value;
        });
        _listener.Start();
    }

    /// <summary>Сумма счётчика по всем значениям первого тега (например, <c>rules.rejected</c> по всем источникам).</summary>
    public long Sum(string instrumentName)
    {
        lock (_values) return _values.Where(p => p.Key == instrumentName || p.Key.StartsWith(instrumentName + "[", StringComparison.Ordinal)).Sum(p => p.Value);
    }

    /// <summary>Текущее значение наблюдаемого gauge (целочисленного): слушатель запрашивает измерение при вызове.</summary>
    public int Gauge(string instrumentName)
    {
        _listener.RecordObservableInstruments();
        lock (_values) return _gauges.GetValueOrDefault(instrumentName);
    }

    public long this[string key]
    {
        get
        {
            lock (_values) return _values.GetValueOrDefault(key);
        }
    }

    public long Read => this["ktm.datasaver.messages.read"];

    public long Written => this["ktm.datasaver.events.written"];

    public long Duplicates => this["ktm.datasaver.events.duplicates"];

    public long CorrectionsWritten => this["ktm.datasaver.corrections.written"];

    public void Dispose() => _listener.Dispose();
}
