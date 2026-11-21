using System.Diagnostics.Metrics;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>Сумма измерений счётчиков одного <see cref="Meter"/>; ключ - имя инструмента, для тегированных - <c>имя[значение первого тега]</c>.</summary>
internal sealed class MetricCounters : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly Dictionary<string, long> _values = [];

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
        _listener.Start();
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
