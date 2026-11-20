using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Нативные логи librdkafka -> ILogger. Уровни syslog: Emergency..Critical -> Critical, Error -> Error,
/// Warning -> Warning, Notice/Info -> Debug, Debug -> Trace. Транспортные сообщения (FAIL, TRANSPORT, BROKERFAIL,
/// CONNECT) повторяются на каждую попытку переподключения к каждому брокеру: пока брокер недоступен, они идут
/// не чаще раза в 30 с на фасет и уровнем Warning (причину сбоя в любом случае сообщает error-callback). Остальные
/// ошибки и Critical не подавляются. Подавление идёт по ключу (фасет): сообщение с тем же фасетом, но другим текстом,
/// в течение окна подавляется тоже.
/// </summary>
internal sealed class KafkaNativeLogBridge(ILogger logger, LogThrottle throttle)
{
    private static readonly HashSet<string> TransportFacilities = new(StringComparer.OrdinalIgnoreCase)
    {
        "FAIL", "TRANSPORT", "BROKERFAIL", "CONNECT",
    };

    public void Handle(LogMessage message)
    {
        var level = Map(message.Level);
        var isTransportFacility = TransportFacilities.Contains(message.Facility);
        var isWarningOrError = level is LogLevel.Warning or LogLevel.Error;
        if (isTransportFacility && isWarningOrError)
        {
            // Ключ общий для consumer и producer: один фасет - одна строка за окно на процесс.
            if (!throttle.TryPass("native:" + message.Facility, out var suppressed))
            {
                return;
            }

            if (logger.IsEnabled(LogLevel.Warning))
            {
                logger.Log(LogLevel.Warning, "librdkafka [{Facility}] {Message} (suppressed since last: {Suppressed}; repeats are not logged for 30 s)",
                    message.Facility, message.Message, suppressed);
            }

            return;
        }

        if (logger.IsEnabled(level))
        {
            logger.Log(level, "librdkafka [{Facility}] {Message}", message.Facility, message.Message);
        }
    }

    internal static LogLevel Map(SyslogLevel level) => level switch
    {
        SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical => LogLevel.Critical,
        SyslogLevel.Error => LogLevel.Error,
        SyslogLevel.Warning => LogLevel.Warning,
        SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Debug,
        _ => LogLevel.Trace,
    };
}
