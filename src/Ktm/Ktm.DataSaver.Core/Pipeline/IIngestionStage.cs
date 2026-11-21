using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Core.Pipeline;

/// <summary>
/// Стадия конвейера после разбора формата: чистая функция «сообщение -> вердикт» (ADR-0003 п. 2.1).
/// Стадия не пишет в БД и не публикует, не меняет тело, не обращается к БД журнала на каждое сообщение.
/// Исключение стадии не превращается в отбраковку: оно покидает конвейер и трактуется как сбой пачки.
/// </summary>
public interface IIngestionStage
{
    IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed);
}
