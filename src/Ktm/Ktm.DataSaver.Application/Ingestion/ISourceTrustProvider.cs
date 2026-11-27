namespace Ktm.DataSaver.Core.Ingestion;

/// <summary>
/// Степень доверия к данным источника на момент приёма, 0..100 (Схема_БД_ER.md п. 7.1). Значение фиксируется в строке
/// журнала; источник - настройки, не сообщение. До появления хранилища настроек - временная реализация из окружения.
/// </summary>
public interface ISourceTrustProvider
{
    short GetTrust(string source);
}
