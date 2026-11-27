using System.Reflection;
using System.Text;
using Ktm.DataSaver.Application.Pipeline;

namespace Ktm.DataSaver.Application.Resilience;

/// <summary>Значения заголовка <c>ktm-dlq-reason</c> (ADR-0006 п. 2.4).</summary>
public static class DeadLetterReasons
{
    /// <summary>БД отвергла сообщение: SQLSTATE 22/23.</summary>
    public const string WriteDataError = "write_data_error";

    /// <summary>Ошибка сериализации значения параметра при записи.</summary>
    public const string WriteValueError = "write_value_error";

    /// <summary>Неожиданное исключение любой стадии.</summary>
    public const string UnexpectedError = "unexpected_error";
}

/// <summary>
/// Причина ухода сообщения в dlq. Строится из исключения без <see cref="Exception.Message"/>: только тип исключения,
/// SQLSTATE и имена ограничения и таблицы (значений из строки нет).
/// </summary>
public sealed record DeadLetterInfo(
    string Reason,
    string Stage,
    string ExceptionType,
    string? SqlState,
    string? ConstraintName,
    string? TableName,
    int Attempts,
    DateTimeOffset FailedAt)
{
    /// <summary>Предел длины текста <see cref="Detail"/> в UTF-16 code units.</summary>
    public const int MaxDetailLength = 1024;

    /// <summary>Текст для человека: стадия, тип ошибки, SQLSTATE, ограничение и таблица; не длиннее <see cref="MaxDetailLength"/>.</summary>
    public string Detail
    {
        get
        {
            var text = new StringBuilder("stage=").Append(Stage).Append("; exception=").Append(ExceptionType);
            if (!string.IsNullOrEmpty(SqlState))
            {
                text.Append("; sqlstate=").Append(SqlState);
            }

            if (!string.IsNullOrEmpty(ConstraintName))
            {
                text.Append("; constraint=").Append(ConstraintName);
            }

            if (!string.IsNullOrEmpty(TableName))
            {
                text.Append("; table=").Append(TableName);
            }

            return Truncate(text.ToString(), MaxDetailLength);
        }
    }

    /// <summary>
    /// Описание причины по исключению. Стадия берётся из <see cref="PipelineStageException"/> в цепочке, иначе -
    /// <paramref name="stage"/> (для записи - <see cref="PipelineStages.Write"/>).
    /// </summary>
    public static DeadLetterInfo FromException(Exception error, string stage, int attempts, DateTimeOffset failedAt)
    {
        ArgumentNullException.ThrowIfNull(error);

        var effectiveStage = FindStage(error) ?? stage;
        var core = Unwrap(error);
        var errorClass = WriteErrorClassifier.Classify(error);
        var reason = errorClass == WriteErrorClass.Data && effectiveStage == PipelineStages.Write
            ? DeadLetterReasons.WriteDataError
            : effectiveStage == PipelineStages.Write && IsValueError(core)
                ? DeadLetterReasons.WriteValueError
                : DeadLetterReasons.UnexpectedError;
        var location = WriteErrorClassifier.FindLocation(error);
        return new DeadLetterInfo(
            reason,
            effectiveStage,
            TypeName(core),
            WriteErrorClassifier.FindSqlState(error),
            location?.ConstraintName,
            location?.TableName,
            attempts,
            failedAt);
    }

    /// <summary>Короткое имя типа первого не-обёрточного исключения: для лога вместо текста исключения.</summary>
    public static string ShortTypeName(Exception error) => Unwrap(error).GetType().Name;

    private static string? FindStage(Exception error)
    {
        for (var current = error; current is not null; current = current.InnerException)
        {
            if (current is PipelineStageException stage)
            {
                return stage.Stage;
            }
        }

        return null;
    }

    /// <summary>Первое не-обёрточное исключение: пропускает <see cref="AggregateException"/>, <see cref="PipelineStageException"/>, <see cref="TargetInvocationException"/>.</summary>
    private static Exception Unwrap(Exception error)
    {
        var current = error;
        while (true)
        {
            switch (current)
            {
                case AggregateException { InnerExceptions.Count: > 0 } aggregate:
                    current = aggregate.InnerExceptions[0];
                    continue;
                case PipelineStageException { InnerException: { } stageInner }:
                    current = stageInner;
                    continue;
                case TargetInvocationException { InnerException: { } invocationInner }:
                    current = invocationInner;
                    continue;
                default:
                    return current;
            }
        }
    }

    private static string TypeName(Exception core) =>
        core is IReportedExceptionType { ReportedTypeName: { Length: > 0 } reported } ? reported : core.GetType().FullName ?? core.GetType().Name;

    private static bool IsValueError(Exception core) => WriteErrorClassifier.IsMessageBoundValueError(core);

    private static string Truncate(string value, int max)
    {
        if (value.Length <= max)
        {
            return value;
        }

        return value[..(char.IsHighSurrogate(value[max - 1]) ? max - 1 : max)];
    }
}
