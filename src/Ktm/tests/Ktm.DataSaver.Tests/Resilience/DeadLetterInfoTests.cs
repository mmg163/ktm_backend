using Ktm.DataSaver.Application.Pipeline;
using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Tests.Ingestion;

namespace Ktm.DataSaver.Tests.Resilience;

public class DeadLetterInfoTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 24, 10, 0, 0, TimeSpan.Zero);

    private const string Secret = "Key (card)=(0012345678) ФИО Иванов";

    private sealed class AdaptedDbError(string sqlState, string constraint, string table)
        : Exception(Secret), ISqlStateError, IDbErrorLocation, IReportedExceptionType
    {
        public string? SqlState { get; } = sqlState;

        public string? ConstraintName { get; } = constraint;

        public string? TableName { get; } = table;

        public string ReportedTypeName => "Npgsql.PostgresException";
    }

    [Fact]
    public void DatabaseRejection_GivesWriteDataError_WithSqlStateAndObjects_AndNoExceptionText()
    {
        var info = DeadLetterInfo.FromException(new AdaptedDbError("23514", "ck_events_source", "events"), PipelineStages.Write, 4, Now);

        Assert.Equal(
            (DeadLetterReasons.WriteDataError, "write", "Npgsql.PostgresException", "23514", "ck_events_source", "events", 4, Now),
            (info.Reason, info.Stage, info.ExceptionType, info.SqlState, info.ConstraintName, info.TableName, info.Attempts, info.FailedAt));
        Assert.Contains("23514", info.Detail);
        Assert.Contains("ck_events_source", info.Detail);
        Assert.DoesNotContain("Иванов", info.Detail);
        Assert.DoesNotContain("0012345678", info.Detail);
    }

    [Theory]
    [InlineData(typeof(InvalidCastException))]
    [InlineData(typeof(FormatException))]
    [InlineData(typeof(OverflowException))]
    [InlineData(typeof(ArgumentOutOfRangeException))]
    public void SerializationErrorInWrite_GivesWriteValueError(Type exceptionType)
    {
        var error = (Exception)Activator.CreateInstance(exceptionType, Secret)!;

        var info = DeadLetterInfo.FromException(error, PipelineStages.Write, 4, Now);

        Assert.Equal(DeadLetterReasons.WriteValueError, info.Reason);
        Assert.Equal(exceptionType.FullName, info.ExceptionType);
        Assert.Null(info.SqlState);
        Assert.DoesNotContain("Иванов", info.Detail);
    }

    [Theory]
    [InlineData(PipelineStages.Parse)]
    [InlineData(PipelineStages.Validation)]
    [InlineData(PipelineStages.Rules)]
    public void StageFailure_TakesStageFromWrapper_AndTypeFromFirstNonWrapperInner(string stage)
    {
        // Обёртки: PipelineStageException и AggregateException; тип берётся у первого не-обёрточного исключения.
        var error = new PipelineStageException(stage, new AggregateException(new InvalidOperationException(Secret)));

        var info = DeadLetterInfo.FromException(error, PipelineStages.Write, 3, Now);

        Assert.Equal((DeadLetterReasons.UnexpectedError, stage, typeof(InvalidOperationException).FullName), (info.Reason, info.Stage, info.ExceptionType));
        Assert.Equal(3, info.Attempts);
        Assert.DoesNotContain("Иванов", info.Detail);
        // Даже сериализационное исключение вне записи - не write_value_error.
        Assert.Equal(DeadLetterReasons.UnexpectedError, DeadLetterInfo.FromException(new PipelineStageException(stage, new FormatException()), PipelineStages.Write, 1, Now).Reason);
    }

    [Fact]
    public void Detail_IsCutAtLimit_WithoutSplittingASurrogatePair()
    {
        // Префикс детали - 48 символов: эмодзи начинается на позиции 1023, граница 1024 приходится на его середину.
        var longTable = new string('t', 975) + "😀😀";
        var info = new DeadLetterInfo("r", "write", "X", "23514", null, longTable, 3, Now);

        var detail = info.Detail;

        Assert.Equal(1023, detail.Length);
        Assert.False(char.IsHighSurrogate(detail[^1]));
    }
}
