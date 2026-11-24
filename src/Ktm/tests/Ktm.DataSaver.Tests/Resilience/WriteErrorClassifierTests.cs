using System.Net.Sockets;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Resilience;

namespace Ktm.DataSaver.Tests.Resilience;

public class WriteErrorClassifierTests
{
    private sealed class FakeDbException(string? sqlState, Exception? inner = null)
        : Exception("db", inner), ISqlStateError
    {
        public string? SqlState { get; } = sqlState;
    }

    [Theory]
    [InlineData("57P01", WriteErrorClass.Transient)]
    [InlineData("57P02", WriteErrorClass.Transient)]
    [InlineData("57P03", WriteErrorClass.Transient)]
    [InlineData("53000", WriteErrorClass.Transient)]
    [InlineData("53300", WriteErrorClass.Transient)]
    [InlineData("40001", WriteErrorClass.Transient)]
    [InlineData("40P01", WriteErrorClass.Transient)]
    [InlineData("08006", WriteErrorClass.Transient)]
    [InlineData("42501", WriteErrorClass.FatalConfig)]
    [InlineData("42P01", WriteErrorClass.FatalConfig)]
    [InlineData("28P01", WriteErrorClass.FatalConfig)]
    [InlineData("22001", WriteErrorClass.Data)]
    [InlineData("22P02", WriteErrorClass.Data)]
    [InlineData("23502", WriteErrorClass.Data)]
    [InlineData("23514", WriteErrorClass.Data)]
    [InlineData("28000", WriteErrorClass.FatalConfig)]
    [InlineData("42601", WriteErrorClass.FatalConfig)]
    [InlineData("3D000", WriteErrorClass.FatalConfig)]
    [InlineData("3F000", WriteErrorClass.FatalConfig)]
    [InlineData("0A000", WriteErrorClass.FatalConfig)]
    [InlineData("22P05", WriteErrorClass.Data)]
    [InlineData("57014", WriteErrorClass.Transient)]
    [InlineData("99999", WriteErrorClass.Transient)]
    [InlineData(null, WriteErrorClass.Transient)]
    public void Sqlstate_maps_to_class(string? sqlState, WriteErrorClass expected) =>
        Assert.Equal(expected, WriteErrorClassifier.ClassifySqlState(sqlState));

    [Fact]
    public void Exception_with_sqlstate_is_classified_by_it_even_when_wrapped()
    {
        var wrapped = new InvalidOperationException("outer", new FakeDbException("42501"));

        Assert.Equal(WriteErrorClass.FatalConfig, WriteErrorClassifier.Classify(wrapped));
    }

    [Fact]
    public void Timeout_and_socket_break_are_transient()
    {
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new TimeoutException()));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new IOException("x", new SocketException())));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new FakeDbException(null, new SocketException())));
    }

    // Решение лида (ADR-0006 п. 2.2): прежнее правило «неизвестное исключение - Transient» отменено.
    // Без предела повторяется только инфраструктура; неожиданное исключение - свойство сообщения.
    [Fact]
    public void Unknown_exception_is_unexpected_and_spends_the_message_budget()
    {
        Assert.Equal(WriteErrorClass.Unexpected, WriteErrorClassifier.Classify(new InvalidOperationException()));
        Assert.Equal(WriteErrorClass.Unexpected, WriteErrorClassifier.Classify(new NullReferenceException()));
        Assert.Equal(WriteErrorClass.Unexpected, WriteErrorClassifier.Classify(new FormatException()));
        Assert.Equal(WriteErrorClass.Unexpected, WriteErrorClassifier.Classify(new AggregateException()));
    }

    private sealed class FakeDriverException(string? sqlState) : System.Data.Common.DbException("driver")
    {
        public override string? SqlState { get; } = sqlState;
    }

    private sealed class PoolExhausted : Exception, IInfrastructureFailure;

    [Fact]
    public void Infrastructure_failures_stay_transient_however_they_are_wrapped()
    {
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new System.Net.Sockets.SocketException()));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new ObjectDisposedException("data source")));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new TaskCanceledException()));
        // Драйвер БД без SQLSTATE (нет соединения, сбой протокола) - инфраструктура; с SQLSTATE 22/23 - данные.
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new FakeDriverException(null)));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new InvalidOperationException("outer", new FakeDriverException(null))));
        Assert.Equal(WriteErrorClass.Data, WriteErrorClassifier.Classify(new FakeDriverException("23514")));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new InvalidOperationException("outer", new PoolExhausted())));
    }

    [Fact]
    public void Stage_failure_is_classified_by_its_inner_exception()
    {
        Assert.Equal(WriteErrorClass.Unexpected, WriteErrorClassifier.Classify(new PipelineStageException(PipelineStages.Rules, new InvalidOperationException())));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new PipelineStageException(PipelineStages.Validation, new TimeoutException())));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new PipelineStageException(PipelineStages.Parse, new IOException())));
    }

    [Fact]
    public void Canceled_query_through_wrapper_is_transient()
    {
        var wrapped = new InvalidOperationException("outer", new FakeDbException("57014"));

        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(wrapped));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new OperationCanceledException()));
    }

    [Fact]
    public void Aggregate_transient_wins_over_data_and_fatal_wins_over_all()
    {
        var mixed = new AggregateException(new FakeDbException("22001"), new TimeoutException());
        var withFatal = new AggregateException(new TimeoutException(), new FakeDbException("42501"));
        var onlyData = new AggregateException(new FakeDbException("22001"), new FakeDbException("23502"));
        var dataAndUnexpected = new AggregateException(new FakeDbException("22001"), new InvalidOperationException());

        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(mixed));
        Assert.Equal(WriteErrorClass.FatalConfig, WriteErrorClassifier.Classify(withFatal));
        Assert.Equal(WriteErrorClass.Data, WriteErrorClassifier.Classify(onlyData));
        Assert.Equal(WriteErrorClass.Unexpected, WriteErrorClassifier.Classify(dataAndUnexpected));
    }
}
