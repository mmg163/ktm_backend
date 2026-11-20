using System.Net.Sockets;
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
    [InlineData("99999", WriteErrorClass.Transient)]
    [InlineData(null, WriteErrorClass.Transient)]
    public void Sqlstate_maps_to_class(string? sqlState, WriteErrorClass expected) =>
        Assert.Equal(expected, WriteErrorClassifier.Classify(sqlState));

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

    [Fact]
    public void Unknown_exception_is_transient_so_nothing_is_lost()
    {
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(new InvalidOperationException()));
    }
}
