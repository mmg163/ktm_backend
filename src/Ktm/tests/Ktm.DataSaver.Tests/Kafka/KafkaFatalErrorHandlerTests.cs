using Confluent.Kafka;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ktm.DataSaver.Tests.Kafka;

public sealed class KafkaFatalErrorHandlerTests
{
    private sealed class RecordingTerminator : IProcessTerminator
    {
        public List<int> ExitCodes { get; } = [];

        public void Terminate(int exitCode, string reason) => ExitCodes.Add(exitCode);
    }

    [Fact]
    public void Non_fatal_error_does_not_terminate_process()
    {
        var terminator = new RecordingTerminator();
        var handler = new KafkaFatalErrorHandler(terminator, NullLogger<KafkaFatalErrorHandler>.Instance, new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval));

        var fatal = handler.Handle(new Error(ErrorCode.Local_Transport, "broker down", isFatal: false));

        Assert.False(fatal);
        Assert.Empty(terminator.ExitCodes);
    }

    [Fact]
    public void Fatal_error_terminates_process_once_with_non_zero_code()
    {
        var terminator = new RecordingTerminator();
        var handler = new KafkaFatalErrorHandler(terminator, NullLogger<KafkaFatalErrorHandler>.Instance, new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval));
        var error = new Error(ErrorCode.Local_Fatal, "fenced", isFatal: true);

        Assert.True(handler.Handle(error));
        Assert.True(handler.Handle(error));

        var code = Assert.Single(terminator.ExitCodes);
        Assert.NotEqual(0, code);
    }
}
