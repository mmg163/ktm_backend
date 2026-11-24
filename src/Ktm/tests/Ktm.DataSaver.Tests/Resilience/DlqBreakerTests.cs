using Ktm.DataSaver.Core.Resilience;

namespace Ktm.DataSaver.Tests.Resilience;

public class DlqBreakerTests
{
    [Fact]
    public void Opens_exactlyAtThreshold_AndClosesOnlyBySuccessfulWriteOfEvents()
    {
        var breaker = new DlqBreaker(3);

        breaker.Register(2);
        Assert.False(breaker.IsOpen);
        breaker.Register(1);
        Assert.True(breaker.IsOpen);
        breaker.Register(5);
        Assert.True(breaker.IsOpen);

        // Запись пустой пачки (одни отбракованные) - не успех записи.
        breaker.OnWriteSucceeded(0);
        Assert.True(breaker.IsOpen);

        breaker.OnWriteSucceeded(1);
        Assert.False(breaker.IsOpen);
        Assert.Equal(0, breaker.Count);
    }

    [Fact]
    public void ZeroOrNegativeRegistration_DoesNotChangeState_AndThresholdOneOpensOnFirstMessage()
    {
        var breaker = new DlqBreaker(2);
        breaker.Register(1);

        breaker.Register(0);
        breaker.Register(-3);

        Assert.Equal(1, breaker.Count);
        Assert.False(breaker.IsOpen);

        var strict = new DlqBreaker(1);
        strict.Register(1);
        Assert.True(strict.IsOpen);
    }
}
