using System.Net.Sockets;
using System.Text.Json;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Resilience;
using Ktm.DataSaver.Infrastructure.Postgres;
using Npgsql;

namespace Ktm.DataSaver.Tests.Postgres;

/// <summary>M5: исключения при работе с соединением не должны попадать в Unexpected (здоровые сообщения не уходят в dlq). БД не нужна.</summary>
public class NpgsqlEventStoreFailureTests
{
    private sealed class FixedTrust : ISourceTrustProvider
    {
        public short GetTrust(string source) => 100;
    }

    private static ParsedEvent Event(int schemaVersion = 1)
    {
        using var document = JsonDocument.Parse("""{"a":1}""");
        var body = document.RootElement.Clone();
        return new ParsedEvent(schemaVersion, "scud", "e-1", new DateTimeOffset(2026, 3, 1, 12, 0, 0, TimeSpan.Zero),
            "access", "dev", "p-1", body, null, body);
    }

    private static readonly DateTimeOffset Now = new(2026, 3, 1, 12, 0, 1, TimeSpan.Zero);

    public static TheoryData<Exception> InfrastructureExceptions => new()
    {
        new InvalidOperationException("connection is not open"),
        new ObjectDisposedException("transaction"),
        new IOException("broken pipe"),
        new SocketException(),
        new TimeoutException(),
        new NotSupportedException(),
        new Exception("anything"),
    };

    public static TheoryData<Exception> MessageBoundExceptions => new()
    {
        new InvalidCastException(),
        new FormatException(),
        new OverflowException(),
        new ArgumentOutOfRangeException("p"),
        new ArgumentException("bad"),
    };

    [Theory]
    [MemberData(nameof(InfrastructureExceptions))]
    public void Connection_section_exception_becomes_transient_without_copying_its_text(Exception original)
    {
        Assert.False(NpgsqlEventStore.IsMessageBound(original));

        var adapted = PostgresStoreException.ConnectionUnavailable(original);

        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(adapted));
        Assert.Same(original, adapted.InnerException);
        Assert.DoesNotContain("broken pipe", adapted.Message);
        Assert.DoesNotContain("connection is not open", adapted.Message);
        Assert.DoesNotContain("anything", adapted.Message);
        Assert.Contains(original.GetType().Name, adapted.Message);
    }

    [Theory]
    [MemberData(nameof(MessageBoundExceptions))]
    public void Value_errors_stay_message_bound_and_unexpected(Exception original)
    {
        Assert.True(NpgsqlEventStore.IsMessageBound(original));
        Assert.Equal(WriteErrorClass.Unexpected, WriteErrorClassifier.Classify(original));
    }

    [Fact]
    public void Driver_errors_keep_their_class_and_cancellation_is_not_message_bound()
    {
        Assert.False(NpgsqlEventStore.IsMessageBound(new OperationCanceledException()));

        var withoutSqlState = PostgresStoreException.Adapt(new NpgsqlException("net", new SocketException()));
        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(withoutSqlState));

        var check = new PostgresException("secret value", "ERROR", "ERROR", "23514");
        Assert.Equal(WriteErrorClass.Data, WriteErrorClassifier.Classify(PostgresStoreException.Adapt(check)));
    }

    [Fact]
    public async Task Unreachable_database_is_transient()
    {
        await using var dataSource = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=u;Password=p;Database=d;Timeout=2;Pooling=false");
        var store = new NpgsqlEventStore(dataSource, new FixedTrust());

        var thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await store.WriteAsync([Event()], Now, CancellationToken.None));

        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(thrown));
    }

    [Fact]
    public async Task Disposed_data_source_is_transient()
    {
        var dataSource = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=u;Password=p;Database=d;Timeout=2");
        var store = new NpgsqlEventStore(dataSource, new FixedTrust());
        await dataSource.DisposeAsync();

        var thrown = await Assert.ThrowsAnyAsync<Exception>(async () => await store.WriteAsync([Event()], Now, CancellationToken.None));

        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(thrown));
    }

    [Fact]
    public async Task Value_mapping_error_is_raised_before_the_connection_is_opened_and_stays_unexpected()
    {
        // Схема v1 не помещается в smallint: ошибка отображения значения, а не отказ соединения (порт 1 недоступен: открытие дало бы Transient).
        await using var dataSource = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=u;Password=p;Database=d;Timeout=2;Pooling=false");
        var store = new NpgsqlEventStore(dataSource, new FixedTrust());

        var thrown = await Assert.ThrowsAsync<OverflowException>(async () => await store.WriteAsync([Event(schemaVersion: 70000)], Now, CancellationToken.None));

        Assert.Equal(WriteErrorClass.Unexpected, WriteErrorClassifier.Classify(thrown));
    }
}
