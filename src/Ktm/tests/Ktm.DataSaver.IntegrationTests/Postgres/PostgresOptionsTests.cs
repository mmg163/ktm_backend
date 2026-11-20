using Ktm.DataSaver.Infrastructure.Postgres;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Задача 5.6: разрешение пароля и отсутствие его в тексте логов и исключений.</summary>
public sealed class PostgresOptionsTests
{
    private const string Secret = "S3cr3t-P@ss!";

    [Fact]
    public void Password_from_file_is_applied_with_pool_timeout_and_keepalive_and_never_printed()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, Secret + "\n");
            var options = new PostgresOptions
            {
                PasswordFile = file, CommandTimeoutSeconds = 45, MaxPoolSize = 7, MinPoolSize = 2, KeepAliveSeconds = 20,
            };

            var parsed = new NpgsqlConnectionStringBuilder(options.BuildConnectionString());

            Assert.Equal(Secret, parsed.Password);
            Assert.Equal(45, parsed.CommandTimeout);
            Assert.Equal(7, parsed.MaxPoolSize);
            Assert.Equal(2, parsed.MinPoolSize);
            Assert.Equal(20, parsed.KeepAlive);
            Assert.DoesNotContain(Secret, options.ToString());
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public void Invalid_options_fail_without_leaking_password()
    {
        var both = new PostgresOptions { Password = Secret, PasswordFile = "secret-file" };
        var badPool = new PostgresOptions { Password = Secret, MinPoolSize = 5, MaxPoolSize = 2 };
        var missingFile = new PostgresOptions { PasswordFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N")) };

        foreach (var options in new[] { both, badPool })
        {
            var ex = Assert.Throws<InvalidOperationException>(() => options.BuildConnectionString());
            Assert.DoesNotContain(Secret, ex.Message);
            Assert.DoesNotContain(Secret, options.ToString());
        }

        Assert.Throws<InvalidOperationException>(() => missingFile.BuildConnectionString());
    }
}
