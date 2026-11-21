using Ktm.DataSaver.Infrastructure.Postgres;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Задача 5.6: разбор настроек подключения, пароль не попадает в логи и исключения; временный провайдер доверия.</summary>
public sealed class PostgresOptionsTests
{
    private const string Secret = "S3cr3t-P@ss!";

    private static PostgresOptions Valid() => new() { Host = "db", Database = "ktm", Username = "ktm_app", Password = Secret };

    [Fact]
    public void Password_from_file_is_applied_with_pool_timeout_and_keepalive_and_never_printed()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, Secret + "\n");
            var options = Valid();
            options.Password = null;
            options.PasswordFile = file;
            options.CommandTimeoutSeconds = 45;
            options.MaxPoolSize = 7;
            options.MinPoolSize = 2;
            options.KeepAliveSeconds = 20;

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
    public void Invalid_or_incomplete_options_fail_startup_without_leaking_password()
    {
        var both = Valid();
        both.PasswordFile = "secret-file";
        var badPool = Valid();
        badPool.MinPoolSize = 5;
        badPool.MaxPoolSize = 2;
        var noPassword = Valid();
        noPassword.Password = null;
        var noHost = Valid();
        noHost.Host = "";
        var noDatabase = Valid();
        noDatabase.Database = "";
        var noUser = Valid();
        noUser.Username = "";
        var badSsl = Valid();
        badSsl.SslMode = "Sometimes";

        foreach (var options in new[] { both, badPool, noPassword, noHost, noDatabase, noUser, badSsl })
        {
            var ex = Assert.Throws<InvalidOperationException>(() => options.BuildConnectionString());
            Assert.DoesNotContain(Secret, ex.Message);
            Assert.DoesNotContain(Secret, options.ToString());
        }

        Assert.Contains("Allowed values", Assert.Throws<InvalidOperationException>(() => badSsl.BuildConnectionString()).Message);
        var missingFile = Valid();
        missingFile.Password = null;
        missingFile.PasswordFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        Assert.Throws<InvalidOperationException>(() => missingFile.BuildConnectionString());
    }

    [Theory]
    [InlineData(null, SslMode.Require)]
    [InlineData("Disable", SslMode.Disable)]
    [InlineData("prefer", SslMode.Prefer)]
    [InlineData("VerifyCA", SslMode.VerifyCA)]
    [InlineData("VerifyFull", SslMode.VerifyFull)]
    public void SslMode_defaults_to_Require_and_parses_allowed_values(string? value, SslMode expected)
    {
        var options = Valid();
        if (value is not null) options.SslMode = value;

        Assert.Equal(expected, new NpgsqlConnectionStringBuilder(options.BuildConnectionString()).SslMode);
    }

    [Fact]
    public void Trust_defaults_to_100_and_per_source_overrides_apply()
    {
        var provider = new EnvironmentSourceTrustProvider(new Dictionary<string, string?>
        {
            [EnvironmentSourceTrustProvider.SourcePrefix + "1c-do"] = "60",
            ["Unrelated"] = "x",
        });

        Assert.Equal((short)60, provider.GetTrust("1c-do"));
        Assert.Equal((short)100, provider.GetTrust("skud"));
    }

    [Theory]
    [InlineData("DataSaver__Trust__Default", "101")]
    [InlineData("DataSaver__Trust__Source__skud", "abc")]
    [InlineData("DataSaver__Trust__Source__skud", "-1")]
    public void Invalid_trust_value_fails_at_startup_not_at_write(string name, string value)
    {
        var ex = Assert.Throws<InvalidSettingException>(
            () => new EnvironmentSourceTrustProvider(new Dictionary<string, string?> { [name] = value }));

        Assert.Contains(name, ex.Message);
    }
}
