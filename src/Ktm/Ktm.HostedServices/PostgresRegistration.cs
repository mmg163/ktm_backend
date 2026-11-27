using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Ktm.HostedServices;

public sealed class PostgresSettings
{
    public string Host { get; set; } = "";
    public int Port { get; set; } = 5432;
    public string Database { get; set; } = "";
    public string Username { get; set; } = "ktm_app";
    public string? Password { get; set; }
    public string? Password_FILE { get; set; }
}

public static class PostgresRegistration
{
    public static IServiceCollection AddModuleDatabase<TContext>(this IServiceCollection services, IConfiguration configuration, string sectionName)
        where TContext : DbContext
    {
        var section = configuration.GetSection(sectionName);
        services.AddOptions<PostgresSettings>(sectionName).Bind(section)
            .Validate(settings => !string.IsNullOrWhiteSpace(settings.Host) && settings.Port is > 0 and <= 65535
                && !string.IsNullOrWhiteSpace(settings.Database) && !string.IsNullOrWhiteSpace(settings.Username),
                $"{sectionName}: PostgreSQL host, port, database and username are required.")
            .Validate(settings => PasswordValid(settings), $"{sectionName}: specify a valid password or password file.")
            .ValidateOnStart();
        services.AddDbContext<TContext>((provider, builder) =>
        {
            var settings = provider.GetRequiredService<IOptionsMonitor<PostgresSettings>>().Get(sectionName);
            var password = string.IsNullOrWhiteSpace(settings.Password_FILE) ? settings.Password : File.ReadAllText(settings.Password_FILE).TrimEnd('\r', '\n');
            var connection = new NpgsqlConnectionStringBuilder
            {
                Host = settings.Host, Port = settings.Port, Database = settings.Database,
                Username = settings.Username, Password = password, Timezone = "UTC"
            };
            builder.UseNpgsql(connection.ConnectionString);
        });
        return services;
    }

    private static bool PasswordValid(PostgresSettings settings)
    {
        bool hasValue = !string.IsNullOrWhiteSpace(settings.Password);
        bool hasFile = !string.IsNullOrWhiteSpace(settings.Password_FILE);
        if (hasValue == hasFile) return false;
        if (!hasFile) return true;
        try { return !string.IsNullOrWhiteSpace(File.ReadAllText(settings.Password_FILE!)); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException) { return false; }
    }
}
