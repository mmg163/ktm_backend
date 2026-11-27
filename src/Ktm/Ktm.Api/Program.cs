using Ktm.Shared;
using Microsoft.Extensions.Options;
using Ktm.Directory.Infrastructure;
using Ktm.Identity.Infrastructure;
using Ktm.HostedServices;

var builder = WebApplication.CreateBuilder(args);

// Фиксированное смещение используется только для местных календарных дат.
builder.Services.AddOptions<TimeOptions>()
    .Bind(builder.Configuration.GetSection("Time"))
    .Validate(options => options.UtcOffsetMinutes is >= -840 and <= 840,
        "Time:UtcOffsetMinutes must be between -840 and 840.")
    .ValidateOnStart();

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddProblemDetails();
builder.Services.AddSingleton(TimeProvider.System);
if (builder.Configuration.GetSection("Ktm:Api:Postgres").Exists())
{
    builder.Services.AddModuleDatabase<DirectoryDbContext>(builder.Configuration, "Ktm:Api:Postgres");
    builder.Services.AddModuleDatabase<IdentityDbContext>(builder.Configuration, "Ktm:Api:Postgres");
}

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseExceptionHandler();
app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
    {
        var utcOffset = TimeSpan.FromMinutes(app.Services.GetRequiredService<IOptions<TimeOptions>>().Value.UtcOffsetMinutes);
        var localToday = DateOnly.FromDateTime(app.Services.GetRequiredService<TimeProvider>().GetUtcNow().ToOffset(utcOffset).DateTime);
        var forecast = Enumerable.Range(1, 5).Select(index =>
                new WeatherForecast
                (
                    localToday.AddDays(index),
                    Random.Shared.Next(-20, 55),
                    summaries[Random.Shared.Next(summaries.Length)]
                ))
            .ToArray();
        return forecast;
    })
    .WithName("GetWeatherForecast");

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}

public partial class Program;
