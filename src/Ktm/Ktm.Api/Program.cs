var builder = WebApplication.CreateBuilder(args);

// Фиксированное смещение используется только для местных календарных дат.
var offsetMinutes = builder.Configuration.GetValue<int>("Time:UtcOffsetMinutes");
if (offsetMinutes is < -840 or > 840)
{
    throw new InvalidOperationException("Time:UtcOffsetMinutes must be between -840 and 840.");
}
var utcOffset = TimeSpan.FromMinutes(offsetMinutes);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
    {
        var localToday = DateOnly.FromDateTime(DateTimeOffset.UtcNow.ToOffset(utcOffset).DateTime);
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
