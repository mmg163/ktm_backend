using Ktm.DataSaver;
using Ktm.DataSaver.Health;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddDataSaver(builder.Configuration);

var app = builder.Build();
app.MapDataSaverHealth();
await app.RunAsync();

/// <summary>Точка входа для WebApplicationFactory в интеграционных тестах.</summary>
public partial class Program;
