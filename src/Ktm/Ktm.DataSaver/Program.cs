using Ktm.DataSaver;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHostedService<DataSaverWorker>();

var app = builder.Build();
await app.RunAsync();
