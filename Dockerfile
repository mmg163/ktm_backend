# Data Saver (ADR-0002). Контекст сборки - КОРЕНЬ репозитория (Core.csproj встраивает схему из docs/api, design Р-10а):
#   docker build -t ktm-data-saver .
# Версии образов - линия .NET 10 (net10.0, src/Ktm/Directory.Build.props).

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Слой восстановления пакетов: только файлы проектов и общие props.
COPY src/Ktm/Directory.Build.props src/Ktm/Directory.Build.targets src/Ktm/Directory.Packages.props src/Ktm/
COPY src/Ktm/Ktm.DataSaver/Ktm.DataSaver.csproj src/Ktm/Ktm.DataSaver/
COPY src/Ktm/Ktm.DataSaver.Core/Ktm.DataSaver.Core.csproj src/Ktm/Ktm.DataSaver.Core/
COPY src/Ktm/Ktm.DataSaver.Infrastructure/Ktm.DataSaver.Infrastructure.csproj src/Ktm/Ktm.DataSaver.Infrastructure/
RUN dotnet restore src/Ktm/Ktm.DataSaver/Ktm.DataSaver.csproj

COPY docs/api docs/api
COPY src/Ktm/Ktm.DataSaver src/Ktm/Ktm.DataSaver
COPY src/Ktm/Ktm.DataSaver.Core src/Ktm/Ktm.DataSaver.Core
COPY src/Ktm/Ktm.DataSaver.Infrastructure src/Ktm/Ktm.DataSaver.Infrastructure
RUN dotnet publish src/Ktm/Ktm.DataSaver/Ktm.DataSaver.csproj \
    --no-restore -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

# Порт health (DataSaver__Health__Port, по умолчанию 8080); приложение слушает его само.
EXPOSE 8080

# Непривилегированный пользователь `app` из базового образа.
USER $APP_UID
ENTRYPOINT ["dotnet", "Ktm.DataSaver.dll"]
