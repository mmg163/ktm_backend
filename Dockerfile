# Data Saver (ADR-0002). Контекст сборки - КОРЕНЬ репозитория (Core.csproj встраивает схему из docs/api, design Р-10а):
#   docker build -t ktm-data-saver .
# Версии образов - линия .NET 10 (net10.0, src/Ktm/Directory.Build.props).

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Слой восстановления пакетов: только файлы проектов и общие props.
COPY src/Ktm/Directory.Build.props src/Ktm/Directory.Build.targets src/Ktm/Directory.Packages.props src/Ktm/
COPY src/Ktm/Ktm.HostedServices/Ktm.HostedServices.csproj src/Ktm/Ktm.HostedServices/
COPY src/Ktm/Ktm.DataSaver.Application/Ktm.DataSaver.Application.csproj src/Ktm/Ktm.DataSaver.Application/
COPY src/Ktm/Ktm.DataSaver.Infrastructure/Ktm.DataSaver.Infrastructure.csproj src/Ktm/Ktm.DataSaver.Infrastructure/
COPY src/Ktm/Ktm.Shared/Ktm.Shared.csproj src/Ktm/Ktm.Shared/
COPY src/Ktm/Ktm.Binding.Application/Ktm.Binding.Application.csproj src/Ktm/Ktm.Binding.Application/
COPY src/Ktm/Ktm.Binding.Infrastructure/Ktm.Binding.Infrastructure.csproj src/Ktm/Ktm.Binding.Infrastructure/
COPY src/Ktm/Ktm.Interpretation.Application/Ktm.Interpretation.Application.csproj src/Ktm/Ktm.Interpretation.Application/
COPY src/Ktm/Ktm.Interpretation.Infrastructure/Ktm.Interpretation.Infrastructure.csproj src/Ktm/Ktm.Interpretation.Infrastructure/
RUN dotnet restore src/Ktm/Ktm.HostedServices/Ktm.HostedServices.csproj

COPY docs/api docs/api
COPY src/Ktm/Ktm.HostedServices src/Ktm/Ktm.HostedServices
COPY src/Ktm/Ktm.DataSaver.Application src/Ktm/Ktm.DataSaver.Application
COPY src/Ktm/Ktm.DataSaver.Infrastructure src/Ktm/Ktm.DataSaver.Infrastructure
COPY src/Ktm/Ktm.Shared src/Ktm/Ktm.Shared
COPY src/Ktm/Ktm.Binding.Application src/Ktm/Ktm.Binding.Application
COPY src/Ktm/Ktm.Binding.Infrastructure src/Ktm/Ktm.Binding.Infrastructure
COPY src/Ktm/Ktm.Interpretation.Application src/Ktm/Ktm.Interpretation.Application
COPY src/Ktm/Ktm.Interpretation.Infrastructure src/Ktm/Ktm.Interpretation.Infrastructure
RUN dotnet publish src/Ktm/Ktm.HostedServices/Ktm.HostedServices.csproj \
    --no-restore -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

# Порт health (DataSaver__Health__Port, по умолчанию 8080); приложение слушает его само.
EXPOSE 8080

# Непривилегированный пользователь `app` из базового образа.
USER $APP_UID
ENTRYPOINT ["dotnet", "Ktm.HostedServices.dll"]
