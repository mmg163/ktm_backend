using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ktm.DataSaver.Admin;

/// <summary>
/// Раздел admin-области <c>/admin/v1/{Id}</c> (ADR-0006 п. 2.6). Новый раздел - это класс, реализующий интерфейс, и строка
/// <see cref="AdminSectionRegistration.AddAdminSection{TSection}"/>: инфраструктура области (обработчик ошибок, тег OpenAPI,
/// список разделов) не меняется. Реестр разделов - зарегистрированные в контейнере реализации.
/// </summary>
public interface IAdminSection
{
    /// <summary>Идентификатор раздела: сегмент пути и тег OpenAPI; латиница в нижнем регистре, цифры и дефис.</summary>
    string Id { get; }

    /// <summary>Название для меню админки.</summary>
    string Title { get; }

    /// <summary>Регистрирует маршруты раздела в группе <c>/admin/v1/{Id}</c> (тег и обработчик ошибок уже применены).</summary>
    void MapEndpoints(RouteGroupBuilder group);
}

/// <summary>Элемент ответа <c>GET /admin/v1/sections</c>.</summary>
/// <param name="Id">Идентификатор раздела (совпадает с идентификатором клиентского модуля админки).</param>
/// <param name="Title">Название раздела для меню.</param>
/// <param name="ApiBase">Базовый путь API раздела.</param>
public sealed record AdminSectionInfo(string Id, string Title, string ApiBase);

public static class AdminSectionRegistration
{
    /// <summary>Добавляет раздел в реестр admin-области.</summary>
    public static IServiceCollection AddAdminSection<TSection>(this IServiceCollection services)
        where TSection : class, IAdminSection
    {
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IAdminSection, TSection>());
        return services;
    }

    /// <summary>
    /// Разделы, поставляемые сервисом: единое место регистрации для composition root и тестов (снимок OpenAPI в <c>docs/api</c>
    /// строится по этому списку). Новый раздел - строка здесь.
    /// </summary>
    public static IServiceCollection AddBuiltInAdminSections(this IServiceCollection services) =>
        services.AddAdminSection<Reprocessing.ReprocessingSection>();
}
