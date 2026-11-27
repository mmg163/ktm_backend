namespace Ktm.Shared;

/// <summary>Общая ошибка прикладного сценария со стабильным кодом.</summary>
public sealed record ApplicationError(string Code, string Category);
