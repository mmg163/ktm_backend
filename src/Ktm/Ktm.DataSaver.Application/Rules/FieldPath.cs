using System.Globalization;
using System.Text.Json;

namespace Ktm.DataSaver.Core.Rules;

/// <summary>
/// Путь к полю тела сообщения (грамматика <c>var</c> JsonLogic, ADR-0005 п. 2.5): сегменты через точку,
/// сегмент <c>[A-Za-z0-9_-]+</c>, регистр значим; сегмент из цифр у массива - индекс элемента, у объекта - имя свойства.
/// Первый сегмент - поле конверта формата v1. Вычисление не выделяет память.
/// </summary>
public sealed class FieldPath
{
    public const int MaxLength = 256;

    /// <summary>Поля конверта формата v1 (kafka-event-format.md); внутри <c>payload</c> и <c>correction</c> структура свободна.</summary>
    public static IReadOnlyList<string> EnvelopeFields { get; } =
        ["schemaVersion", "source", "sourceEventId", "occurredAt", "eventType", "deviceId", "externalPersonId", "payload", "correction"];

    private readonly Segment[] _segments;

    private FieldPath(string text, Segment[] segments)
    {
        Text = text;
        _segments = segments;
    }

    /// <summary>Путь в исходной записи.</summary>
    public string Text { get; }

    /// <summary>Разбирает путь; при ошибке <paramref name="error"/> содержит причину (без значений из сообщений).</summary>
    public static bool TryParse(string? text, out FieldPath? path, out string? error)
    {
        path = null;
        if (string.IsNullOrEmpty(text))
        {
            error = "путь не может быть пустым";
            return false;
        }

        if (text.Length > MaxLength)
        {
            error = $"путь длиннее {MaxLength} символов";
            return false;
        }

        var parts = text.Split('.');
        var segments = new Segment[parts.Length];
        for (var i = 0; i < parts.Length; i++)
        {
            var part = parts[i];
            if (part.Length == 0)
            {
                error = $"пустой сегмент в пути \"{text}\"";
                return false;
            }

            foreach (var c in part)
            {
                if (!(c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '_' or '-'))
                {
                    error = $"недопустимый символ в пути \"{text}\" (разрешены буквы, цифры, '_', '-' и разделитель '.')";
                    return false;
                }
            }

            var index = -1;
            if (IsDigits(part) && int.TryParse(part, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
            {
                index = parsed;
            }

            segments[i] = new Segment(part, index);
        }

        if (!EnvelopeFields.Contains(parts[0], StringComparer.Ordinal))
        {
            error = $"неизвестное поле \"{parts[0]}\" (допустимы: {string.Join(", ", EnvelopeFields)})";
            return false;
        }

        path = new FieldPath(text, segments);
        error = null;
        return true;
    }

    /// <summary>
    /// Находит значение по пути в теле сообщения. <c>false</c> - поля нет (нет свойства, путь идёт через не-объект,
    /// индекс за границей); JSON <c>null</c> - это присутствующее значение.
    /// </summary>
    public bool TryGet(JsonElement body, out JsonElement value)
    {
        var current = body;
        foreach (var segment in _segments)
        {
            JsonElement next;
            switch (current.ValueKind)
            {
                case JsonValueKind.Object:
                    if (!current.TryGetProperty(segment.Name, out next))
                    {
                        value = default;
                        return false;
                    }

                    break;
                case JsonValueKind.Array when segment.Index >= 0:
                    if (segment.Index >= current.GetArrayLength())
                    {
                        value = default;
                        return false;
                    }

                    next = current[segment.Index];
                    break;
                default:
                    value = default;
                    return false;
            }

            current = next;
        }

        value = current;
        return true;
    }

    private static bool IsDigits(string s)
    {
        foreach (var c in s)
        {
            if (c is < '0' or > '9') return false;
        }

        return true;
    }

    private readonly record struct Segment(string Name, int Index);
}
