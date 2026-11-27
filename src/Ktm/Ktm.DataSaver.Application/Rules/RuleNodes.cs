using System.Text.Json;

namespace Ktm.DataSaver.Core.Rules;

/// <summary>
/// Листовая проверка, созданная оператором при загрузке правила. Потокобезопасна и не изменяется; вычисление не бросает исключений
/// на любых данных сообщения (единственное исключение - прерывание по пределу совпадения <c>regex</c>, его обрабатывает стадия правил).
/// </summary>
public interface IPredicate
{
    bool Evaluate(JsonElement body);

    /// <summary>Описание листа для трассировки: <c>путь операция</c> (например <c>payload.confidence &lt;</c>), без значений из сообщения.</summary>
    string Describe();
}

/// <summary>Скомпилированный узел условия. Результат всегда булев (трёхзначной логики нет).</summary>
public interface IConditionNode
{
    bool Evaluate(JsonElement body);

    /// <summary>
    /// Для узла, уже вернувшего истину: добавляет описания листьев, обеспечивших истину; <paramref name="prefix"/> -
    /// накопленные отрицания (<c>!</c>).
    /// </summary>
    void CollectFired(JsonElement body, List<string> sink, string prefix = "");

    /// <summary>
    /// Для узла, уже вернувшего ложь: добавляет описания листьев, которые сами были ложны (причина лжи узла).
    /// Используется под отрицанием.
    /// </summary>
    void CollectFailed(JsonElement body, List<string> sink, string prefix);
}

internal sealed class LeafNode(IPredicate predicate) : IConditionNode
{
    public bool Evaluate(JsonElement body) => predicate.Evaluate(body);

    public void CollectFired(JsonElement body, List<string> sink, string prefix = "") => sink.Add(prefix + predicate.Describe());

    public void CollectFailed(JsonElement body, List<string> sink, string prefix) => sink.Add(prefix + predicate.Describe());
}

internal sealed class AndNode(IConditionNode[] children) : IConditionNode
{
    public bool Evaluate(JsonElement body)
    {
        foreach (var child in children)
        {
            if (!child.Evaluate(body)) return false;
        }

        return true;
    }

    public void CollectFired(JsonElement body, List<string> sink, string prefix = "")
    {
        foreach (var child in children) child.CollectFired(body, sink, prefix);
    }

    public void CollectFailed(JsonElement body, List<string> sink, string prefix)
    {
        foreach (var child in children)
        {
            if (!child.Evaluate(body)) child.CollectFailed(body, sink, prefix);
        }
    }
}

internal sealed class OrNode(IConditionNode[] children) : IConditionNode
{
    public bool Evaluate(JsonElement body)
    {
        foreach (var child in children)
        {
            if (child.Evaluate(body)) return true;
        }

        return false;
    }

    public void CollectFired(JsonElement body, List<string> sink, string prefix = "")
    {
        foreach (var child in children)
        {
            if (child.Evaluate(body)) child.CollectFired(body, sink, prefix);
        }
    }

    // Ложный OR: ложны все операнды.
    public void CollectFailed(JsonElement body, List<string> sink, string prefix)
    {
        foreach (var child in children) child.CollectFailed(body, sink, prefix);
    }
}

internal sealed class NotNode(IConditionNode child) : IConditionNode
{
    public bool Evaluate(JsonElement body) => !child.Evaluate(body);

    // Узел истинен, значит вложенный ложен: перечисляем только листья, которые сами были ложны, с пометкой отрицания.
    public void CollectFired(JsonElement body, List<string> sink, string prefix = "") => child.CollectFailed(body, sink, prefix + "!");

    // Узел ложен, значит вложенный истинен: причина - его сработавшие листья.
    public void CollectFailed(JsonElement body, List<string> sink, string prefix) => child.CollectFired(body, sink, prefix + "!");
}

/// <summary>Скомпилированное правило: определение и корень условия.</summary>
public sealed class CompiledRule(RuleDefinition definition, IConditionNode root, int regexNodeCount = 0)
{
    public RuleDefinition Definition { get; } = definition;

    /// <summary>Число листьев <c>regex</c> в условии (для лимита на набор).</summary>
    public int RegexNodeCount { get; } = regexNodeCount;

    public bool Evaluate(JsonElement body) => root.Evaluate(body);

    /// <summary>Листья, обеспечившие срабатывание; вызывается только для правила, которое уже сработало (повторный проход).</summary>
    public IReadOnlyList<string> TraceFired(JsonElement body)
    {
        var sink = new List<string>();
        root.CollectFired(body, sink);
        return sink.Distinct(StringComparer.Ordinal).ToList();
    }
}
