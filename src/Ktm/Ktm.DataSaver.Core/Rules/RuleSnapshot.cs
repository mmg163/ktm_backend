using System.Security.Cryptography;
using System.Text;

namespace Ktm.DataSaver.Core.Rules;

/// <summary>Правило, не вошедшее в снимок. <c>Reason</c> не содержит значений из сообщений (при загрузке их нет).</summary>
public sealed record RuleSnapshotError(string? RuleId, int? RuleVersion, string Reason);

/// <summary>Идентификатор и версия правила.</summary>
public sealed record RuleRef(string Id, int Version);

/// <summary>Отличия снимка от прежнего по всем строкам источника (включая выключенные).</summary>
public sealed record RuleSnapshotDiff(
    IReadOnlyList<RuleRef> Added,
    IReadOnlyList<RuleRef> Changed,
    IReadOnlyList<RuleRef> Removed,
    IReadOnlyList<RuleRef> ChangedWithoutVersionIncrease)
{
    public static RuleSnapshotDiff None { get; } = new([], [], [], []);
}

/// <summary>
/// Неизменяемый набор правил, готовый к вычислению: индекс «источник -> правила источника и общие» в порядке
/// <c>priority</c>, затем <c>rule_id</c> (ordinal); ссылка на снимок заменяется целиком (ADR-0005 п. 2.8, 2.10).
/// </summary>
public sealed class RuleSnapshot
{
    private readonly Dictionary<string, CompiledRule[]> _bySource;
    private readonly CompiledRule[] _common;

    internal RuleSnapshot(
        string? revision,
        CompiledRule[] rules,
        Dictionary<string, CompiledRule[]> bySource,
        CompiledRule[] common,
        IReadOnlyList<RuleSnapshotError> errors,
        Dictionary<string, RuleFingerprint> fingerprints,
        RuleSnapshotDiff changes)
    {
        Revision = revision;
        Rules = rules;
        _bySource = bySource;
        _common = common;
        Errors = errors;
        Fingerprints = fingerprints;
        Changes = changes;
    }

    /// <summary>Пустой снимок до первой загрузки: ни одно сообщение не отбраковывается.</summary>
    public static RuleSnapshot Empty { get; } = new(null, [], new(StringComparer.Ordinal), [], [], new(StringComparer.Ordinal), RuleSnapshotDiff.None);

    /// <summary>Ревизия источника (непрозрачная строка); <c>null</c> - загрузки ещё не было.</summary>
    public string? Revision { get; }

    /// <summary>Применяемые правила (включённые и скомпилированные) в порядке вычисления.</summary>
    public IReadOnlyList<CompiledRule> Rules { get; }

    /// <summary>Включённые правила, не вошедшие в снимок, и проблемы источника.</summary>
    public IReadOnlyList<RuleSnapshotError> Errors { get; }

    /// <summary>Изменения относительно предыдущего снимка.</summary>
    public RuleSnapshotDiff Changes { get; }

    internal Dictionary<string, RuleFingerprint> Fingerprints { get; }

    /// <summary>Правила для источника события: свои и общие в едином порядке; для неизвестного источника - только общие.</summary>
    public ReadOnlySpan<CompiledRule> RulesFor(string source) => _bySource.TryGetValue(source, out var rules) ? rules : _common;
}

internal readonly record struct RuleFingerprint(int Version, string Hash);

/// <summary>Сборка снимка: правила компилируются по отдельности, ошибочное не влияет на остальные.</summary>
public sealed class RuleSnapshotBuilder(
    ConditionCompiler compiler,
    int maxRules = RuleSnapshotBuilder.DefaultMaxRules,
    int maxRegexNodes = RuleSnapshotBuilder.DefaultMaxRegexNodes)
{
    public const int DefaultMaxRules = 500;
    /// <summary>Единственное место дефолта предела узлов <c>regex</c> в наборе; <c>RulesOptions.MaxRegexNodes</c> берёт его отсюда.</summary>
    public const int DefaultMaxRegexNodes = 64;

    public RuleSnapshot Build(
        string revision,
        IReadOnlyList<RuleDefinition> rules,
        IReadOnlyList<RuleProblem> problems,
        RuleSnapshot? previous = null)
    {
        var errors = new List<RuleSnapshotError>();
        foreach (var problem in problems)
        {
            errors.Add(new RuleSnapshotError(problem.RuleId, problem.RuleVersion,RuleErrors.Format(problem.Location, problem.Message)));
        }

        var fingerprints = new Dictionary<string, RuleFingerprint>(StringComparer.Ordinal);
        var unique = new List<RuleDefinition>(rules.Count);
        foreach (var rule in rules)
        {
            if (fingerprints.TryAdd(rule.Id, new RuleFingerprint(rule.Version, Hash(rule))))
            {
                unique.Add(rule);
            }
            else
            {
                errors.Add(new RuleSnapshotError(rule.Id, rule.Version, "rule_id повторяется в наборе: правило не применяется"));
            }
        }

        var enabled = unique.Where(r => r.Enabled).ToList();
        enabled.Sort(static (a, b) => a.Priority != b.Priority ? a.Priority.CompareTo(b.Priority) : string.CompareOrdinal(a.Id, b.Id));

        var compiled = new List<CompiledRule>(Math.Min(enabled.Count, maxRules));
        var regexNodes = 0;
        foreach (var rule in enabled)
        {
            if (compiled.Count >= maxRules)
            {
                errors.Add(new RuleSnapshotError(rule.Id, rule.Version, $"превышен предел MaxRules={maxRules}: правило не применяется"));
                continue;
            }

            var result = compiler.Compile(rule);
            if (result.Succeeded && regexNodes + result.Rule!.RegexNodeCount > maxRegexNodes)
            {
                // Правило, на котором исчерпан предел набора, не применяется; следующие с меньшим числом узлов могут поместиться.
                errors.Add(new RuleSnapshotError(rule.Id, rule.Version, $"превышен предел MaxRegexNodes={maxRegexNodes}: правило не применяется"));
            }
            else if (result.Succeeded)
            {
                regexNodes += result.Rule!.RegexNodeCount;
                compiled.Add(result.Rule);
            }
            else
            {
                errors.Add(new RuleSnapshotError(rule.Id, rule.Version, string.Join("; ", result.Errors)));
            }
        }

        var all = compiled.ToArray();
        var common = all.Where(r => r.Definition.Source is null).ToArray();
        var bySource = RuleDefinition.KnownSources.ToDictionary(
            s => s,
            s => all.Where(r => r.Definition.Source is null || r.Definition.Source == s).ToArray(),
            StringComparer.Ordinal);

        return new RuleSnapshot(revision, all, bySource, common, errors, fingerprints, Diff(previous?.Fingerprints, fingerprints));
    }

    private static RuleSnapshotDiff Diff(Dictionary<string, RuleFingerprint>? old, Dictionary<string, RuleFingerprint> now)
    {
        old ??= [];
        var added = new List<RuleRef>();
        var changed = new List<RuleRef>();
        var removed = new List<RuleRef>();
        var noBump = new List<RuleRef>();

        foreach (var (id, fp) in now)
        {
            if (!old.TryGetValue(id, out var before))
            {
                added.Add(new RuleRef(id, fp.Version));
            }
            else if (before.Version != fp.Version || before.Hash != fp.Hash)
            {
                changed.Add(new RuleRef(id, fp.Version));
                if (before.Version == fp.Version)
                {
                    noBump.Add(new RuleRef(id, fp.Version));
                }
            }
        }

        foreach (var (id, fp) in old)
        {
            if (!now.ContainsKey(id))
            {
                removed.Add(new RuleRef(id, fp.Version));
            }
        }

        return new RuleSnapshotDiff(Ordered(added), Ordered(changed), Ordered(removed), Ordered(noBump));
    }

    private static List<RuleRef> Ordered(List<RuleRef> list)
    {
        list.Sort(static (a, b) => string.CompareOrdinal(a.Id, b.Id));
        return list;
    }

    /// <summary>Хэш канонической записи правила (все поля, условие - как есть в источнике).</summary>
    private static string Hash(RuleDefinition r)
    {
        var canonical = string.Join(
            '\u0001', r.Id, r.Name, r.Description ?? string.Empty, r.Source ?? "*", r.Enabled ? "1" : "0",
            r.Priority, r.Version, r.ReasonCode, r.Condition.GetRawText());
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
    }
}

/// <summary>Читатель текущего снимка (стадия берёт ссылку один раз на сообщение).</summary>
public interface IRuleSnapshotProvider
{
    RuleSnapshot Current { get; }
}

/// <summary>Держатель снимка: атомарная замена ссылки, отметка первой загрузки и последней успешной проверки источника.</summary>
public sealed class RuleSnapshotHolder : IRuleSnapshotProvider
{
    private volatile RuleSnapshot _current = RuleSnapshot.Empty;
    private long _lastCheckTicks = long.MinValue;

    public RuleSnapshot Current => _current;

    /// <summary>Первая успешная загрузка выполнена (пустая таблица - тоже успешная).</summary>
    public bool HasLoaded => Volatile.Read(ref _lastCheckTicks) != long.MinValue;

    /// <summary>Момент последней успешной проверки источника (<c>Unchanged</c> тоже успешная); <c>null</c> до первой.</summary>
    public DateTimeOffset? LastSuccessfulCheck =>
        Volatile.Read(ref _lastCheckTicks) is var t && t != long.MinValue ? new DateTimeOffset(t, TimeSpan.Zero) : null;

    public void Replace(RuleSnapshot snapshot, DateTimeOffset checkedAt)
    {
        _current = snapshot;
        MarkChecked(checkedAt);
    }

    public void MarkChecked(DateTimeOffset checkedAt) => Volatile.Write(ref _lastCheckTicks, checkedAt.UtcTicks);
}
