namespace Ktm.DataSaver.Application.Rules;

public enum RuleRefreshOutcome
{
    Changed,
    Unchanged,
}

public sealed record RuleRefreshResult(RuleRefreshOutcome Outcome, RuleSnapshot Snapshot);

/// <summary>
/// Одна попытка загрузки: читает источник, собирает снимок и заменяет его атомарно. Сбой источника
/// (<see cref="RuleSourceException"/>) пробрасывается, прежний снимок остаётся действующим; логирование и повторы - у вызывающего.
/// Вызывать последовательно (не из двух потоков одновременно).
/// </summary>
public sealed class RuleSnapshotRefresher(IRuleSource source, RuleSnapshotBuilder builder, RuleSnapshotHolder holder, TimeProvider time)
{
    public async ValueTask<RuleRefreshResult> RefreshAsync(CancellationToken cancellationToken)
    {
        var current = holder.Current;
        var result = await source.ReadAsync(current.Revision, cancellationToken);

        if (result.Unchanged)
        {
            if (current.Revision is null)
            {
                throw new RuleSourceException(RuleSourceFailureKind.Invalid, "Источник вернул Unchanged на первой загрузке.");
            }

            holder.MarkChecked(time.GetUtcNow());
            return new RuleRefreshResult(RuleRefreshOutcome.Unchanged, current);
        }

        var snapshot = builder.Build(result.Revision, result.Rules, result.Problems, current);
        holder.Replace(snapshot, time.GetUtcNow());
        return new RuleRefreshResult(RuleRefreshOutcome.Changed, snapshot);
    }
}
