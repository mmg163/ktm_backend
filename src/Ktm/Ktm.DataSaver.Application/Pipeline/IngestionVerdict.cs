namespace Ktm.DataSaver.Core.Pipeline;

/// <summary>Вердикт стадии по сообщению: <see cref="Accept"/> либо <see cref="Reject"/> с <see cref="RejectionInfo"/>.</summary>
public sealed record IngestionVerdict
{
    private IngestionVerdict() { }

    public static IngestionVerdict Accept { get; } = new();

    public RejectionInfo? Rejection { get; private init; }

    public bool IsAccepted => Rejection is null;

    public static IngestionVerdict Reject(RejectionInfo info)
    {
        ArgumentNullException.ThrowIfNull(info);
        return new IngestionVerdict { Rejection = info };
    }
}
