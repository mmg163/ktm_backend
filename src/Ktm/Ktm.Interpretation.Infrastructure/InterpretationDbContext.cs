using Microsoft.EntityFrameworkCore;

namespace Ktm.Interpretation.Infrastructure;

public sealed class InterpretationDbContext(DbContextOptions<InterpretationDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("ktm");
    }
}
