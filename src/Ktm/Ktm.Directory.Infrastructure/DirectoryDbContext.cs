using Microsoft.EntityFrameworkCore;

namespace Ktm.Directory.Infrastructure;

public sealed class DirectoryDbContext(DbContextOptions<DirectoryDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("ktm");
    }
}
