using Microsoft.EntityFrameworkCore;

namespace Ktm.Binding.Infrastructure;

public sealed class BindingDbContext(DbContextOptions<BindingDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("ktm");
    }
}
