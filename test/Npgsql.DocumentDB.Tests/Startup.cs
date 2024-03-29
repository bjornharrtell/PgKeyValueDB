using Microsoft.Extensions.DependencyInjection;
using MysticMind.PostgresEmbed;

namespace Npgsql.DocumentDB.Tests;

public class Startup : IDisposable
{
    private static PgServer? pg;

    public void ConfigureServices(IServiceCollection services)
    {
        pg = new PgServer("16.2.0", clearWorkingDirOnStart: true, clearInstanceDirOnStop: true);
        pg.Start();
        services.AddNpgsqlDocumentDB($"Host=localhost;Port={pg.PgPort};Username=postgres;Password=postgres;Database=postgres");
    }

    public void Dispose()
    {
        pg?.Stop();
        pg?.Dispose();
    }
}
