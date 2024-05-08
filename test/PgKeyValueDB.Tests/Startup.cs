using Microsoft.Extensions.DependencyInjection;
using MysticMind.PostgresEmbed;

namespace Wololo.PgKeyValueDB.Tests;

public class Startup : IDisposable
{
    private static PgServer? pg;

    public void ConfigureServices(IServiceCollection services)
    {
        pg = new PgServer("16.2.0", clearWorkingDirOnStart: true, clearInstanceDirOnStop: true);
        pg.Start();
        services.AddPgKeyValueDB($"Host=localhost;Port={pg.PgPort};Username=postgres;Password=postgres;Database=postgres", serviceKey: "standard");
        services.AddPgKeyValueDB($"Host=localhost;Port={pg.PgPort};Username=postgres;Password=postgres;Database=postgres", a => a.Upsert = false, serviceKey: "noupsert");
    }

    public void Dispose()
    {
        pg?.Stop();
        pg?.Dispose();
    }
}
