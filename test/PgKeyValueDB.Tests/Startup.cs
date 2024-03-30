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
        services.AddPgKeyValueDB($"Host=localhost;Port={pg.PgPort};Username=postgres;Password=postgres;Database=postgres");
    }

    public void Dispose()
    {
        pg?.Stop();
        pg?.Dispose();
    }
}
