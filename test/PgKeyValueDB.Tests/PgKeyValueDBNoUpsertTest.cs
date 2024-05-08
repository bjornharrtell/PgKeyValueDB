using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Wololo.PgKeyValueDB.Tests;

public class PgKeyValueDBNoUpsertTest([FromKeyedServices("noupsert")] PgKeyValueDB kv)
{
    public class Poco
    {
        public string? Value { get; set; }
    }

    [Fact]
    public void DuplicateKeyTest()
    {
        var key = nameof(DuplicateKeyTest);
        var pid = nameof(DuplicateKeyTest);
        kv.Set(key, new Poco { Value = key }, pid);
        var ex = Record.Exception(() => {
            kv.Set(key, new Poco { Value = key }, pid);
        });
        Assert.IsType<PostgresException>(ex);
        Assert.Contains("23505", ex.Message);
    }
}