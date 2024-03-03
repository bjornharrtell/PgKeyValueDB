namespace Npgsql.DocumentDB.Tests;

public class NpgsqlDocumentDBTest(NpgsqlDocumentDB kv)
{
    public class Poco {
        public string? Value { get; set; }
    }

    [Fact]
    public void BasicTest()
    {
        var key = nameof(BasicTest);
        kv.Set(key, new Poco { Value = key });
        var poco = kv.Get<Poco>(key);
        Assert.Equal(key, poco?.Value);
        var result = kv.Remove(key);
        Assert.True(result);
    }
    
    [Fact]
    public void NonExistingKeyGetTest()
    {
        var key = nameof(BasicTest);
        var value = kv.Get<Poco>(key);
        Assert.Null(value);
    }

    [Fact]
    public void NonExistingKeyRemoveTest()
    {
        var key = nameof(BasicTest);
        var result = kv.Remove(key);
        Assert.False(result);
    }

    [Fact]
    public void DuplicateKeyTest()
    {
        var key = nameof(DuplicateKeyTest);
        kv.Set(key, new Poco { Value = key });
        var ex = Assert.Throws<PostgresException>(() => {
            kv.Set(key, new Poco { Value = key });
        });
        Assert.Equal("23505", ex.SqlState);
    }
}