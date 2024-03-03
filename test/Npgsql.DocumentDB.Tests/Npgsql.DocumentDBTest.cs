namespace Npgsql.DocumentDB.Tests;

public class NpgsqlDocumentDBTest(NpgsqlDocumentDB kv)
{
    public class Poco {
        public string? Value { get; set; }
    }

    [Fact]
    public void BasicTest()
    {
        kv.Set("BasicTest1", new Poco { Value = "BasicTest1" });
        var poco = kv.Get<Poco>("BasicTest1");
        Assert.Equal("BasicTest1", poco?.Value);
    }
    
    [Fact]
    public void NonExistingKeyTest()
    {
        var value = kv.Get<Poco>("NonExistingKeyTest");
        Assert.Null(value);
    }

    [Fact]
    public void DuplicateKeyTest()
    {
        kv.Set("DuplicateKeyTest1", new Poco { Value = "DuplicateKeyTest1" });
        var ex = Assert.Throws<PostgresException>(() => {
            kv.Set("DuplicateKeyTest1", new Poco { Value = "DuplicateKeyTest1" });
        });
        Assert.Equal("23505", ex.SqlState);
    }
}