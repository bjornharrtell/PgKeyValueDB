namespace Wololo.PgKeyValueDB.Tests;

public class PgKeyValueDBTest(PgKeyValueDB kv)
{
    public class Poco
    {
        public string? Value { get; set; }
    }

    [Fact]
    public void BasicTest()
    {
        var key = nameof(BasicTest);
        kv.Set(key, new Poco { Value = key });
        var poco = kv.Get<Poco>(key);
        Assert.Equal(key, poco?.Value);
        var count1 = kv.Count();
        Assert.Equal(1, count1);
        var result = kv.Remove(key);
        Assert.True(result);
        var count2 = kv.Count();
        Assert.Equal(0, count2);
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
    public void RemoveAllTest()
    {
        var key = nameof(BasicTest);
        kv.Set(key, new Poco { Value = key });
        var result = kv.RemoveAll();
        Assert.Equal(1, result);
    }
}