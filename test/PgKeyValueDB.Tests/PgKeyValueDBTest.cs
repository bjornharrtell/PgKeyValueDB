using Npgsql;

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
        var pid = nameof(BasicTest);
        kv.Upsert(key, new Poco { Value = key }, pid);
        var poco = kv.Get<Poco>(key, pid);
        Assert.Equal(key, poco?.Value);
        var count1 = kv.Count(pid);
        Assert.Equal(1, count1);
        var result = kv.Remove(key, pid);
        Assert.True(result);
        var count2 = kv.Count(pid);
        Assert.Equal(0, count2);
    }

    [Fact]
    public void NonExistingKeyGetTest()
    {
        var key = nameof(NonExistingKeyGetTest);
        var pid = nameof(NonExistingKeyGetTest);
        var value = kv.Get<Poco>(key, pid);
        Assert.Null(value);
    }

    [Fact]
    public void NonExistingKeyRemoveTest()
    {
        var key = nameof(NonExistingKeyRemoveTest);
        var pid = nameof(NonExistingKeyRemoveTest);
        var result = kv.Remove(key ,pid);
        Assert.False(result);
    }

    [Fact]
    public void RemoveAllTest()
    {
        var key = nameof(RemoveAllTest);
        var pid = nameof(RemoveAllTest);
        kv.Upsert(key, new Poco { Value = key }, pid);
        var result = kv.RemoveAll(pid);
        Assert.Equal(1, result);
    }

    [Fact]
    public void RemoveAllExpiredTest()
    {
        var key = nameof(RemoveAllExpiredTest);
        var pid = nameof(RemoveAllExpiredTest);
        kv.Upsert(key, new Poco { Value = key }, pid);
        var result = kv.RemoveAllExpired(pid);
        Assert.Equal(0, result);
        kv.Upsert(key, new Poco { Value = key }, pid, DateTimeOffset.UtcNow.AddMinutes(-1));
        result = kv.RemoveAllExpired(pid);
        Assert.Equal(1, result);
    }

    [Fact]
    public void DuplicateKeyTest()
    {
        var key = nameof(DuplicateKeyTest);
        var pid = nameof(DuplicateKeyTest);
        kv.Create(key, new Poco { Value = key }, pid);
        var ex = Record.Exception(() => {
            kv.Create(key, new Poco { Value = key }, pid);
        });
        Assert.IsType<PostgresException>(ex);
        Assert.Contains("23505", ex.Message);
    }
}