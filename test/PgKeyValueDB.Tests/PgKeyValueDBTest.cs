using Microsoft.Extensions.DependencyInjection;
using MysticMind.PostgresEmbed;

namespace Wololo.PgKeyValueDB.Tests;

[TestClass]
public class PgKeyValueDBTest
{
    private static PgServer pg = null!;
    private static PgKeyValueDB kv = null!;

    [ClassInitialize]
    public static void ClassInit(TestContext context)
    {
        IServiceCollection services = new ServiceCollection();
        pg = new PgServer("16.2.0", clearWorkingDirOnStart: true, clearInstanceDirOnStop: true);
        pg.Start();
        services.AddPgKeyValueDB($"Host=localhost;Port={pg.PgPort};Username=postgres;Password=postgres;Database=postgres", b => {
            b.SchemaName = "pgkeyvaluetest";
            b.TableName = "pgkeyvaluetest";
        });
        var serviceProvider = services.BuildServiceProvider();
        kv = serviceProvider.GetRequiredService<PgKeyValueDB>();
    }

    [ClassCleanup()]
    public static void ClassCleanup()
    {
        pg?.Stop();
        pg?.Dispose();
    }

    public class Poco
    {
        public string? Value { get; set; }
    }

    [TestMethod]
    public void BasicTest()
    {
        var key = nameof(BasicTest);
        var pid = nameof(BasicTest);
        kv.Upsert(key, new Poco { Value = key }, pid);
        var poco = kv.Get<Poco>(key, pid);
        Assert.AreEqual(key, poco?.Value);
        var count1 = kv.Count(pid);
        Assert.AreEqual(1, count1);
        var result = kv.Remove(key, pid);
        Assert.IsTrue(result);
        var count2 = kv.Count(pid);
        Assert.AreEqual(0, count2);
    }

    [TestMethod]
    public void NonExistingKeyGetTest()
    {
        var key = nameof(NonExistingKeyGetTest);
        var pid = nameof(NonExistingKeyGetTest);
        var value = kv.Get<Poco>(key, pid);
        Assert.IsNull(value);
    }

    [TestMethod]
    public void NonExistingKeyRemoveTest()
    {
        var key = nameof(NonExistingKeyRemoveTest);
        var pid = nameof(NonExistingKeyRemoveTest);
        var result = kv.Remove(key, pid);
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void RemoveAllTest()
    {
        var key = nameof(RemoveAllTest);
        var pid = nameof(RemoveAllTest);
        kv.Upsert(key, new Poco { Value = key }, pid);
        var result = kv.RemoveAll(pid);
        Assert.AreEqual(1, result);
    }

    [TestMethod]
    public void RemoveAllExpiredTest()
    {
        var key = nameof(RemoveAllExpiredTest);
        var pid = nameof(RemoveAllExpiredTest);
        kv.Upsert(key, new Poco { Value = key }, pid);
        var result = kv.RemoveAllExpired(pid);
        Assert.AreEqual(0, result);
        kv.Upsert(key, new Poco { Value = key }, pid, DateTimeOffset.UtcNow.AddMinutes(-1));
        result = kv.RemoveAllExpired(pid);
        Assert.AreEqual(1, result);
    }

    [TestMethod]
    public void DuplicateKeyTest()
    {
        var key = nameof(DuplicateKeyTest);
        var pid = nameof(DuplicateKeyTest);
        var ok = kv.Create(key, new Poco { Value = key }, pid);
        var notok = kv.Create(key, new Poco { Value = key }, pid);
        Assert.IsTrue(ok);
        Assert.IsFalse(notok);
    }

    [TestMethod]
    public void MissingKeyTest()
    {
        var key = nameof(MissingKeyTest);
        var pid = nameof(MissingKeyTest);
        var result = kv.Update(key, new Poco { Value = key }, pid);
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void GetHashSetTest()
    {
        var key1 = nameof(GetHashSetTest) + "1";
        var key2 = nameof(GetHashSetTest) + "2";
        var pid = nameof(GetHashSetTest);
        kv.Upsert(key1, new Poco { Value = key1 }, pid);
        kv.Upsert(key2, new Poco { Value = key2 }, pid);
        var set1 = kv.GetHashSet<Poco>(pid);
        Assert.AreEqual(2, set1.Count);
    }
}