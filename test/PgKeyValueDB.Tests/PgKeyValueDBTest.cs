using Microsoft.Extensions.DependencyInjection;
using MysticMind.PostgresEmbed;

namespace Wololo.PgKeyValueDB.Tests;

public enum UserStatus
{
    Active,
    Inactive,
    Pending
}

public enum UserRole
{
    Admin,
    User,
    Guest
}

public class Address
{
    public string? Street { get; set; }
    public string? City { get; set; }
    public string? Country { get; set; }
    public int ZipCode { get; set; }
}

public class UserProfile
{
    public string? Name { get; set; }
    public int Age { get; set; }
    public UserStatus Status { get; set; }
    public UserRole Role { get; set; }
    public Address? PrimaryAddress { get; set; }
    public Address? SecondaryAddress { get; set; }
    public List<string>? Tags { get; set; }
    public bool IsVerified { get; set; }
}

public class Poco
{
    public string? Value { get; set; }
}

[TestClass]
public class PgKeyValueDBTest
{
    private static PgServer pg = null!;
    private static PgKeyValueDB kv = null!;
    private const string TestPid = "AdvancedTest";

    [ClassInitialize]
    public static void ClassInit(TestContext context)
    {
        IServiceCollection services = new ServiceCollection();
        pg = new PgServer("16.2.0", clearWorkingDirOnStart: true, clearInstanceDirOnStop: true);
        pg.Start();
        services.AddPgKeyValueDB($"Host=localhost;Port={pg.PgPort};Username=postgres;Password=postgres;Database=postgres", b =>
        {
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


    private static async Task SetupTestData()
    {
        var users = new[]
        {
            new UserProfile
            {
                Name = "John Doe",
                Age = 30,
                Status = UserStatus.Active,
                Role = UserRole.Admin,
                PrimaryAddress = new Address
                {
                    Street = "123 Main St",
                    City = "New York",
                    Country = "USA",
                    ZipCode = 10001
                },
                SecondaryAddress = new Address
                {
                    Street = "456 Park Ave",
                    City = "Boston",
                    Country = "USA",
                    ZipCode = 02108
                },
                Tags = new List<string> { "vip", "early-adopter" },
                IsVerified = true
            },
            new UserProfile
            {
                Name = "Jane Smith",
                Age = 25,
                Status = UserStatus.Pending,
                Role = UserRole.User,
                PrimaryAddress = new Address
                {
                    Street = "789 Oak Rd",
                    City = "San Francisco",
                    Country = "USA",
                    ZipCode = 94102
                },
                Tags = new List<string> { "beta-tester" },
                IsVerified = false
            }
        };

        for (int i = 0; i < users.Length; i++)
        {
            await kv.UpsertAsync($"user_{i}", users[i], TestPid);
        }
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
    public async void GetListTest()
    {
        var key1 = nameof(GetListTest) + "1";
        var key2 = nameof(GetListTest) + "2";
        var pid = nameof(GetListTest);
        kv.Upsert(key1, new Poco { Value = key1 }, pid);
        kv.Upsert(key2, new Poco { Value = key2 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid).ToListAsync();
        Assert.AreEqual(2, list1.Count);
    }

    [TestMethod]
    public async void GetListOffsetTest()
    {
        var key1 = nameof(GetListOffsetTest) + "1";
        var key2 = nameof(GetListOffsetTest) + "2";
        var key3 = nameof(GetListOffsetTest) + "3";
        var key4 = nameof(GetListOffsetTest) + "4";
        var pid = nameof(GetListOffsetTest);
        kv.Upsert(key1, new Poco { Value = key1 }, pid);
        kv.Upsert(key2, new Poco { Value = key2 }, pid);
        kv.Upsert(key3, new Poco { Value = key3 }, pid);
        kv.Upsert(key4, new Poco { Value = key4 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid, null, 2, 1).ToListAsync();
        Assert.AreEqual(2, list1.Count);
        Assert.AreEqual(nameof(GetListOffsetTest) + "2", list1[0].Value);
        var list2 = await kv.GetListAsync<Poco>(pid, null, 2, 3).ToListAsync();
        Assert.AreEqual(1, list2.Count);
        Assert.AreEqual(nameof(GetListOffsetTest) + "4", list2[0].Value);
    }

    [TestMethod]
    public async void GetListFilterTest()
    {
        var key1 = nameof(GetListFilterTest) + "1";
        var key2 = nameof(GetListFilterTest) + "2";
        var pid = nameof(GetListFilterTest);
        kv.Upsert(key1, new Poco { Value = key1 }, pid);
        kv.Upsert(key2, new Poco { Value = key2 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid, p => p.Value == nameof(GetListFilterTest) + "2").ToListAsync();
        Assert.AreEqual(1, list1.Count);
    }

    [TestMethod]
    public async void GetListFilterStartsWithTest()
    {
        var key1 = nameof(GetListFilterStartsWithTest) + "1";
        var key2 = nameof(GetListFilterStartsWithTest) + "2";
        var pid = nameof(GetListFilterStartsWithTest);
        kv.Upsert(key1, new Poco { Value = key1 }, pid);
        kv.Upsert(key2, new Poco { Value = key2 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid, p => p.Value!.StartsWith(nameof(GetListFilterStartsWithTest))).ToListAsync();
        Assert.AreEqual(2, list1.Count);
    }

    [TestMethod]
    public void CountFilterTest()
    {
        var key1 = nameof(CountFilterTest) + "1";
        var key2 = nameof(CountFilterTest) + "2";
        var key3 = nameof(CountFilterTest) + "3";
        var pid = nameof(CountFilterTest);

        kv.Upsert(key1, new Poco { Value = key1 }, pid);
        kv.Upsert(key2, new Poco { Value = key2 }, pid);
        kv.Upsert(key3, new Poco { Value = key3 }, pid);

        // Count all
        var totalCount = kv.Count<Poco>(pid);
        Assert.AreEqual(3, totalCount);

        // Count with exact match
        var exactCount = kv.Count<Poco>(pid, p => p.Value == key1);
        Assert.AreEqual(1, exactCount);

        // Count with prefix match
        var prefixCount = kv.Count<Poco>(pid, p => p.Value!.StartsWith(nameof(CountFilterTest)));
        Assert.AreEqual(3, prefixCount);
    }

    [TestMethod]
    public async Task CountFilterAsyncTest()
    {
        var key1 = nameof(CountFilterAsyncTest) + "1";
        var key2 = nameof(CountFilterAsyncTest) + "2";
        var key3 = nameof(CountFilterAsyncTest) + "3";
        var pid = nameof(CountFilterAsyncTest);

        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);
        await kv.UpsertAsync(key3, new Poco { Value = key3 }, pid);

        // Count with prefix match async
        var prefixCount = await kv.CountAsync<Poco>(pid, p => p.Value!.StartsWith(nameof(CountFilterAsyncTest)));
        Assert.AreEqual(3, prefixCount);
    }

    [TestMethod]
    public void RemoveAllFilterTest()
    {
        var key1 = nameof(RemoveAllFilterTest) + "1";
        var key2 = nameof(RemoveAllFilterTest) + "2";
        var key3 = nameof(RemoveAllFilterTest) + "3";
        var pid = nameof(RemoveAllFilterTest);

        kv.Upsert(key1, new Poco { Value = key1 }, pid);
        kv.Upsert(key2, new Poco { Value = key2 }, pid);
        kv.Upsert(key3, new Poco { Value = key3 }, pid);

        // Remove specific item
        var removedCount = kv.RemoveAll<Poco>(pid, p => p.Value == key1);
        Assert.AreEqual(1, removedCount);

        // Verify remaining count
        var remainingCount = kv.Count<Poco>(pid);
        Assert.AreEqual(2, remainingCount);

        // Remove remaining items with prefix
        var remainingRemoved = kv.RemoveAll<Poco>(pid, p => p.Value!.StartsWith(nameof(RemoveAllFilterTest)));
        Assert.AreEqual(2, remainingRemoved);

        // Verify all gone
        var finalCount = kv.Count<Poco>(pid);
        Assert.AreEqual(0, finalCount);
    }

    [TestMethod]
    public async Task RemoveAllFilterAsyncTest()
    {
        var key1 = nameof(RemoveAllFilterAsyncTest) + "1";
        var key2 = nameof(RemoveAllFilterAsyncTest) + "2";
        var key3 = nameof(RemoveAllFilterAsyncTest) + "3";
        var pid = nameof(RemoveAllFilterAsyncTest);

        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);
        await kv.UpsertAsync(key3, new Poco { Value = key3 }, pid);

        // Remove with filter async
        var removedCount = await kv.RemoveAllAsync<Poco>(pid, p => p.Value!.StartsWith(nameof(RemoveAllFilterAsyncTest)));
        Assert.AreEqual(3, removedCount);

        // Verify all gone
        var finalCount = await kv.CountAsync<Poco>(pid);
        Assert.AreEqual(0, finalCount);
    }

    [TestMethod]
    public async Task StringEqualsTest()
    {
        var key1 = nameof(StringEqualsTest) + "1";
        var key2 = nameof(StringEqualsTest) + "2";
        var pid = nameof(StringEqualsTest);

        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);

        // Test instance method
        var list1 = await kv.GetListAsync<Poco>(pid, p => p.Value!.Equals(key1)).ToListAsync();
        Assert.AreEqual(1, list1.Count);
        Assert.AreEqual(key1, list1[0].Value);

        // Test static method
        var list2 = await kv.GetListAsync<Poco>(pid, p => string.Equals(p.Value, key2)).ToListAsync();
        Assert.AreEqual(1, list2.Count);
        Assert.AreEqual(key2, list2[0].Value);
    }

    [TestMethod]
    public async Task QueryByEnumValue()
    {
        var activeUsers = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.Status == UserStatus.Active).ToListAsync();
        Assert.AreEqual(1, activeUsers.Count);
        Assert.AreEqual("John Doe", activeUsers[0].Name);

        var pendingUsers = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.Status == UserStatus.Pending && u.Role == UserRole.User).ToListAsync();
        Assert.AreEqual(1, pendingUsers.Count);
        Assert.AreEqual("Jane Smith", pendingUsers[0].Name);
    }

    [TestMethod]
    public async Task QueryByNestedProperty()
    {
        var nyUsers = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.PrimaryAddress!.City == "New York").ToListAsync();
        Assert.AreEqual(1, nyUsers.Count);
        Assert.AreEqual("John Doe", nyUsers[0].Name);

        var sfUsers = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.PrimaryAddress!.City == "San Francisco" && u.Age < 30).ToListAsync();
        Assert.AreEqual(1, sfUsers.Count);
        Assert.AreEqual("Jane Smith", sfUsers[0].Name);
    }

    [TestMethod]
    public async Task QueryWithConstant()
    {
        const int AGE_THRESHOLD = 28;
        var olderUsers = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.Age > AGE_THRESHOLD).ToListAsync();
        Assert.AreEqual(1, olderUsers.Count);
        Assert.AreEqual("John Doe", olderUsers[0].Name);
    }

    [TestMethod]
    public async Task ComplexQueryTest()
    {
        var verifiedAdmins = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.IsVerified &&
                 u.Role == UserRole.Admin &&
                 u.PrimaryAddress!.Country == "USA" &&
                 u.SecondaryAddress!.City == "Boston").ToListAsync();

        Assert.AreEqual(1, verifiedAdmins.Count);
        Assert.AreEqual("John Doe", verifiedAdmins[0].Name);
    }

    [TestMethod]
    public async Task MultipleNestedConditionsTest()
    {
        var users = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.PrimaryAddress!.ZipCode < 20000 &&
                 u.PrimaryAddress.City != "San Francisco" &&
                 (u.SecondaryAddress == null || u.SecondaryAddress.City == "Boston")).ToListAsync();

        Assert.AreEqual(1, users.Count);
        Assert.AreEqual("John Doe", users[0].Name);
    }

    [TestMethod]
    public async Task EnumComparisonWithStringTest()
    {
        var activeUsers = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.Status.ToString() == "Active" && u.Role.ToString() == "Admin").ToListAsync();

        Assert.AreEqual(1, activeUsers.Count);
        Assert.AreEqual("John Doe", activeUsers[0].Name);
    }

    [TestMethod]
    public async Task NestedPropertyWithConstantComparisonTest()
    {
        const string COUNTRY = "USA";
        const int ZIP_THRESHOLD = 90000;

        var westCoastUsers = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.PrimaryAddress!.Country == COUNTRY &&
                 u.PrimaryAddress.ZipCode > ZIP_THRESHOLD).ToListAsync();

        Assert.AreEqual(1, westCoastUsers.Count);
        Assert.AreEqual("Jane Smith", westCoastUsers[0].Name);
    }

    [TestMethod]
    public async Task RemoveWithComplexFilterTest()
    {
        // First, add a test user
        await kv.UpsertAsync("test_user", new UserProfile
        {
            Name = "Test User",
            Age = 35,
            Status = UserStatus.Inactive,
            Role = UserRole.Guest,
            PrimaryAddress = new Address
            {
                Street = "Test St",
                City = "Test City",
                Country = "Test Country",
                ZipCode = 99999
            },
            IsVerified = false
        }, TestPid);

        // Remove using complex filter
        var removedCount = await kv.RemoveAllAsync<UserProfile>(TestPid,
            u => u.Status == UserStatus.Inactive &&
                 u.Role == UserRole.Guest &&
                 u.PrimaryAddress!.ZipCode == 99999);

        Assert.AreEqual(1, removedCount);

        // Verify removal
        var remainingInactiveUsers = await kv.GetListAsync<UserProfile>(TestPid,
            u => u.Status == UserStatus.Inactive).ToListAsync();
        Assert.AreEqual(0, remainingInactiveUsers.Count);
    }
}