using System.Linq.Expressions;
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

public class UserProfile : UserProfileBase1
{
    public override string? Id { get; set; } // Add this line
    public string? Name { get; set; }
    public string? DisplayName { get; set; }
    public int Age { get; set; }
    public UserStatus Status { get; set; }
    public UserRole Role { get; set; }
    public Address? PrimaryAddress { get; set; }
    public Address? SecondaryAddress { get; set; }
    public List<string>? Tags { get; set; }
    public bool? IsVerified { get; set; }
    public override List<string>? AdditionalIds { get; set; } // Add this line
}

public abstract class UserProfileBase1 : UserProfileBase2, IUserProfileBase1
{ }

public abstract class UserProfileBase2 : IUserProfileBase2
{
    public abstract string? Id { get; set; } // Add this line
    public virtual List<string>? AdditionalIds { get; set; } // Add this line
}

public interface IUserProfileBase1 : IUserProfileBase2
{ }

public interface IUserProfileBase2
{
    string? Id { get; set; } // Add this line
    List<string>? AdditionalIds { get; set; } // Add this line
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
    public void RemoveAllExpiredGlobalTest()
    {
        var key = nameof(RemoveAllExpiredGlobalTest);
        var pid = nameof(RemoveAllExpiredGlobalTest);
        kv.Upsert(key, new Poco { Value = key }, pid);
        var result = kv.RemoveAllExpiredGlobal();
        Assert.AreEqual(0, result);
        kv.Upsert(key, new Poco { Value = key }, pid, DateTimeOffset.UtcNow.AddMinutes(-1));
        result = kv.RemoveAllExpiredGlobal();
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
    public async Task GetListTest()
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
    public async Task GetListOffsetTest()
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
    public async Task GetListFilterTest()
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
    public async Task GetListFilterStartsWithTest()
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
    public async Task GetListFilterContainsTest()
    {
        var key1 = nameof(GetListFilterContainsTest) + "1";
        var key2 = nameof(GetListFilterContainsTest) + "2";
        var pid = nameof(GetListFilterContainsTest);
        kv.Upsert(key1, new Poco { Value = key1 }, pid);
        kv.Upsert(key2, new Poco { Value = key2 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid, p => p.Value!.Contains(nameof(GetListFilterContainsTest))).ToListAsync();
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
        var totalCount = kv.Count<Poco>(pid);
        Assert.AreEqual(3, totalCount);
        var exactCount = kv.Count<Poco>(pid, p => p.Value == key1);
        Assert.AreEqual(1, exactCount);
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
        var removedCount = kv.RemoveAll<Poco>(pid, p => p.Value == key1);
        Assert.AreEqual(1, removedCount);
        var remainingCount = kv.Count<Poco>(pid);
        Assert.AreEqual(2, remainingCount);
        var remainingRemoved = kv.RemoveAll<Poco>(pid, p => p.Value!.StartsWith(nameof(RemoveAllFilterTest)));
        Assert.AreEqual(2, remainingRemoved);
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
        var removedCount = await kv.RemoveAllAsync<Poco>(pid, p => p.Value!.StartsWith(nameof(RemoveAllFilterAsyncTest)));
        Assert.AreEqual(3, removedCount);
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
        var list1 = await kv.GetListAsync<Poco>(pid, p => p.Value!.Equals(key1)).ToListAsync();
        Assert.AreEqual(1, list1.Count);
        Assert.AreEqual(key1, list1[0].Value);
        var list2 = await kv.GetListAsync<Poco>(pid, p => string.Equals(p.Value, key2)).ToListAsync();
        Assert.AreEqual(1, list2.Count);
        Assert.AreEqual(key2, list2[0].Value);
    }

    [TestMethod]
    public async Task QueryByEnumValue()
    {
        var key = nameof(QueryByEnumValue);
        var pid = nameof(QueryByEnumValue);
        var testUser = new UserProfile
        {
            Name = "John Doe",
            Status = UserStatus.Active,
            Role = UserRole.Admin
        };
        await kv.UpsertAsync(key, testUser, pid);
        var activeUsers = await kv.GetListAsync<UserProfile>(pid, u => u.Status == UserStatus.Active).ToListAsync();
        Assert.AreEqual(1, activeUsers.Count);
        Assert.AreEqual("John Doe", activeUsers[0].Name);
    }

    [TestMethod]
    public async Task QueryByNestedProperty()
    {
        var key = nameof(QueryByNestedProperty);
        var pid = nameof(QueryByNestedProperty);
        var testUser = new UserProfile
        {
            Name = "John Doe",
            PrimaryAddress = new Address
            {
                City = "New York",
                Country = "USA"
            }
        };
        await kv.UpsertAsync(key, testUser, pid);
        var nyUsers = await kv.GetListAsync<UserProfile>(pid, u => u.PrimaryAddress!.City == "New York").ToListAsync();
        Assert.AreEqual(1, nyUsers.Count);
        Assert.AreEqual("John Doe", nyUsers[0].Name);
    }

    [TestMethod]
    public async Task QueryWithConstant()
    {
        const int AGE_THRESHOLD = 28;
        var key = nameof(QueryWithConstant);
        var pid = nameof(QueryWithConstant);
        var testUser = new UserProfile
        {
            Name = "John Doe",
            Age = 30
        };
        await kv.UpsertAsync(key, testUser, pid);
        var olderUsers = await kv.GetListAsync<UserProfile>(pid, u => u.Age > AGE_THRESHOLD).ToListAsync();
        Assert.AreEqual(1, olderUsers.Count);
    }

    [TestMethod]
    public async Task ComplexQueryTest()
    {
        var key = nameof(ComplexQueryTest);
        var pid = nameof(ComplexQueryTest);
        var testUser = new UserProfile
        {
            Name = "John Doe",
            IsVerified = true,
            Role = UserRole.Admin,
            PrimaryAddress = new Address { Country = "USA" },
            SecondaryAddress = new Address { City = "Boston" }
        };
        await kv.UpsertAsync(key, testUser, pid);
        Expression<Func<UserProfile, bool>> expr = u =>
            u.IsVerified == true &&
            u.Role == UserRole.Admin &&
            u.PrimaryAddress!.Country == "USA" &&
            u.SecondaryAddress!.City == "Boston";
        var verifiedAdmins = await kv.GetListAsync(pid, expr).ToListAsync();
        Assert.AreEqual(1, verifiedAdmins.Count);
    }

    [TestMethod]
    public async Task ComplexQuery2Test()
    {
        var key = nameof(ComplexQueryTest);
        var pid = nameof(ComplexQueryTest);
        var testUser = new UserProfile
        {
            Name = "John Doe",
            IsVerified = true,
            Role = UserRole.Admin,
            PrimaryAddress = new Address { Country = "USA" },
            SecondaryAddress = new Address { City = "Boston" }
        };
        await kv.UpsertAsync(key, testUser, pid);
        Expression<Func<UserProfile, bool>> expr = u =>
            !(u.IsVerified == false) &&
            u.Role == UserRole.Admin &&
            u.PrimaryAddress!.Country == "USA" &&
            u.SecondaryAddress!.City == "Boston";
        var verifiedAdmins = await kv.GetListAsync(pid, expr).ToListAsync();
        Assert.AreEqual(1, verifiedAdmins.Count);
    }

    [TestMethod]
    public async Task MultipleNestedConditionsTest()
    {
        var key = nameof(MultipleNestedConditionsTest);
        var pid = nameof(MultipleNestedConditionsTest);
        var testUser = new UserProfile
        {
            Name = "John Doe",
            PrimaryAddress = new Address
            {
                ZipCode = 10001,
                City = "New York"
            },
            SecondaryAddress = new Address
            {
                City = "Boston"
            }
        };
        await kv.UpsertAsync(key, testUser, pid);
        Expression<Func<UserProfile, bool>> expr = u =>
            u.PrimaryAddress!.ZipCode < 20000 &&
            u.PrimaryAddress.City != "San Francisco" &&
            (u.SecondaryAddress == null || u.SecondaryAddress.City == "Boston");
        var users = await kv.GetListAsync(pid, expr).ToListAsync();
        Assert.AreEqual(1, users.Count);
    }

    [TestMethod]
    public async Task EnumComparisonWithStringTest()
    {
        var key = nameof(EnumComparisonWithStringTest);
        var pid = nameof(EnumComparisonWithStringTest);

        var testUser = new UserProfile
        {
            Name = "John Doe",
            Status = UserStatus.Active,
            Role = UserRole.Admin
        };
        await kv.UpsertAsync(key, testUser, pid);
        Expression<Func<UserProfile, bool>> expr = u =>
            u.Status.ToString() == "Active" &&
            u.Role.ToString() == "Admin";
        var activeAdmins = await kv.GetListAsync(pid, expr).ToListAsync();
        Assert.AreEqual(1, activeAdmins.Count);
    }

    [TestMethod]
    public async Task NestedPropertyWithConstantComparisonTest()
    {
        const string COUNTRY = "USA";
        const int ZIP_THRESHOLD = 90000;
        var key = nameof(NestedPropertyWithConstantComparisonTest);
        var pid = nameof(NestedPropertyWithConstantComparisonTest);
        var testUser = new UserProfile
        {
            Name = "Jane Smith",
            PrimaryAddress = new Address
            {
                Country = "USA",
                ZipCode = 94102
            }
        };
        await kv.UpsertAsync(key, testUser, pid);
        Expression<Func<UserProfile, bool>> expr = u =>
            u.PrimaryAddress!.Country == COUNTRY &&
            u.PrimaryAddress.ZipCode > ZIP_THRESHOLD;
        var westCoastUsers = await kv.GetListAsync(pid, expr).ToListAsync();
        Assert.AreEqual(1, westCoastUsers.Count);
    }

    [TestMethod]
    public async Task FilterWithStringParameterTest()
    {
        var key = nameof(FilterWithStringParameterTest);
        var pid = nameof(FilterWithStringParameterTest);
        var testData = new[]
        {
            new UserProfile
            {
                Name = "John Smith",
                DisplayName = "Johnny",
                Status = UserStatus.Active
            },
            new UserProfile
            {
                Name = "Jane Johnny",
                DisplayName = "Jane",
                Status = UserStatus.Active
            }
        };
        foreach (var user in testData)
        {
            await kv.UpsertAsync($"{key}_{user.Name}", user, pid);
        }
        string filterText = "john";  // Note: lowercase to test case insensitivity
        Expression<Func<UserProfile, bool>> expr = p =>
            p.Status == UserStatus.Active &&
            (p.Name!.Contains(filterText, StringComparison.OrdinalIgnoreCase) ||
             p.DisplayName!.Contains(filterText, StringComparison.OrdinalIgnoreCase));
        var results = await kv.GetListAsync(pid, expr).ToListAsync();
        Assert.AreEqual(2, results.Count); // Should match both John Smith and Johnny
        Assert.IsTrue(results.Any(u => u.Name == "John Smith"));
        Assert.IsTrue(results.Any(u => u.DisplayName == "Johnny"));
    }

    [TestMethod]
    public async Task FilterByTagTest()
    {
        var key1 = nameof(FilterByTagTest) + "1";
        var key2 = nameof(FilterByTagTest) + "2";
        var pid = nameof(FilterByTagTest);

        var user1 = new UserProfile
        {
            Name = "Alice",
            Tags = new List<string> { "vip", "early-adopter" }
        };

        var user2 = new UserProfile
        {
            Name = "Bob",
            Tags = new List<string> { "regular", "new-user" }
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // This query should filter users by the "vip" tag
        Expression<Func<UserProfile, bool>> expr = u => u.Tags!.Contains("vip");
        var vipUsers = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.AreEqual(1, vipUsers.Count);
        Assert.AreEqual("Alice", vipUsers[0].Name);
    }

    [TestMethod]
    public async Task ComplexExpressionTest()
    {
        var key1 = nameof(ComplexExpressionTest) + "1";
        var key2 = nameof(ComplexExpressionTest) + "2";
        var pid = nameof(ComplexExpressionTest);

        var user1 = new UserProfile
        {
            Id = "user1",
            Name = "Alice",
            AdditionalIds = new List<string> { "id1", "id2" }
        };

        var user2 = new UserProfile
        {
            Id = "user2",
            Name = "Bob",
            AdditionalIds = new List<string> { "id3", "id4" }
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // Variables for the expression
        bool notIdIsEmpty = false;
        string notId = "user3";
        string idOrAdditionalId = "id1";

        // This query should filter users by the complex expression
        Expression<Func<UserProfile, bool>> expr = q =>
            (notIdIsEmpty || q.Id != notId) &&
            (q.Id == idOrAdditionalId || q.AdditionalIds!.Contains(idOrAdditionalId));

        var users = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.AreEqual(1, users.Count);
        Assert.AreEqual("Alice", users[0].Name);
    }

    [TestMethod]
    public async Task ComplexExpressionInheritanceTest()
    {
        var key1 = nameof(ComplexExpressionTest) + "1";
        var key2 = nameof(ComplexExpressionTest) + "2";
        var pid = nameof(ComplexExpressionTest);

        var user1 = new UserProfile
        {
            Id = "user1",
            Name = "Alice",
            AdditionalIds = new List<string> { "id1", "id2" }
        };

        var user2 = new UserProfile
        {
            Id = "user2",
            Name = "Bob",
            AdditionalIds = new List<string> { "id3", "id4" }
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // Variables for the expression
        bool notIdIsEmpty = false;
        string notId = "user3";
        string idOrAdditionalId = "id1";

        // This query should filter users by the complex expression
        List<UserProfile> users = await QueryByBase<UserProfile>(pid, notIdIsEmpty, notId, idOrAdditionalId);

        Assert.AreEqual(1, users.Count);
        Assert.AreEqual("Alice", users[0].Name);
    }

    private static async Task<List<T>> QueryByBase<T>(string pid, bool notIdIsEmpty, string notId, string idOrAdditionalId) where T : UserProfileBase1
    {
        Expression<Func<T, bool>> expr = q =>
            (notIdIsEmpty || q.Id != notId) &&
            (q.Id == idOrAdditionalId || q.AdditionalIds!.Contains(idOrAdditionalId));

        var users = await kv.GetListAsync(pid, expr).ToListAsync();
        return users;
    }

    [TestMethod]
    public async Task ComplexExpressionInterfaceInheritanceTest()
    {
        var key1 = nameof(ComplexExpressionTest) + "1";
        var key2 = nameof(ComplexExpressionTest) + "2";
        var pid = nameof(ComplexExpressionTest);

        var user1 = new UserProfile
        {
            Id = "user1",
            Name = "Alice",
            AdditionalIds = new List<string> { "id1", "id2" }
        };

        var user2 = new UserProfile
        {
            Id = "user2",
            Name = "Bob",
            AdditionalIds = new List<string> { "id3", "id4" }
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // Variables for the expression
        bool notIdIsEmpty = false;
        string notId = "user3";
        string idOrAdditionalId = "id1";

        // This query should filter users by the complex expression
        List<UserProfile> users = await QueryByInterfaceBase<UserProfile>(pid, notIdIsEmpty, notId, idOrAdditionalId);

        Assert.AreEqual(1, users.Count);
        Assert.AreEqual("Alice", users[0].Name);
    }

    private static async Task<List<T>> QueryByInterfaceBase<T>(string pid, bool notIdIsEmpty, string notId, string idOrAdditionalId) where T : IUserProfileBase1
    {
        Expression<Func<T, bool>> expr = q =>
            (notIdIsEmpty || q.Id != notId) &&
            (q.Id == idOrAdditionalId || q.AdditionalIds!.Contains(idOrAdditionalId));

        var users = await kv.GetListAsync(pid, expr).ToListAsync();
        return users;
    }

    [TestMethod]
    public async Task FilterByIsNullOrWhiteSpaceTest()
    {
        var key1 = nameof(FilterByIsNullOrWhiteSpaceTest) + "1";
        var key2 = nameof(FilterByIsNullOrWhiteSpaceTest) + "2";
        var key3 = nameof(FilterByIsNullOrWhiteSpaceTest) + "3";
        var key4 = nameof(FilterByIsNullOrWhiteSpaceTest) + "4";
        var pid = nameof(FilterByIsNullOrWhiteSpaceTest);

        var user1 = new UserProfile
        {
            Name = "Alice",
            DisplayName = "Alice Smith"
        };

        var user2 = new UserProfile
        {
            Name = "Bob",
            DisplayName = null // null value
        };

        var user3 = new UserProfile
        {
            Name = "Charlie",
            DisplayName = "" // empty string
        };

        var user4 = new UserProfile
        {
            Name = "David",
            DisplayName = "   " // whitespace only
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);
        await kv.UpsertAsync(key3, user3, pid);
        await kv.UpsertAsync(key4, user4, pid);

        // This query should filter users with null, empty, or whitespace-only DisplayName
        Expression<Func<UserProfile, bool>> expr = u => string.IsNullOrWhiteSpace(u.DisplayName);
        var usersWithEmptyDisplayName = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.AreEqual(3, usersWithEmptyDisplayName.Count);
        Assert.IsTrue(usersWithEmptyDisplayName.Any(u => u.Name == "Bob"));
        Assert.IsTrue(usersWithEmptyDisplayName.Any(u => u.Name == "Charlie"));
        Assert.IsTrue(usersWithEmptyDisplayName.Any(u => u.Name == "David"));

        // Test the opposite - users with valid DisplayName
        Expression<Func<UserProfile, bool>> exprNotEmpty = u => !string.IsNullOrWhiteSpace(u.DisplayName);
        var usersWithValidDisplayName = await kv.GetListAsync(pid, exprNotEmpty).ToListAsync();

        Assert.AreEqual(1, usersWithValidDisplayName.Count);
        Assert.AreEqual("Alice", usersWithValidDisplayName[0].Name);
    }

    [TestMethod]
    public async Task FilterByHasValueTest()
    {
        var key1 = nameof(FilterByHasValueTest) + "1";
        var key2 = nameof(FilterByHasValueTest) + "2";
        var key3 = nameof(FilterByHasValueTest) + "3";
        var pid = nameof(FilterByHasValueTest);

        var user1 = new UserProfile
        {
            Name = "Alice",
            IsVerified = true
        };

        var user2 = new UserProfile
        {
            Name = "Bob",
            IsVerified = false
        };

        var user3 = new UserProfile
        {
            Name = "Charlie",
            IsVerified = null // null value
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);
        await kv.UpsertAsync(key3, user3, pid);

        // This query should filter users where IsVerified has a value (not null)
        Expression<Func<UserProfile, bool>> expr = u => u.IsVerified.HasValue;
        var usersWithVerificationStatus = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.AreEqual(2, usersWithVerificationStatus.Count);
        Assert.IsTrue(usersWithVerificationStatus.Any(u => u.Name == "Alice"));
        Assert.IsTrue(usersWithVerificationStatus.Any(u => u.Name == "Bob"));

        // Test the opposite - users where IsVerified is null
        Expression<Func<UserProfile, bool>> exprNoValue = u => !u.IsVerified.HasValue;
        var usersWithoutVerificationStatus = await kv.GetListAsync(pid, exprNoValue).ToListAsync();

        Assert.AreEqual(1, usersWithoutVerificationStatus.Count);
        Assert.AreEqual("Charlie", usersWithoutVerificationStatus[0].Name);
    }
}