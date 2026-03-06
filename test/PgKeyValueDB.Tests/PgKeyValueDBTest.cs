using System.Linq.Expressions;
using Microsoft.Extensions.DependencyInjection;
using MysticMind.PostgresEmbed;

[assembly: Parallelize]

namespace Wololo.PgKeyValueDB.Tests;

// Extension method to simulate upstream usage pattern
public static class StringExtensions
{
    public static bool IsNullOrWhiteSpace(this string? value)
    {
        return string.IsNullOrWhiteSpace(value);
    }
}

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

public class User
{
    public string? DataType { get; set; }
    public string? Email { get; set; }
    public string? Phone { get; set; }
    public string? Username { get; set; }
    public string? UserId { get; set; }
}

public class UserProfile : UserProfileBase1
{
    public override string? Id { get; set; } // Add this line
    public string? Name { get; set; }
    public string? DisplayName { get; set; }
    public string? Email { get; set; }
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

// Models to simulate upstream Party/Link patterns
public class PartyLink
{
    public string? Name { get; set; }
    public string? Type { get; set; }
}

public class Session
{
    public string? Id { get; set; }
    public List<PartyLink>? DownPartyLinks { get; set; }
    public List<PartyLink>? UpPartyLinks { get; set; }
}

public class TypedModel
{
    public string? Name { get; set; }
    public short ShortVal { get; set; }
    public long LongVal { get; set; }
    public decimal DecimalVal { get; set; }
    public double DoubleVal { get; set; }
    public float FloatVal { get; set; }
    public DateTimeOffset? CreatedAt { get; set; }
}

public class ModelWithBool
{
    public string? Name { get; set; }
    public bool IsActive { get; set; }
}

public class ModelWithField
{
    [System.Text.Json.Serialization.JsonInclude]
    public string? Name;
    [System.Text.Json.Serialization.JsonInclude]
    public int Value;
}

public class InnerItem
{
    public int? Score { get; set; }
}

public class OuterItem
{
    public string? Name { get; set; }
    public InnerItem? Details { get; set; }
}

public class ScheduledItem
{
    public string? Name { get; set; }
    public DateTime? ScheduledAt { get; set; }
}

public class Deep3
{
    public string? Value { get; set; }
}

public class Deep2
{
    public Deep3? Inner { get; set; }
}

public class Deep1
{
    public string? Name { get; set; }
    public Deep2? Middle { get; set; }
}

public class ModelWithGuid
{
    public string? Name { get; set; }
    public Guid ExternalId { get; set; }
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
        var pgVersion = Environment.GetEnvironmentVariable("PG_VERSION") ?? "18.3.0";
        pg = new PgServer(pgVersion, clearWorkingDirOnStart: true, clearInstanceDirOnStop: true);
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
        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid).ToListAsync();
        Assert.HasCount(2, list1);
    }

    [TestMethod]
    public async Task GetListOffsetTest()
    {
        var key1 = nameof(GetListOffsetTest) + "1";
        var key2 = nameof(GetListOffsetTest) + "2";
        var key3 = nameof(GetListOffsetTest) + "3";
        var key4 = nameof(GetListOffsetTest) + "4";
        var pid = nameof(GetListOffsetTest);
        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);
        await kv.UpsertAsync(key3, new Poco { Value = key3 }, pid);
        await kv.UpsertAsync(key4, new Poco { Value = key4 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid, null, 2, 1).ToListAsync();
        Assert.HasCount(2, list1);
        Assert.AreEqual(nameof(GetListOffsetTest) + "2", list1[0].Value);
        var list2 = await kv.GetListAsync<Poco>(pid, null, 2, 3).ToListAsync();
        Assert.HasCount(1, list2);
        Assert.AreEqual(nameof(GetListOffsetTest) + "4", list2[0].Value);
    }

    [TestMethod]
    public async Task GetListFilterTest()
    {
        var key1 = nameof(GetListFilterTest) + "1";
        var key2 = nameof(GetListFilterTest) + "2";
        var pid = nameof(GetListFilterTest);
        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid, p => p.Value == nameof(GetListFilterTest) + "2").ToListAsync();
        Assert.HasCount(1, list1);
    }

    [TestMethod]
    public async Task GetListFilterStartsWithTest()
    {
        var key1 = nameof(GetListFilterStartsWithTest) + "1";
        var key2 = nameof(GetListFilterStartsWithTest) + "2";
        var pid = nameof(GetListFilterStartsWithTest);
        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid, p => p.Value!.StartsWith(nameof(GetListFilterStartsWithTest))).ToListAsync();
        Assert.HasCount(2, list1);
    }

    [TestMethod]
    public async Task GetListFilterContainsTest()
    {
        var key1 = nameof(GetListFilterContainsTest) + "1";
        var key2 = nameof(GetListFilterContainsTest) + "2";
        var pid = nameof(GetListFilterContainsTest);
        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);
        var list1 = await kv.GetListAsync<Poco>(pid, p => p.Value!.Contains(nameof(GetListFilterContainsTest))).ToListAsync();
        Assert.HasCount(2, list1);
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
        Assert.HasCount(1, list1);
        Assert.AreEqual(key1, list1[0].Value);
        var list2 = await kv.GetListAsync<Poco>(pid, p => string.Equals(p.Value, key2)).ToListAsync();
        Assert.HasCount(1, list2);
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
        Assert.HasCount(1, activeUsers);
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
        Assert.HasCount(1, nyUsers);
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
        Assert.HasCount(1, olderUsers);
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
        Assert.HasCount(1, verifiedAdmins);
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
            u.IsVerified != false &&
            u.Role == UserRole.Admin &&
            u.PrimaryAddress!.Country == "USA" &&
            u.SecondaryAddress!.City == "Boston";
        var verifiedAdmins = await kv.GetListAsync(pid, expr).ToListAsync();
        Assert.HasCount(1, verifiedAdmins);
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
        Assert.HasCount(1, users);
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
        Assert.HasCount(1, activeAdmins);
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
        Assert.HasCount(1, westCoastUsers);
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
        Assert.HasCount(2, results); // Should match both John Smith and Johnny
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
            Tags = ["vip", "early-adopter"]
        };

        var user2 = new UserProfile
        {
            Name = "Bob",
            Tags = ["regular", "new-user"]
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // This query should filter users by the "vip" tag
        Expression<Func<UserProfile, bool>> expr = u => u.Tags!.Contains("vip");
        var vipUsers = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.HasCount(1, vipUsers);
        Assert.AreEqual("Alice", vipUsers[0].Name);
    }

    [TestMethod]
    public async Task FilterByAnyTagTest()
    {
        var key1 = nameof(FilterByAnyTagTest) + "1";
        var key2 = nameof(FilterByAnyTagTest) + "2";
        var pid = nameof(FilterByAnyTagTest);

        var user1 = new UserProfile
        {
            Name = "Alice",
            Tags = ["vip", "early-adopter"]
        };

        var user2 = new UserProfile
        {
            Name = "Bob",
            Tags = ["regular", "new-user"]
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // This query should filter users where any tag starts with "vip"
        Expression<Func<UserProfile, bool>> expr = u => u.Tags!.Any(tag => tag.StartsWith("vip"));
        var vipUsers = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.HasCount(1, vipUsers);
        Assert.AreEqual("Alice", vipUsers[0].Name);
    }

    [TestMethod]
    public async Task FilterByAnyWithoutPredicateTest()
    {
        var key1 = nameof(FilterByAnyWithoutPredicateTest) + "1";
        var key2 = nameof(FilterByAnyWithoutPredicateTest) + "2";
        var key3 = nameof(FilterByAnyWithoutPredicateTest) + "3";
        var pid = nameof(FilterByAnyWithoutPredicateTest);

        var user1 = new UserProfile
        {
            Name = "Alice",
            Tags = ["vip", "early-adopter"]
        };

        var user2 = new UserProfile
        {
            Name = "Bob",
            Tags = []
        };

        var user3 = new UserProfile
        {
            Name = "Charlie",
            Tags = null
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);
        await kv.UpsertAsync(key3, user3, pid);

        // This query should filter users who have any tags (excluding null and empty)
        Expression<Func<UserProfile, bool>> expr = u => u.Tags!.Any();
        var usersWithTags = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.HasCount(1, usersWithTags);
        Assert.AreEqual("Alice", usersWithTags[0].Name);
    }

    [TestMethod]
    public async Task FilterByAnyTagEqualsTest()
    {
        var key1 = nameof(FilterByAnyTagEqualsTest) + "1";
        var key2 = nameof(FilterByAnyTagEqualsTest) + "2";
        var pid = nameof(FilterByAnyTagEqualsTest);

        var user1 = new UserProfile
        {
            Name = "Alice",
            Tags = ["vip", "early-adopter"]
        };

        var user2 = new UserProfile
        {
            Name = "Bob",
            Tags = ["regular", "new-user"]
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // This query should filter users where any tag equals exactly "vip"
        Expression<Func<UserProfile, bool>> expr = u => u.Tags!.Any(tag => tag == "vip");
        var vipUsers = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.HasCount(1, vipUsers);
        Assert.AreEqual("Alice", vipUsers[0].Name);
    }

    [TestMethod]
    public async Task FilterByAnyTagContainsTest()
    {
        var key1 = nameof(FilterByAnyTagContainsTest) + "1";
        var key2 = nameof(FilterByAnyTagContainsTest) + "2";
        var pid = nameof(FilterByAnyTagContainsTest);

        var user1 = new UserProfile
        {
            Name = "Alice",
            Tags = ["super-vip", "early-adopter"]
        };

        var user2 = new UserProfile
        {
            Name = "Bob",
            Tags = ["regular", "new-user"]
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // This query should filter users where any tag contains "vip"
        Expression<Func<UserProfile, bool>> expr = u => u.Tags!.Any(tag => tag.Contains("vip"));
        var vipUsers = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.HasCount(1, vipUsers);
        Assert.AreEqual("Alice", vipUsers[0].Name);
    }

    [TestMethod]
    public async Task FilterByAnyTagEndsWithTest()
    {
        var key1 = nameof(FilterByAnyTagEndsWithTest) + "1";
        var key2 = nameof(FilterByAnyTagEndsWithTest) + "2";
        var pid = nameof(FilterByAnyTagEndsWithTest);

        var user1 = new UserProfile
        {
            Name = "Alice",
            Tags = ["member-vip", "early-adopter"]
        };

        var user2 = new UserProfile
        {
            Name = "Bob",
            Tags = ["regular", "new-user"]
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // This query should filter users where any tag ends with "vip"
        Expression<Func<UserProfile, bool>> expr = u => u.Tags!.Any(tag => tag.EndsWith("vip"));
        var vipUsers = await kv.GetListAsync(pid, expr).ToListAsync();

        Assert.HasCount(1, vipUsers);
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
            AdditionalIds = ["id1", "id2"]
        };

        var user2 = new UserProfile
        {
            Id = "user2",
            Name = "Bob",
            AdditionalIds = ["id3", "id4"]
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

        Assert.HasCount(1, users);
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
            AdditionalIds = ["id1", "id2"]
        };

        var user2 = new UserProfile
        {
            Id = "user2",
            Name = "Bob",
            AdditionalIds = ["id3", "id4"]
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // Variables for the expression
        bool notIdIsEmpty = false;
        string notId = "user3";
        string idOrAdditionalId = "id1";

        // This query should filter users by the complex expression
        List<UserProfile> users = await QueryByBase<UserProfile>(pid, notIdIsEmpty, notId, idOrAdditionalId);

        Assert.HasCount(1, users);
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
            AdditionalIds = ["id1", "id2"]
        };

        var user2 = new UserProfile
        {
            Id = "user2",
            Name = "Bob",
            AdditionalIds = ["id3", "id4"]
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);

        // Variables for the expression
        bool notIdIsEmpty = false;
        string notId = "user3";
        string idOrAdditionalId = "id1";

        // This query should filter users by the complex expression
        List<UserProfile> users = await QueryByInterfaceBase<UserProfile>(pid, notIdIsEmpty, notId, idOrAdditionalId);

        Assert.HasCount(1, users);
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

        Assert.HasCount(3, usersWithEmptyDisplayName);
        Assert.IsTrue(usersWithEmptyDisplayName.Any(u => u.Name == "Bob"));
        Assert.IsTrue(usersWithEmptyDisplayName.Any(u => u.Name == "Charlie"));
        Assert.IsTrue(usersWithEmptyDisplayName.Any(u => u.Name == "David"));

        // Test the opposite - users with valid DisplayName
        Expression<Func<UserProfile, bool>> exprNotEmpty = u => !string.IsNullOrWhiteSpace(u.DisplayName);
        var usersWithValidDisplayName = await kv.GetListAsync(pid, exprNotEmpty).ToListAsync();

        Assert.HasCount(1, usersWithValidDisplayName);
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

        Assert.HasCount(2, usersWithVerificationStatus);
        Assert.IsTrue(usersWithVerificationStatus.Any(u => u.Name == "Alice"));
        Assert.IsTrue(usersWithVerificationStatus.Any(u => u.Name == "Bob"));

        // Test the opposite - users where IsVerified is null
        Expression<Func<UserProfile, bool>> exprNoValue = u => !u.IsVerified.HasValue;
        var usersWithoutVerificationStatus = await kv.GetListAsync(pid, exprNoValue).ToListAsync();

        Assert.HasCount(1, usersWithoutVerificationStatus);
        Assert.AreEqual("Charlie", usersWithoutVerificationStatus[0].Name);
    }

    [TestMethod]
    public async Task FilterWithExtensionMethodIsNullOrWhiteSpaceTest()
    {
        var key1 = nameof(FilterWithExtensionMethodIsNullOrWhiteSpaceTest) + "1";
        var key2 = nameof(FilterWithExtensionMethodIsNullOrWhiteSpaceTest) + "2";
        var key3 = nameof(FilterWithExtensionMethodIsNullOrWhiteSpaceTest) + "3";
        var pid = nameof(FilterWithExtensionMethodIsNullOrWhiteSpaceTest);

        var user1 = new UserProfile { Name = "Alice", DisplayName = "Alice Display", Email = "alice@example.com" };
        var user2 = new UserProfile { Name = "Bob", DisplayName = null, Email = null };
        var user3 = new UserProfile { Name = "Charlie", DisplayName = "", Email = "" };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);
        await kv.UpsertAsync(key3, user3, pid);

        // Test extension method with different closure variable values

        // Test 1: Non-null/non-empty string - should return true
        string? validEmail = "alice";
        Expression<Func<UserProfile, bool>> query1 = u => !validEmail.IsNullOrWhiteSpace();
        var result1 = await kv.GetListAsync(pid, query1).ToListAsync();
        Assert.HasCount(3, result1, "!validEmail.IsNullOrWhiteSpace() should return all users");

        // Test 2: Empty string - should return false
        string? emptyEmail = "";
        Expression<Func<UserProfile, bool>> query2 = u => !emptyEmail.IsNullOrWhiteSpace();
        var result2 = await kv.GetListAsync(pid, query2).ToListAsync();
        Assert.IsEmpty(result2, "!emptyEmail.IsNullOrWhiteSpace() should return no users");

        // Test 3: Null string - should return false
        string? nullEmail = null;
        Expression<Func<UserProfile, bool>> query3 = u => !nullEmail.IsNullOrWhiteSpace();
        var result3 = await kv.GetListAsync(pid, query3).ToListAsync();
        Assert.IsEmpty(result3, "!nullEmail.IsNullOrWhiteSpace() should return no users");

        // Test 4: Whitespace string - should return false
        string? whitespaceEmail = "   ";
        Expression<Func<UserProfile, bool>> query4 = u => !whitespaceEmail.IsNullOrWhiteSpace();
        var result4 = await kv.GetListAsync(pid, query4).ToListAsync();
        Assert.IsEmpty(result4, "!whitespaceEmail.IsNullOrWhiteSpace() should return no users");

        // Test 5: Complex expression combining extension method with property checks
        Expression<Func<UserProfile, bool>> query5 = u =>
            !validEmail.IsNullOrWhiteSpace() && u.Name == "Alice";
        var result5 = await kv.GetListAsync(pid, query5).ToListAsync();
        Assert.HasCount(1, result5, "Combined expression should find Alice");
        Assert.AreEqual("Alice", result5[0].Name);

        // Test 6: Verify the static method version still works
        Expression<Func<UserProfile, bool>> query6 = u => string.IsNullOrWhiteSpace(u.DisplayName);
        var result6 = await kv.GetListAsync(pid, query6).ToListAsync();
        Assert.HasCount(2, result6, "Static method should find Bob and Charlie (null/empty DisplayName)");
    }

    [TestMethod]
    public async Task ComplexConditionalExpressionTest()
    {
        var pid = nameof(ComplexConditionalExpressionTest);
        var key1 = pid + "1";
        var key2 = pid + "2";
        var key3 = pid + "3";

        var user1 = new User
        {
            DataType = "Employee",
            Email = "alice@example.com",
            Phone = "123-456-7890",
            Username = "alice123",
            UserId = "user001"
        };

        var user2 = new User
        {
            DataType = "Employee",
            Email = "bob@example.com",
            Phone = "987-654-3210",
            Username = "bobuser",
            UserId = "user002"
        };

        var user3 = new User
        {
            DataType = "Customer",
            Email = "charlie@example.com",
            Phone = "555-123-4567",
            Username = "charlie_c",
            UserId = "user003"
        };

        await kv.UpsertAsync(key1, user1, pid);
        await kv.UpsertAsync(key2, user2, pid);
        await kv.UpsertAsync(key3, user3, pid);

        // Test variables matching the upstream pattern
        bool queryFilters = true;
        string UserDataType = "Employee";
        string filterEmail = "alice";
        string filterPhone = "";
        string filterUsername = "";
        string filterUserId = "";

        // This expression mirrors the upstream code that causes the PostgreSQL syntax error
        Expression<Func<User, bool>> whereQuery = u => !queryFilters ? u.DataType!.Equals(UserDataType) :
                u.DataType!.Equals(UserDataType) && (
                    (!string.IsNullOrWhiteSpace(filterEmail) && u.Email!.Contains(filterEmail, StringComparison.CurrentCultureIgnoreCase)) ||
                    (!string.IsNullOrWhiteSpace(filterPhone) && u.Phone!.Contains(filterPhone, StringComparison.CurrentCultureIgnoreCase)) ||
                    (!string.IsNullOrWhiteSpace(filterUsername) && u.Username!.Contains(filterUsername, StringComparison.CurrentCultureIgnoreCase)) ||
                    (!string.IsNullOrWhiteSpace(filterUserId) && u.UserId!.Contains(filterUserId, StringComparison.CurrentCultureIgnoreCase))
                );

        // This should trigger the PostgreSQL syntax error at position 142
        var users = await kv.GetListAsync(pid, whereQuery).ToListAsync();

        // If the test passes, we expect to find alice@example.com
        Assert.HasCount(1, users);
        Assert.AreEqual("alice@example.com", users[0].Email);
    }

    [TestMethod]
    public async Task SimpleConditionalExpressionTest()
    {
        var pid = nameof(SimpleConditionalExpressionTest);
        var key1 = pid + "1";

        var user1 = new User
        {
            DataType = "Employee",
            Email = "alice@example.com",
            Phone = "123-456-7890",
            Username = "alice123",
            UserId = "user001"
        };

        await kv.UpsertAsync(key1, user1, pid);

        // Simple conditional expression that should trigger the PostgreSQL syntax error
        bool useSimpleFilter = false;
        string targetDataType = "Employee";

        Expression<Func<User, bool>> simpleConditional = u => useSimpleFilter ? u.DataType!.Equals("Customer") : u.DataType!.Equals(targetDataType);

        // This should also trigger the PostgreSQL syntax error at some position
        var users = await kv.GetListAsync(pid, simpleConditional).ToListAsync();

        // If the test passes, we expect to find the employee
        Assert.HasCount(1, users);
        Assert.AreEqual("alice@example.com", users[0].Email);
    }

    // Tests for upstream reported Any() issues with Where().Count() and Where().Any()
    [TestMethod]
    public async Task SimpleWhereCountTest()
    {
        var pid = nameof(SimpleWhereCountTest);
        var key1 = pid + "1";
        var key2 = pid + "2";

        var session1 = new Session
        {
            Id = "session1",
            DownPartyLinks = [
                new PartyLink { Name = "party1", Type = "TypeA" },
                new PartyLink { Name = "party2", Type = "TypeB" }
            ]
        };

        var session2 = new Session
        {
            Id = "session2",
            DownPartyLinks = [
                new PartyLink { Name = "party3", Type = "TypeA" }
            ]
        };

        await kv.UpsertAsync(key1, session1, pid);
        await kv.UpsertAsync(key2, session2, pid);

        // Simple test: just check if Where().Count() > 0 works
        string targetName = "party1";
        Expression<Func<Session, bool>> expr = s => s.DownPartyLinks!.Where(d => d.Name == targetName).Count() > 0;

        var sessions = await kv.GetListAsync(pid, expr).ToListAsync();

        // Should find session1 which has party1
        Assert.HasCount(1, sessions);
        Assert.AreEqual("session1", sessions[0].Id);
    }

    [TestMethod]
    public async Task WhereCountWithNullCheckTest()
    {
        var pid = nameof(WhereCountWithNullCheckTest);
        var key1 = pid + "1";
        var key2 = pid + "2";

        var session1 = new Session
        {
            Id = "session1",
            DownPartyLinks = [
                new PartyLink { Name = "party1", Type = "TypeA" }
            ]
        };

        var session2 = new Session
        {
            Id = "session2",
            DownPartyLinks = null
        };

        await kv.UpsertAsync(key1, session1, pid);
        await kv.UpsertAsync(key2, session2, pid);

        // Test with null check: s.DownPartyLinks != null && Count() > 0
        string targetName = "party1";
        Expression<Func<Session, bool>> expr = s => s.DownPartyLinks != null && s.DownPartyLinks.Where(d => d.Name == targetName).Count() > 0;

        var sessions = await kv.GetListAsync(pid, expr).ToListAsync();

        // Should find session1 which has party1
        Assert.HasCount(1, sessions);
        Assert.AreEqual("session1", sessions[0].Id);
    }

    [TestMethod]
    public async Task WhereCountTest()
    {
        var pid = nameof(WhereCountTest);
        var key1 = pid + "1";
        var key2 = pid + "2";
        var key3 = pid + "3";

        var session1 = new Session
        {
            Id = "session1",
            DownPartyLinks = [
                new PartyLink { Name = "party1", Type = "TypeA" },
                new PartyLink { Name = "party2", Type = "TypeB" }
            ]
        };

        var session2 = new Session
        {
            Id = "session2",
            DownPartyLinks = [
                new PartyLink { Name = "party3", Type = "TypeA" }
            ]
        };

        var session3 = new Session
        {
            Id = "session3",
            DownPartyLinks = []
        };

        await kv.UpsertAsync(key1, session1, pid);
        await kv.UpsertAsync(key2, session2, pid);
        await kv.UpsertAsync(key3, session3, pid);

        // Test pattern from upstream: s.DownPartyLinks.Where(d => d.Name == downPartyName).Count() > 0
        bool queryByDownPartyName = true;
        string downPartyName = "party1";

        Expression<Func<Session, bool>> expr = s =>
            !queryByDownPartyName || (s.DownPartyLinks != null && s.DownPartyLinks.Where(d => d.Name == downPartyName).Count() > 0);

        var sessions = await kv.GetListAsync(pid, expr).ToListAsync();

        // If the test passes, we expect to find session1 which has party1
        Assert.HasCount(1, sessions);
        Assert.AreEqual("session1", sessions[0].Id);
    }

    [TestMethod]
    public async Task WhereAnyTest()
    {
        var pid = nameof(WhereAnyTest);
        var key1 = pid + "1";
        var key2 = pid + "2";
        var key3 = pid + "3";

        var session1 = new Session
        {
            Id = "session1",
            UpPartyLinks = [
                new PartyLink { Name = "upParty1", Type = "TypeA" },
                new PartyLink { Name = "upParty2", Type = "TypeB" }
            ]
        };

        var session2 = new Session
        {
            Id = "session2",
            UpPartyLinks = [
                new PartyLink { Name = "upParty3", Type = "TypeA" }
            ]
        };

        var session3 = new Session
        {
            Id = "session3",
            UpPartyLinks = []
        };

        await kv.UpsertAsync(key1, session1, pid);
        await kv.UpsertAsync(key2, session2, pid);
        await kv.UpsertAsync(key3, session3, pid);

        // Test pattern from upstream: s.UpPartyLinks.Where(u => u.Name == upPartyName).Any()
        bool queryByUpPartyName = true;
        string upPartyName = "upParty1";

        Expression<Func<Session, bool>> expr = s =>
            !queryByUpPartyName || (s.UpPartyLinks != null && s.UpPartyLinks.Where(u => u.Name == upPartyName).Any());

        var sessions = await kv.GetListAsync(pid, expr).ToListAsync();

        // If the test passes, we expect to find session1 which has upParty1
        Assert.HasCount(1, sessions);
        Assert.AreEqual("session1", sessions[0].Id);
    }

    [TestMethod]
    public async Task CombinedUpstreamPatternsTest()
    {
        var pid = nameof(CombinedUpstreamPatternsTest);
        var key1 = pid + "1";
        var key2 = pid + "2";
        var key3 = pid + "3";

        var session1 = new Session
        {
            Id = "session1",
            DownPartyLinks = [
                new PartyLink { Name = "party1", Type = "TypeA" }
            ],
            UpPartyLinks = [
                new PartyLink { Name = "upParty1", Type = "TypeX" }
            ]
        };

        var session2 = new Session
        {
            Id = "session2",
            DownPartyLinks = [
                new PartyLink { Name = "party2", Type = "TypeB" }
            ],
            UpPartyLinks = [
                new PartyLink { Name = "upParty2", Type = "TypeX" }
            ]
        };

        var session3 = new Session
        {
            Id = "session3",
            DownPartyLinks = [
                new PartyLink { Name = "party1", Type = "TypeA" }
            ],
            UpPartyLinks = [
                new PartyLink { Name = "upParty3", Type = "TypeY" }
            ]
        };

        await kv.UpsertAsync(key1, session1, pid);
        await kv.UpsertAsync(key2, session2, pid);
        await kv.UpsertAsync(key3, session3, pid);

        // Test the combined pattern from upstream with all three variations
        bool queryByDownPartyName = true;
        string downPartyName = "party1";
        bool queryByUpPartyName = true;
        string upPartyName = "upParty1";
        bool queryByUpPartyType = true;
        string upPartyType = "TypeX";

        Expression<Func<Session, bool>> expr = s =>
            (!queryByDownPartyName || (s.DownPartyLinks != null && s.DownPartyLinks.Where(d => d.Name == downPartyName).Count() > 0)) &&
            (!queryByUpPartyName || (s.UpPartyLinks != null && s.UpPartyLinks.Where(u => u.Name == upPartyName).Any())) &&
            (!queryByUpPartyType || (s.UpPartyLinks != null && s.UpPartyLinks.Any(u => u.Type == upPartyType)));

        var sessions = await kv.GetListAsync(pid, expr).ToListAsync();

        // If the test passes, we expect to find session1 which matches all criteria
        Assert.HasCount(1, sessions);
        Assert.AreEqual("session1", sessions[0].Id);
    }

    [TestMethod]
    public void ExistsTest()
    {
        var key = nameof(ExistsTest);
        var pid = nameof(ExistsTest);
        Assert.IsFalse(kv.Exists(key, pid));
        kv.Upsert(key, new Poco { Value = key }, pid);
        Assert.IsTrue(kv.Exists(key, pid));
    }

    [TestMethod]
    public async Task ExistsAsyncTest()
    {
        var key = nameof(ExistsAsyncTest);
        var pid = nameof(ExistsAsyncTest);
        Assert.IsFalse(await kv.ExistsAsync(key, pid));
        await kv.UpsertAsync(key, new Poco { Value = key }, pid);
        Assert.IsTrue(await kv.ExistsAsync(key, pid));
        // Expired item should not exist
        await kv.UpsertAsync(key, new Poco { Value = key }, pid, DateTimeOffset.UtcNow.AddMinutes(-1));
        Assert.IsFalse(await kv.ExistsAsync(key, pid));
    }

    [TestMethod]
    public async Task GetAsyncTest()
    {
        var key = nameof(GetAsyncTest);
        var pid = nameof(GetAsyncTest);
        var missing = await kv.GetAsync<Poco>(key, pid);
        Assert.IsNull(missing);
        await kv.UpsertAsync(key, new Poco { Value = key }, pid);
        var result = await kv.GetAsync<Poco>(key, pid);
        Assert.AreEqual(key, result?.Value);
    }

    [TestMethod]
    public async Task CreateAsyncTest()
    {
        var key = nameof(CreateAsyncTest);
        var pid = nameof(CreateAsyncTest);
        var ok = await kv.CreateAsync(key, new Poco { Value = key }, pid);
        Assert.IsTrue(ok);
        var notok = await kv.CreateAsync(key, new Poco { Value = key }, pid);
        Assert.IsFalse(notok);
    }

    [TestMethod]
    public async Task UpdateAsyncTest()
    {
        var key = nameof(UpdateAsyncTest);
        var pid = nameof(UpdateAsyncTest);
        // Update non-existing returns false
        var notok = await kv.UpdateAsync(key, new Poco { Value = "old" }, pid);
        Assert.IsFalse(notok);
        await kv.UpsertAsync(key, new Poco { Value = "old" }, pid);
        var ok = await kv.UpdateAsync(key, new Poco { Value = "new" }, pid);
        Assert.IsTrue(ok);
        var result = await kv.GetAsync<Poco>(key, pid);
        Assert.AreEqual("new", result?.Value);
    }

    [TestMethod]
    public async Task RemoveAsyncTest()
    {
        var key = nameof(RemoveAsyncTest);
        var pid = nameof(RemoveAsyncTest);
        // Remove non-existing returns false
        Assert.IsFalse(await kv.RemoveAsync(key, pid));
        await kv.UpsertAsync(key, new Poco { Value = key }, pid);
        Assert.IsTrue(await kv.RemoveAsync(key, pid));
        Assert.IsFalse(await kv.ExistsAsync(key, pid));
    }

    [TestMethod]
    public async Task CountAsyncBasicTest()
    {
        var key1 = nameof(CountAsyncBasicTest) + "1";
        var key2 = nameof(CountAsyncBasicTest) + "2";
        var pid = nameof(CountAsyncBasicTest);
        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);
        var count = await kv.CountAsync(pid);
        Assert.AreEqual(2, count);
    }

    [TestMethod]
    public async Task RemoveAllAsyncBasicTest()
    {
        var key1 = nameof(RemoveAllAsyncBasicTest) + "1";
        var key2 = nameof(RemoveAllAsyncBasicTest) + "2";
        var pid = nameof(RemoveAllAsyncBasicTest);
        await kv.UpsertAsync(key1, new Poco { Value = key1 }, pid);
        await kv.UpsertAsync(key2, new Poco { Value = key2 }, pid);
        var removed = await kv.RemoveAllAsync(pid);
        Assert.AreEqual(2, removed);
        Assert.AreEqual(0, await kv.CountAsync(pid));
    }

    [TestMethod]
    public async Task RemoveAllExpiredAsyncTest()
    {
        var key = nameof(RemoveAllExpiredAsyncTest);
        var pid = nameof(RemoveAllExpiredAsyncTest);
        await kv.UpsertAsync(key, new Poco { Value = key }, pid);
        var removed1 = await kv.RemoveAllExpiredAsync(pid);
        Assert.AreEqual(0, removed1);
        await kv.UpsertAsync(key, new Poco { Value = key }, pid, DateTimeOffset.UtcNow.AddMinutes(-1));
        var removed2 = await kv.RemoveAllExpiredAsync(pid);
        Assert.AreEqual(1, removed2);
    }

    [TestMethod]
    public async Task RemoveAllExpiredGlobalAsyncTest()
    {
        var key = nameof(RemoveAllExpiredGlobalAsyncTest);
        var pid = nameof(RemoveAllExpiredGlobalAsyncTest);
        // Non-expired item should survive a global cleanup
        await kv.UpsertAsync(key, new Poco { Value = key }, pid);
        await kv.RemoveAllExpiredGlobalAsync();
        Assert.AreEqual(1, await kv.CountAsync(pid));
        // Expired item should be removed by global cleanup
        await kv.UpsertAsync(key, new Poco { Value = key }, pid, DateTimeOffset.UtcNow.AddMinutes(-1));
        await kv.RemoveAllExpiredGlobalAsync();
        Assert.AreEqual(0, await kv.CountAsync(pid));
    }

    [TestMethod]
    public async Task GreaterThanOrEqualOperatorTest()
    {
        var pid = nameof(GreaterThanOrEqualOperatorTest);
        await kv.UpsertAsync(pid + "1", new UserProfile { Name = "A", Age = 20 }, pid);
        await kv.UpsertAsync(pid + "2", new UserProfile { Name = "B", Age = 30 }, pid);
        await kv.UpsertAsync(pid + "3", new UserProfile { Name = "C", Age = 40 }, pid);

        var ge30 = await kv.GetListAsync<UserProfile>(pid, u => u.Age >= 30).ToListAsync();
        Assert.HasCount(2, ge30);

        var le30 = await kv.GetListAsync<UserProfile>(pid, u => u.Age <= 30).ToListAsync();
        Assert.HasCount(2, le30);

        var between = await kv.GetListAsync<UserProfile>(pid, u => u.Age >= 25 && u.Age <= 35).ToListAsync();
        Assert.HasCount(1, between);
        Assert.AreEqual("B", between[0].Name);
    }

    [TestMethod]
    public async Task NumericTypeFilterTest()
    {
        var pid = nameof(NumericTypeFilterTest);
        var item1 = new TypedModel { Name = "item1", ShortVal = 10, LongVal = 1000L, DecimalVal = 9.99m, DoubleVal = 1.5, FloatVal = 2.5f };
        var item2 = new TypedModel { Name = "item2", ShortVal = 20, LongVal = 2000L, DecimalVal = 19.99m, DoubleVal = 3.5, FloatVal = 4.5f };
        await kv.UpsertAsync(pid + "1", item1, pid);
        await kv.UpsertAsync(pid + "2", item2, pid);

        var byShort = await kv.GetListAsync<TypedModel>(pid, m => m.ShortVal >= 15).ToListAsync();
        Assert.HasCount(1, byShort);
        Assert.AreEqual("item2", byShort[0].Name);

        var byLong = await kv.GetListAsync<TypedModel>(pid, m => m.LongVal <= 1500L).ToListAsync();
        Assert.HasCount(1, byLong);
        Assert.AreEqual("item1", byLong[0].Name);

        var byDecimal = await kv.GetListAsync<TypedModel>(pid, m => m.DecimalVal > 15m).ToListAsync();
        Assert.HasCount(1, byDecimal);
        Assert.AreEqual("item2", byDecimal[0].Name);

        var byDouble = await kv.GetListAsync<TypedModel>(pid, m => m.DoubleVal > 2.0).ToListAsync();
        Assert.HasCount(1, byDouble);
        Assert.AreEqual("item2", byDouble[0].Name);

        var byFloat = await kv.GetListAsync<TypedModel>(pid, m => m.FloatVal > 3.0f).ToListAsync();
        Assert.HasCount(1, byFloat);
        Assert.AreEqual("item2", byFloat[0].Name);
    }

    [TestMethod]
    public async Task DateTimeOffsetFilterTest()
    {
        var pid = nameof(DateTimeOffsetFilterTest);
        var now = DateTimeOffset.UtcNow;
        var item1 = new TypedModel { Name = "old", CreatedAt = now.AddDays(-10) };
        var item2 = new TypedModel { Name = "new", CreatedAt = now.AddDays(-1) };
        await kv.UpsertAsync(pid + "1", item1, pid);
        await kv.UpsertAsync(pid + "2", item2, pid);

        var cutoff = now.AddDays(-5);
        var recent = await kv.GetListAsync<TypedModel>(pid, m => m.CreatedAt > cutoff).ToListAsync();
        Assert.HasCount(1, recent);
        Assert.AreEqual("new", recent[0].Name);
    }

    [TestMethod]
    public async Task NonNullableBoolPropertyTest()
    {
        var pid = nameof(NonNullableBoolPropertyTest);
        await kv.UpsertAsync(pid + "1", new ModelWithBool { Name = "active", IsActive = true }, pid);
        await kv.UpsertAsync(pid + "2", new ModelWithBool { Name = "inactive", IsActive = false }, pid);

        // Property on left (member == constant) - first branch in VisitBinary bool case
        var active = await kv.GetListAsync<ModelWithBool>(pid, m => m.IsActive == true).ToListAsync();
        Assert.HasCount(1, active);
        Assert.AreEqual("active", active[0].Name);

        // Constant on left (constant == member) - else branch in VisitBinary bool case
        var alsoActive = await kv.GetListAsync<ModelWithBool>(pid, m => true == m.IsActive).ToListAsync();
        Assert.HasCount(1, alsoActive);
        Assert.AreEqual("active", alsoActive[0].Name);

        // Inactive items
        var inactive = await kv.GetListAsync<ModelWithBool>(pid, m => m.IsActive == false).ToListAsync();
        Assert.HasCount(1, inactive);
        Assert.AreEqual("inactive", inactive[0].Name);
    }

    [TestMethod]
    public async Task NullCapturedVariableTest()
    {
        var pid = nameof(NullCapturedVariableTest);
        await kv.UpsertAsync(pid + "1", new Poco { Value = "something" }, pid);
        string? nullFilter = null;
        // Null captured variable — AddParameter called with null value
        var result = await kv.GetListAsync<Poco>(pid, p => p.Value == nullFilter).ToListAsync();
        Assert.IsEmpty(result);
    }

    [TestMethod]
    public async Task FieldAccessTest()
    {
        var pid = nameof(FieldAccessTest);
        await kv.UpsertAsync(pid + "1", new ModelWithField { Name = "alpha", Value = 1 }, pid);
        await kv.UpsertAsync(pid + "2", new ModelWithField { Name = "beta", Value = 2 }, pid);

        // Accessing public fields triggers FieldInfo path in GetMemberType
        var result = await kv.GetListAsync<ModelWithField>(pid, m => m.Name == "alpha").ToListAsync();
        Assert.HasCount(1, result);
        Assert.AreEqual("alpha", result[0].Name);

        var byValue = await kv.GetListAsync<ModelWithField>(pid, m => m.Value > 1).ToListAsync();
        Assert.HasCount(1, byValue);
        Assert.AreEqual("beta", byValue[0].Name);
    }

    [TestMethod]
    public async Task ToLowerToUpperFilterTest()
    {
        var pid = nameof(ToLowerToUpperFilterTest);
        await kv.UpsertAsync(pid + "1", new Poco { Value = "Hello" }, pid);
        await kv.UpsertAsync(pid + "2", new Poco { Value = "World" }, pid);

        var lower = await kv.GetListAsync<Poco>(pid, p => p.Value!.ToLower() == "hello").ToListAsync();
        Assert.HasCount(1, lower);

        var upper = await kv.GetListAsync<Poco>(pid, p => p.Value!.ToUpper() == "WORLD").ToListAsync();
        Assert.HasCount(1, upper);
    }

    [TestMethod]
    public async Task EndsWithFilterTest()
    {
        var pid = nameof(EndsWithFilterTest);
        await kv.UpsertAsync(pid + "1", new Poco { Value = "foobar" }, pid);
        await kv.UpsertAsync(pid + "2", new Poco { Value = "bazqux" }, pid);

        var result = await kv.GetListAsync<Poco>(pid, p => p.Value!.EndsWith("bar")).ToListAsync();
        Assert.HasCount(1, result);
        Assert.AreEqual("foobar", result[0].Value);
    }

    [TestMethod]
    public async Task NestedPropertyHasValueTest()
    {
        var pid = nameof(NestedPropertyHasValueTest);
        var user1 = new UserProfile { Name = "Alice", PrimaryAddress = new Address { ZipCode = 10001 } };
        var user2 = new UserProfile { Name = "Bob", PrimaryAddress = null };
        await kv.UpsertAsync(pid + "1", user1, pid);
        await kv.UpsertAsync(pid + "2", user2, pid);

        // Test null check on nested property (not HasValue, just null check)
        var withAddress = await kv.GetListAsync<UserProfile>(pid, u => u.PrimaryAddress != null).ToListAsync();
        Assert.HasCount(1, withAddress);
        Assert.AreEqual("Alice", withAddress[0].Name);
    }

    [TestMethod]
    public async Task CountWithTagsTest()
    {
        var pid = nameof(CountWithTagsTest);
        var user1 = new UserProfile { Name = "Alice", Tags = ["vip", "user"] };
        var user2 = new UserProfile { Name = "Bob", Tags = ["user"] };
        var user3 = new UserProfile { Name = "Charlie", Tags = [] };
        await kv.UpsertAsync(pid + "1", user1, pid);
        await kv.UpsertAsync(pid + "2", user2, pid);
        await kv.UpsertAsync(pid + "3", user3, pid);

        // Count() on collection via LINQ
        var withVip = await kv.GetListAsync<UserProfile>(pid, u => u.Tags!.Count() > 1).ToListAsync();
        Assert.HasCount(1, withVip);
        Assert.AreEqual("Alice", withVip[0].Name);
    }

    [TestMethod]
    public async Task ClosureNullableHasValueTest()
    {
        var pid = nameof(ClosureNullableHasValueTest);
        await kv.UpsertAsync(pid + "1", new Poco { Value = "a" }, pid);
        await kv.UpsertAsync(pid + "2", new Poco { Value = "b" }, pid);

        // Closure nullable with value — HasValue evaluates to true, all records match
        int? withValue = 5;
        var all = await kv.GetListAsync<Poco>(pid, p => withValue.HasValue).ToListAsync();
        Assert.HasCount(2, all);

        // Closure nullable without value — HasValue evaluates to false, no records match
        int? withoutValue = null;
        var none = await kv.GetListAsync<Poco>(pid, p => withoutValue.HasValue).ToListAsync();
        Assert.IsEmpty(none);
    }

    [TestMethod]
    public async Task NestedNullableHasValueTest()
    {
        var pid = nameof(NestedNullableHasValueTest);
        await kv.UpsertAsync(pid + "1", new OuterItem { Name = "has-score", Details = new InnerItem { Score = 42 } }, pid);
        await kv.UpsertAsync(pid + "2", new OuterItem { Name = "no-score", Details = new InnerItem { Score = null } }, pid);
        await kv.UpsertAsync(pid + "3", new OuterItem { Name = "no-details", Details = null }, pid);

        // Details.Score.HasValue — nested nullable property HasValue check
        var withScore = await kv.GetListAsync<OuterItem>(pid, m => m.Details != null && m.Details.Score.HasValue).ToListAsync();
        Assert.HasCount(1, withScore);
        Assert.AreEqual("has-score", withScore[0].Name);
    }

    [TestMethod]
    public async Task DateTimeFieldFilterTest()
    {
        var pid = nameof(DateTimeFieldFilterTest);
        var now = DateTime.UtcNow;
        await kv.UpsertAsync(pid + "1", new ScheduledItem { Name = "past", ScheduledAt = now.AddDays(-2) }, pid);
        await kv.UpsertAsync(pid + "2", new ScheduledItem { Name = "future", ScheduledAt = now.AddDays(2) }, pid);

        DateTime cutoff = now;
        var future = await kv.GetListAsync<ScheduledItem>(pid, m => m.ScheduledAt > cutoff).ToListAsync();
        Assert.HasCount(1, future);
        Assert.AreEqual("future", future[0].Name);
    }

    // Covers lines 374-377: closure-captured object whose property is accessed as a nested member
    [TestMethod]
    public async Task ClosureObjectPropertyFilterTest()
    {
        var pid = nameof(ClosureObjectPropertyFilterTest);
        await kv.UpsertAsync(pid + "1", new Poco { Value = "alpha" }, pid);
        await kv.UpsertAsync(pid + "2", new Poco { Value = "beta" }, pid);

        // filter.Value is a MemberAccess on a Constant (closure), hitting the
        // parentMember.Expression?.NodeType == ExpressionType.Constant path in VisitMember
        var filter = new Poco { Value = "alpha" };
        var result = await kv.GetListAsync<Poco>(pid, p => p.Value == filter.Value).ToListAsync();
        Assert.HasCount(1, result);
        Assert.AreEqual("alpha", result[0].Value);
    }

    // Covers lines 426-432: triple-nested property traversal in BuildNestedJsonPath
    [TestMethod]
    public async Task TripleNestedPropertyTest()
    {
        var pid = nameof(TripleNestedPropertyTest);
        var item1 = new Deep1 { Name = "found", Middle = new Deep2 { Inner = new Deep3 { Value = "deep-val" } } };
        var item2 = new Deep1 { Name = "other", Middle = new Deep2 { Inner = new Deep3 { Value = "different" } } };
        await kv.UpsertAsync(pid + "1", item1, pid);
        await kv.UpsertAsync(pid + "2", item2, pid);

        // u.Middle.Inner.Value requires BuildNestedJsonPath to traverse 3 levels,
        // hitting the else-if(MemberExpression parent) branch for the intermediate level
        var result = await kv.GetListAsync<Deep1>(pid, u => u.Middle!.Inner!.Value == "deep-val").ToListAsync();
        Assert.HasCount(1, result);
        Assert.AreEqual("found", result[0].Name);
    }

    // Covers line 623: GetNpgsqlType Guid branch
    [TestMethod]
    public async Task GuidFilterTest()
    {
        var pid = nameof(GuidFilterTest);
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        await kv.UpsertAsync(pid + "1", new ModelWithGuid { Name = "first", ExternalId = id1 }, pid);
        await kv.UpsertAsync(pid + "2", new ModelWithGuid { Name = "second", ExternalId = id2 }, pid);

        // Closure Guid triggers GetNpgsqlType(typeof(Guid)) → NpgsqlDbType.Uuid
        var result = await kv.GetListAsync<ModelWithGuid>(pid, m => m.ExternalId == id1).ToListAsync();
        Assert.HasCount(1, result);
        Assert.AreEqual("first", result[0].Name);
    }

    // Covers line 627: GetNpgsqlType Int16 (Smallint) branch
    [TestMethod]
    public async Task ShortClosureVariableTest()
    {
        var pid = nameof(ShortClosureVariableTest);
        var item1 = new TypedModel { Name = "low", ShortVal = 5 };
        var item2 = new TypedModel { Name = "high", ShortVal = 20 };
        await kv.UpsertAsync(pid + "1", item1, pid);
        await kv.UpsertAsync(pid + "2", item2, pid);

        // A short closure variable makes AddParameter receive typeof(short),
        // flowing through GetNpgsqlType → TypeCode.Int16 → NpgsqlDbType.Smallint
        short threshold = 10;
        var result = await kv.GetListAsync<TypedModel>(pid, m => m.ShortVal > threshold).ToListAsync();
        Assert.HasCount(1, result);
        Assert.AreEqual("high", result[0].Name);
    }

    // Covers line 633: GetNpgsqlType Boolean branch via the null-value code path
    [TestMethod]
    public async Task NullNullableBoolClosureTest()
    {
        var pid = nameof(NullNullableBoolClosureTest);
        await kv.UpsertAsync(pid + "1", new UserProfile { Name = "Alice", IsVerified = true }, pid);
        await kv.UpsertAsync(pid + "2", new UserProfile { Name = "Bob", IsVerified = false }, pid);

        // A null bool? closure variable causes AddParameter(null, typeof(bool?)) →
        // null path → GetNpgsqlType(bool?) → strips nullable → TypeCode.Boolean
        bool? filter = null;
        // SQL: col = NULL → always NULL → no rows returned
        var result = await kv.GetListAsync<UserProfile>(pid, u => u.IsVerified == filter).ToListAsync();
        Assert.IsEmpty(result);
    }
}