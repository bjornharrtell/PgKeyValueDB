using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;
using Wololo.PgKeyValueDB;

namespace Microsoft.Extensions.DependencyInjection;

public static class NpgsqlServiceCollectionExtensions
{
    public static IServiceCollection AddPgKeyValueDB(this IServiceCollection serviceCollection, string connectionString, Action<PgKeyValueDBBuilder>? action = null, ServiceLifetime dataSourceLifetime = ServiceLifetime.Singleton, object? serviceKey = null)
    {
        serviceCollection.TryAdd(new ServiceDescriptor(typeof(PgKeyValueDB), serviceKey, delegate (IServiceProvider sp, object? key)
        {
            PgKeyValueDBBuilder builder = new(connectionString, key);
            action?.Invoke(builder);
            return builder.Build();
        }, dataSourceLifetime));
        return serviceCollection;
    }
}

public class PgKeyValueDBBuilder(string connectionString, object? serviceKey = null)
{
    const string DEFAULT_TABLE_NAME = "npgsql_documentdb";

    public string TableName { get; set; } = DEFAULT_TABLE_NAME;

    string CreateTableName() => serviceKey == null ? TableName : $"{TableName}_{serviceKey}";

    internal object Build()
    {
        var dataSource = new NpgsqlDataSourceBuilder(connectionString).EnableDynamicJson().Build();
        var tableName = CreateTableName();
        return new PgKeyValueDB(dataSource, tableName);
    }
}