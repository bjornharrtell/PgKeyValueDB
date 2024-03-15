using NpgsqlTypes;

namespace Npgsql.DocumentDB;

public class NpgsqlDocumentDB
{
    readonly NpgsqlDataSource dataSource; 

    public NpgsqlDocumentDB(NpgsqlDataSource dataSource)
    {
        this.dataSource = dataSource;
        Init();
    }

    private void Init()
    {
        dataSource.CreateCommand("create table if not exists kv (key text primary key check (char_length(key) <= 255), value jsonb not null)").ExecuteNonQuery();
    }

    private static NpgsqlCommand CreateSetCommand<T>(NpgsqlConnection conn, string key, T value) =>
        new("insert into kv (key, value) values ($1, $2) on conflict (key) do update set value = $2", conn) {
            Parameters = { new() { Value = key }, new() { Value = value, NpgsqlDbType = NpgsqlDbType.Jsonb } }
        };

    public void Set<T>(string key, T value)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateSetCommand(conn, key, value);
        cmd.Prepare();
        cmd.ExecuteNonQuery();
    }

    public async Task SetAsync<T>(string key, T value)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateSetCommand(conn, key, value);
        await cmd.PrepareAsync();
        await cmd.ExecuteNonQueryAsync();
    }

    private static NpgsqlCommand CreateRemoveCommand(NpgsqlConnection conn, string key) =>
        new("delete from kv where key = $1", conn) {
            Parameters = { new() { Value = key }, new() { Value = key } }
        };

    public bool Remove(string key)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateRemoveCommand(conn, key);
        cmd.Prepare();
        return cmd.ExecuteNonQuery() > 0;
    }

    public async Task<bool> RemoveAsync(string key)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateRemoveCommand(conn, key);
        await cmd.PrepareAsync();
        return await cmd.ExecuteNonQueryAsync() > 0;
    }

    private static NpgsqlCommand CreateGetCommand(NpgsqlConnection conn, string key) =>
        new("select value from kv where key = $1", conn) {
            Parameters = { new() { Value = key } }
        };

    public T? Get<T>(string key)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateGetCommand(conn, key);
        cmd.Prepare();
        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            return default;
        var value = reader.GetFieldValue<T>(0);
        return value;
    }

    public async Task<T?> GetAsync<T>(string key)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateGetCommand(conn, key);
        await cmd.PrepareAsync();
        using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return default;
        var value = await reader.GetFieldValueAsync<T>(0);
        return value;
    }

    private static NpgsqlCommand CreateExistsCommand(NpgsqlConnection conn, string key) =>
        new("select exists(select 1 from kv where key = $1)", conn) {
            Parameters = { new() { Value = key } }
        };

    public bool Exists(string key)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateExistsCommand(conn, key);
        cmd.Prepare();
        using var reader = cmd.ExecuteReader();
        reader.Read();
        var value = reader.GetBoolean(0);
        return value;
    }

    public async Task<bool> ExistsAsync(string key)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateExistsCommand(conn, key);
        await cmd.PrepareAsync();
        using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        var value = reader.GetBoolean(0);
        return value;
    }
}
