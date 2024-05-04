using Npgsql;
using NpgsqlTypes;

namespace Wololo.PgKeyValueDB;

public class PgKeyValueDB
{
    readonly NpgsqlDataSource dataSource;
    readonly string tableName;

    const string DEFAULT_PID = "default";

    public PgKeyValueDB(NpgsqlDataSource dataSource, string tableName)
    {
        this.dataSource = dataSource;
        this.tableName = tableName;
        Init();
    }

    private void Init()
    {
        dataSource.CreateCommand($@"create table if not exists {tableName} (
    pid text check (char_length(id) between 1 and 255),
    id text check (char_length(id) between 1 and 255),
    value jsonb not null,
    created timestamptz not null,
    updated timestamptz,
    expires timestamptz,
    primary key (pid, id)
)")
            .ExecuteNonQuery();
        dataSource.CreateCommand($@"create index if not exists {tableName}_created_idx on {tableName} (created)")
            .ExecuteNonQuery();
        dataSource.CreateCommand($@"create index if not exists {tableName}_updated_idx on {tableName} (updated) where updated is not null")
            .ExecuteNonQuery();
        dataSource.CreateCommand($@"create index if not exists {tableName}_expires_idx on {tableName} (expires) where expires is not null")
            .ExecuteNonQuery();
    }

    private NpgsqlCommand CreateSetCommand<T>(NpgsqlConnection conn, string pid, string id, T value, DateTimeOffset? expires) =>
        new($"insert into {tableName} (pid, id, value, created, expires) values ($1, $2, $3, now(), $4) on conflict (pid, id) do update set value = $3, updated = now(), expires = $4", conn)
        {
            Parameters =
            {
                new() { Value = pid },
                new() { Value = id },
                new() { Value = value, NpgsqlDbType = NpgsqlDbType.Jsonb },
                new() { Value = (object?) expires ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.TimestampTz }
            }
        };

    public void Set<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateSetCommand(conn, pid, id, value, expires);
        cmd.Prepare();
        cmd.ExecuteNonQuery();
    }

    public async Task SetAsync<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateSetCommand(conn, pid, id, value, expires);
        await cmd.PrepareAsync();
        await cmd.ExecuteNonQueryAsync();
    }

    private NpgsqlCommand CreateRemoveCommand(NpgsqlConnection conn, string pid, string id) =>
        new($"delete from {tableName} where pid = $1 and id = $2", conn)
        {
            Parameters = { new() { Value = pid }, new() { Value = id } }
        };

    public bool Remove(string id, string pid = DEFAULT_PID)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateRemoveCommand(conn, pid, id);
        cmd.Prepare();
        return cmd.ExecuteNonQuery() > 0;
    }

    public async Task<bool> RemoveAsync(string id, string pid = DEFAULT_PID)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateRemoveCommand(conn, pid, id);
        await cmd.PrepareAsync();
        return await cmd.ExecuteNonQueryAsync() > 0;
    }

    private NpgsqlCommand CreateRemoveAllCommand(NpgsqlConnection conn, string pid) =>
        new($"delete from {tableName} where pid = $1", conn)
        {
            Parameters = { new() { Value = pid } }
        };

    public int RemoveAll(string pid = DEFAULT_PID)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateRemoveAllCommand(conn, pid);
        cmd.Prepare();
        return cmd.ExecuteNonQuery();
    }

    public async Task<int> RemoveAllAsync(string pid = DEFAULT_PID)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateRemoveAllCommand(conn, pid);
        await cmd.PrepareAsync();
        return await cmd.ExecuteNonQueryAsync();
    }

    private NpgsqlCommand CreateRemoveAllExpiredCommand(NpgsqlConnection conn, string pid) =>
        new($"delete from {tableName} where pid = $1 and now() >= expires", conn)
        {
            Parameters = { new() { Value = pid } }
        };

    public int RemoveAllExpired(string pid = DEFAULT_PID)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateRemoveAllExpiredCommand(conn, pid);
        cmd.Prepare();
        return cmd.ExecuteNonQuery();
    }

    public async Task<int> RemoveAllExpiredAsync(string pid = DEFAULT_PID)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateRemoveAllExpiredCommand(conn, pid);
        await cmd.PrepareAsync();
        return await cmd.ExecuteNonQueryAsync();
    }

    private NpgsqlCommand CreateGetCommand(NpgsqlConnection conn, string pid, string id) =>
        new($"select value from {tableName} where pid = $1 and id = $2 and (expires is null or now() < expires)", conn)
        {
            Parameters = { new() { Value = pid }, new() { Value = id } }
        };

    public T? Get<T>(string id, string pid = DEFAULT_PID)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateGetCommand(conn, pid, id);
        cmd.Prepare();
        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            return default;
        var value = reader.GetFieldValue<T>(0);
        return value;
    }

    public async Task<T?> GetAsync<T>(string id, string pid = DEFAULT_PID)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateGetCommand(conn, pid, id);
        await cmd.PrepareAsync();
        using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return default;
        var value = await reader.GetFieldValueAsync<T>(0);
        return value;
    }

    private NpgsqlCommand CreateGetHashSetCommand(NpgsqlConnection conn, string pid, long limit) =>
        new($"select value from {tableName} where pid = $1 and (expires is null or now() < expires) limit $2", conn)
        {
            Parameters = { new() { Value = pid }, new() { Value = limit } }
        };

    public HashSet<T> GetHashSet<T>(string pid = DEFAULT_PID, long limit = 0)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateGetHashSetCommand(conn, pid, limit);
        cmd.Prepare();
        using var reader = cmd.ExecuteReader();
        var set = new HashSet<T>();
        while (reader.Read())
            set.Add(reader.GetFieldValue<T>(0));
        return set;
    }

    public async Task<HashSet<T>> GetHashSetAsync<T>(string pid = DEFAULT_PID, long limit = 0)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateGetHashSetCommand(conn, pid, limit);
        await cmd.PrepareAsync();
        using var reader = await cmd.ExecuteReaderAsync();
        var set = new HashSet<T>();
        while (await reader.ReadAsync())
            set.Add(await reader.GetFieldValueAsync<T>(0));
        return set;
    }

    private NpgsqlCommand CreateExistsCommand(NpgsqlConnection conn, string pid, string id) =>
        new($"select exists(select 1 from {tableName} where pid = $1 and id = $2 and (expires is null or now() < expires))", conn)
        {
            Parameters = { new() { Value = pid }, new() { Value = id } }
        };

    public bool Exists(string id, string pid = DEFAULT_PID)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateExistsCommand(conn, pid, id);
        cmd.Prepare();
        using var reader = cmd.ExecuteReader();
        reader.Read();
        var value = reader.GetBoolean(0);
        return value;
    }

    public async Task<bool> ExistsAsync(string id, string pid = DEFAULT_PID)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateExistsCommand(conn, pid, id);
        await cmd.PrepareAsync();
        using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        var value = reader.GetBoolean(0);
        return value;
    }

    private NpgsqlCommand CreateCountCommand(NpgsqlConnection conn, string pid) =>
        new($"select count(1) from {tableName} where pid = $1 and (expires is null or now() < expires)", conn)
        {
            Parameters = { new() { Value = pid } }
        };

    public long Count(string pid = DEFAULT_PID)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = CreateCountCommand(conn, pid);
        cmd.Prepare();
        using var reader = cmd.ExecuteReader();
        reader.Read();
        var value = reader.GetInt64(0);
        return value;
    }

    public async Task<long> CountAsync(string pid = DEFAULT_PID)
    {
        using var conn = await dataSource.OpenConnectionAsync();
        using var cmd = CreateCountCommand(conn, pid);
        await cmd.PrepareAsync();
        using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        var value = reader.GetInt64(0);
        return value;
    }
}
