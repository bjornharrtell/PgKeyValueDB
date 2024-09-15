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
        dataSource.Execute($@"create table if not exists {tableName} (
    pid text check (char_length(id) between 1 and 255),
    id text check (char_length(id) between 1 and 255),
    value jsonb not null,
    created timestamptz not null,
    updated timestamptz,
    expires timestamptz,
    primary key (pid, id)
)", prepare: false);
        dataSource.Execute($@"create index if not exists {tableName}_created_idx on {tableName} (created)", prepare: false);
        dataSource.Execute($@"create index if not exists {tableName}_updated_idx on {tableName} (updated) where updated is not null", prepare: false);
        dataSource.Execute($@"create index if not exists {tableName}_expires_idx on {tableName} (expires) where expires is not null", prepare: false);
    }

    static NpgsqlParameter[] CreateParams(string pid, string? id = null)
    {
        var list = new List<NpgsqlParameter> { new() { Value = pid } };
        if (id != null)
            list.Add(new() { Value = id });
        return [.. list];
    }
    static NpgsqlParameter[] CreateParams(string pid, long? limit)
    {
        var list = new List<NpgsqlParameter>
        {
            new() { Value = pid },
            new() { Value = limit != null ? limit : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bigint }
        };
        return [.. list];
    }
    static NpgsqlParameter[] CreateParams<T>(string pid, string? id, T? value, DateTimeOffset? expires)
    {
        var list = new List<NpgsqlParameter> { new() { Value = pid } };
        if (id != null)
            list.Add(new() { Value = id });
        if (value != null)
        {
            list.Add(new() { Value = value, NpgsqlDbType = NpgsqlDbType.Jsonb });
            list.Add(new() { Value = (object?)expires ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        }
        return [.. list];
    }

    string SelectSql =>
        $"select value from {tableName} where pid = $1 and id = $2 and (expires is null or now() < expires)";
    string SelectSetSql =>
        $"select value from {tableName} where pid = $1 and (expires is null or now() < expires) limit $2";
    string CreateCreateSql =>
        $"insert into {tableName} (pid, id, value, created, expires) values ($1, $2, $3, now(), $4)";
    string UpdateSql =>
        $"update {tableName} set value = $3, updated = now(), expires = $4 where pid = $1 and id = $2";
    string UpsertSql =>
        $"insert into {tableName} (pid, id, value, created, expires) values ($1, $2, $3, now(), $4) on conflict (pid, id) do update set value = $3, updated = now(), expires = $4";
    string DeleteSql =>
        $"delete from {tableName} where pid = $1 and id = $2";
    string DeleteAllSql =>
        $"delete from {tableName} where pid = $1";
    string DeleteAllExpiredSql =>
        $"delete from {tableName} where pid = $1 and now() >= expires";
    string ExistsSql =>
        $"select exists(select 1 from {tableName} where pid = $1 and id = $2 and (expires is null or now() < expires))";
    string CountSql =>
        $"select count(1) from {tableName} where pid = $1 and (expires is null or now() < expires)";

    public bool Create<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        dataSource.Execute(CreateCreateSql, CreateParams(pid, id, value, expires)) > 0;
    public async Task<bool> CreateAsync<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        await dataSource.ExecuteAsync(CreateCreateSql, CreateParams(pid, id, value, expires)) > 0;
    public bool Update<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        dataSource.Execute(UpdateSql, CreateParams(pid, id, value, expires)) > 0;
    public async Task<bool> UpdateAsync<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        await dataSource.ExecuteAsync(UpdateSql, CreateParams(pid, id, value, expires)) > 0;
    public bool Upsert<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        dataSource.Execute(UpsertSql, CreateParams(pid, id, value, expires)) > 0;
    public async Task<bool> UpsertAsync<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        await dataSource.ExecuteAsync(UpsertSql, CreateParams(pid, id, value, expires)) > 0;
    public bool Remove(string id, string pid = DEFAULT_PID) =>
        dataSource.Execute(DeleteSql, CreateParams(pid, id)) > 0;
    public async Task<bool> RemoveAsync(string id, string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync(DeleteSql, CreateParams(pid, id)) > 0;
    public int RemoveAll(string pid = DEFAULT_PID) =>
        dataSource.Execute(DeleteAllSql, CreateParams(pid));
    public async Task<int> RemoveAllAsync(string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync(DeleteAllSql, CreateParams(pid));
    public int RemoveAllExpired(string pid = DEFAULT_PID) =>
        dataSource.Execute(DeleteAllExpiredSql, CreateParams(pid));
    public async Task<int> RemoveAllExpiredAsync(string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync(DeleteAllExpiredSql, CreateParams(pid));
    public T? Get<T>(string id, string pid = DEFAULT_PID) =>
        dataSource.Execute<T>(SelectSql, CreateParams(pid, id));
    public async Task<T?> GetAsync<T>(string id, string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync<T>(SelectSql, CreateParams(pid, id));
    public HashSet<T> GetHashSet<T>(string pid = DEFAULT_PID, long? limit = null) =>
        dataSource.ExecuteSet<T>(SelectSetSql, CreateParams(pid, limit));
    public async Task<HashSet<T>> GetHashSetAsync<T>(string pid = DEFAULT_PID, long? limit = null) =>
        await dataSource.ExecuteSetAsync<T>(SelectSetSql, CreateParams(pid, limit));
    public bool Exists(string id, string pid = DEFAULT_PID) =>
        dataSource.Execute<bool>(ExistsSql, CreateParams(pid, id));
    public async Task<bool> ExistsAsync(string id, string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync<bool>(ExistsSql, CreateParams(pid, id));
    public long Count(string pid = DEFAULT_PID) =>
        dataSource.Execute<long>(CountSql, CreateParams(pid));
    public async Task<long> CountAsync(string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync<long>(CountSql, CreateParams(pid));
}
