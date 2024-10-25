using System.Linq.Expressions;
using Npgsql;
using NpgsqlTypes;
using Ctx = NpgsqlDataSourceExtensions.NpgsqlCommandContext;

namespace Wololo.PgKeyValueDB;

public class PgKeyValueDB
{
    private readonly NpgsqlDataSource dataSource;
    private readonly string schemaName;
    private readonly string tableName;
    private readonly string tableRef;

    const string DEFAULT_PID = "default";

    public PgKeyValueDB(NpgsqlDataSource dataSource, string schemaName, string tableName)
    {
        this.dataSource = dataSource;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableRef = $"{schemaName}.{tableName}";
        Init();
    }

    private void Init()
    {
        dataSource.Execute(new Ctx($@"create schema if not exists {schemaName}"));

        dataSource.Execute(new Ctx($@"create table if not exists {tableRef} (
    pid text check (char_length(id) between 1 and 255),
    id text check (char_length(id) between 1 and 255),
    value jsonb not null,
    created timestamptz not null,
    updated timestamptz,
    expires timestamptz,
    primary key (pid, id)
)", Prepare: false));
        dataSource.Execute(new Ctx($@"create index if not exists idx_{schemaName}_{tableName}_created on {tableRef} (created)", Prepare: false));
        dataSource.Execute(new Ctx($@"create index if not exists idx_{schemaName}_{tableName}_updated on {tableRef} (updated) where updated is not null", Prepare: false));
        dataSource.Execute(new Ctx($@"create index if not exists idx_{schemaName}_{tableName}_expires on {tableRef} (expires) where expires is not null", Prepare: false));
    }

    static IEnumerable<NpgsqlParameter> CreateParams(string pid, string? id = null)
    {
        var baseParams = new List<NpgsqlParameter> { new() { ParameterName = "pid", Value = pid } };
        if (id != null)
            baseParams.Add(new() { ParameterName = "pid", Value = id });
        return baseParams;
    }

    static IEnumerable<NpgsqlParameter> CreateParams<T>(string pid, string? id, T? value, DateTimeOffset? expires)
    {
        var baseParams = new List<NpgsqlParameter> { new() { ParameterName = "pid", Value = pid } };
        if (id != null)
            baseParams.Add(new() { ParameterName = "id", Value = id });
        if (value != null)
        {
            baseParams.Add(new() { ParameterName = "value", Value = value, NpgsqlDbType = NpgsqlDbType.Jsonb });
            baseParams.Add(new() { ParameterName = "expires", Value = (object?)expires ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        }
        return baseParams;
    }

    string SelectSql =>
        $"select value from {tableRef} where pid = @pid and id = @id and (expires is null or now() < expires)";
    string CreateCreateSql =>
        $"insert into {tableRef} (pid, id, value, created, expires) values (@pid, @id, @value, now(), @expires)";
    string UpdateSql =>
        $"update {tableRef} set value = @value, updated = now(), expires = @expires where pid = @pid and id = @id";
    string UpsertSql =>
        $"insert into {tableRef} (pid, id, value, created, expires) values (@pid, @id, @value, now(), @expires) on conflict (pid, id) do update set value = @value, updated = now(), expires = @expires";
    string DeleteSql =>
        $"delete from {tableRef} where pid = @pid and id = @id";
    string DeleteAllSql =>
        $"delete from {tableRef} where pid = @pid";
    string DeleteAllExpiredSql =>
        $"delete from {tableRef} where pid = @pid and now() >= expires";
    string ExistsSql =>
        $"select exists(select 1 from {tableRef} where pid = @pid and id = @id and (expires is null or now() < expires))";
    string CountSql =>
        $"select count(1) from {tableRef} where pid = @pid and (expires is null or now() < expires)";

    public bool Create<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        dataSource.Execute(new Ctx(CreateCreateSql, CreateParams(pid, id, value, expires))) > 0;
    public async Task<bool> CreateAsync<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        await dataSource.ExecuteAsync(new Ctx(CreateCreateSql, CreateParams(pid, id, value, expires))) > 0;
    public bool Update<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        dataSource.Execute(new Ctx(UpdateSql, CreateParams(pid, id, value, expires))) > 0;
    public async Task<bool> UpdateAsync<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        await dataSource.ExecuteAsync(new Ctx(UpdateSql, CreateParams(pid, id, value, expires))) > 0;
    public bool Upsert<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        dataSource.Execute(new Ctx(UpsertSql, CreateParams(pid, id, value, expires))) > 0;
    public async Task<bool> UpsertAsync<T>(string id, T value, string pid = DEFAULT_PID, DateTimeOffset? expires = null) =>
        await dataSource.ExecuteAsync(new Ctx(UpsertSql, CreateParams(pid, id, value, expires))) > 0;
    public bool Remove(string id, string pid = DEFAULT_PID) =>
        dataSource.Execute(new Ctx(DeleteSql, CreateParams(pid, id))) > 0;
    public async Task<bool> RemoveAsync(string id, string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync(new Ctx(DeleteSql, CreateParams(pid, id))) > 0;
    public int RemoveAll(string pid = DEFAULT_PID) =>
        dataSource.Execute(new Ctx(DeleteAllSql, CreateParams(pid)));
    public int RemoveAll<T>(string pid = DEFAULT_PID, Expression<Func<T, bool>>? where = null) =>
        dataSource.Execute(BuildCommandParams(DeleteAllSql, pid, where));
    public async Task<int> RemoveAllAsync(string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync(new Ctx(DeleteAllSql, CreateParams(pid)));
    public async Task<int> RemoveAllAsync<T>(string pid = DEFAULT_PID, Expression<Func<T, bool>>? where = null) =>
        await dataSource.ExecuteAsync(BuildCommandParams(DeleteAllSql, pid, where));
    public int RemoveAllExpired(string pid = DEFAULT_PID) =>
        dataSource.Execute(new Ctx(DeleteAllExpiredSql, CreateParams(pid)));
    public async Task<int> RemoveAllExpiredAsync(string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync(new Ctx(DeleteAllExpiredSql, CreateParams(pid)));
    public T? Get<T>(string id, string pid = DEFAULT_PID) =>
        dataSource.Execute<T>(new Ctx(SelectSql, CreateParams(pid, id)));
    public async Task<T?> GetAsync<T>(string id, string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync<T>(new Ctx(SelectSql, CreateParams(pid, id)));
    public bool Exists(string id, string pid = DEFAULT_PID) =>
        dataSource.Execute<bool>(new Ctx(ExistsSql, CreateParams(pid, id)));
    public async Task<bool> ExistsAsync(string id, string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync<bool>(new Ctx(ExistsSql, CreateParams(pid, id)));
    public long Count(string pid = DEFAULT_PID) =>
        dataSource.Execute<long>(new Ctx(CountSql, CreateParams(pid)));
    public long Count<T>(string pid = DEFAULT_PID, Expression<Func<T, bool>>? where = null) =>
        dataSource.Execute<long>(BuildCommandParams(CountSql, pid, where));
    public async Task<long> CountAsync(string pid = DEFAULT_PID) =>
        await dataSource.ExecuteAsync<long>(new Ctx(CountSql, CreateParams(pid)));
    public async Task<long> CountAsync<T>(string pid = DEFAULT_PID, Expression<Func<T, bool>>? where = null) =>
        await dataSource.ExecuteAsync<long>(BuildCommandParams(CountSql, pid, where));


    private static Ctx BuildCommandParams<T>(string sql, string pid = DEFAULT_PID, Expression<Func<T, bool>>? where = null)
    {
        var baseParams = new List<NpgsqlParameter>
        {
            new() { ParameterName = "pid", Value = pid }
        };
        if (where != null)
        {
            var visitor = new SqlExpressionVisitor(typeof(T));
            visitor.Visit(where);
            baseParams.AddRange(visitor.Parameters);
            sql = $"{sql} AND {visitor.WhereClause}";
        }
        return new Ctx(sql, baseParams);
    }

    public IAsyncEnumerable<T> GetListAsync<T>(string pid = DEFAULT_PID, Expression<Func<T, bool>>? where = null, long? limit = null, long? offset = null)
    {
        var sql = $"select value from {tableRef} where pid = @pid and (expires is null or now() < expires)";
        var baseParams = new List<NpgsqlParameter>
        {
            new() { ParameterName = "pid", Value = pid },
            new() { ParameterName = "limit", Value = limit != null ? limit : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bigint },
            new() { ParameterName = "offset", Value = offset != null ? offset : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bigint }
        };
        if (where != null)
        {
            var visitor = new SqlExpressionVisitor(typeof(T));
            visitor.Visit(where);
            baseParams.AddRange(visitor.Parameters);
            sql = $"{sql} AND {visitor.WhereClause} limit @limit offset @offset";
        }
        else
        {
            sql += " limit @limit offset @offset";
        }
        var result = dataSource.ExecuteListAsync<T>(new Ctx(sql, baseParams));
        return result;
    }

}
