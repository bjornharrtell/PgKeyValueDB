using NpgsqlTypes;

namespace Npgsql.DocumentDB;

public class NpgsqlDocumentDB
{
    NpgsqlDataSource dataSource; 

    public NpgsqlDocumentDB(NpgsqlDataSource dataSource)
    {
        this.dataSource = dataSource;
        Init();
    }

    private void Init()
    {
        dataSource.CreateCommand("create table if not exists kv (key text primary key check (char_length(key) <= 255), value jsonb not null)").ExecuteNonQuery();
    }

    public void Set<T>(string key, T value)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = new NpgsqlCommand("insert into kv (key, value) values ($1, $2)", conn)
        {
            Parameters = { new() { Value = key }, new() { Value = value, NpgsqlDbType = NpgsqlDbType.Jsonb } }
        };
        cmd.ExecuteNonQuery();
    }

    public T? Get<T>(string key)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = new NpgsqlCommand("select value from kv where key = $1", conn)
        {
            Parameters = { new() { Value = key } }
        };
        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            return default;
        var value = reader.GetFieldValue<T>(0);
        return value;
    }
}
