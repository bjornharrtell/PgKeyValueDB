using Npgsql;

static class NpgsqlDataSourceExtensions
{
    internal static int Execute(this NpgsqlDataSource dataSource, string sql, NpgsqlParameter[]? parameters = null, bool prepare = true)
    {
        try
        {
            using var conn = dataSource.OpenConnection();
            using var cmd = new NpgsqlCommand(sql, conn);
            if (parameters != null)
                foreach (var parameter in parameters)
                    cmd.Parameters.Add(parameter);
            if (prepare)
                cmd.Prepare();
            return cmd.ExecuteNonQuery();
        }
        catch (PostgresException e)
        {
            if (e.SqlState == PostgresErrorCodes.UniqueViolation)
                return 0;
            else throw;
        }
    }

    internal static async Task<int> ExecuteAsync(this NpgsqlDataSource dataSource, string sql, NpgsqlParameter[]? parameters = null, bool prepare = true)
    {
        try
        {
            await using var conn = await dataSource.OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand(sql, conn);
            if (parameters != null)
                foreach (var parameter in parameters)
                    cmd.Parameters.Add(parameter);
            if (prepare)
                await cmd.PrepareAsync();
            return await cmd.ExecuteNonQueryAsync();
        }
        catch (PostgresException e)
        {
            if (e.SqlState == PostgresErrorCodes.UniqueViolation)
                return 0;
            else throw;
        }
    }

    internal static T? Execute<T>(this NpgsqlDataSource dataSource, string sql, NpgsqlParameter[]? parameters = null, bool prepare = true)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = new NpgsqlCommand(sql, conn);
        if (parameters != null)
            foreach (var parameter in parameters)
                cmd.Parameters.Add(parameter);
        if (prepare)
            cmd.Prepare();
        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            return default;
        var value = reader.GetFieldValue<T>(0);
        return value;
    }

    internal static async Task<T?> ExecuteAsync<T>(this NpgsqlDataSource dataSource, string sql, NpgsqlParameter[]? parameters = null, bool prepare = true)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        if (parameters != null)
            foreach (var parameter in parameters)
                cmd.Parameters.Add(parameter);
        if (prepare)
            cmd.Prepare();
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return default;
        var value = await reader.GetFieldValueAsync<T>(0);
        return value;
    }

    internal static async IAsyncEnumerable<T> ExecuteListAsync<T>(this NpgsqlDataSource dataSource, string sql, NpgsqlParameter[]? parameters = null, bool prepare = true)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        if (parameters != null)
            foreach (var parameter in parameters)
                cmd.Parameters.Add(parameter);
        if (prepare)
            cmd.Prepare();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (reader.Read())
            yield return reader.GetFieldValue<T>(0);
    }
}