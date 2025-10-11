using Npgsql;

namespace Wololo.PgKeyValueDB;

internal static class NpgsqlDataSourceExtensions
{
    public record struct NpgsqlCommandContext(string Sql, IEnumerable<NpgsqlParameter>? Parameters = null, bool Prepare = true);

    internal static int Execute(this NpgsqlDataSource dataSource, NpgsqlCommandContext context)
    {
        try
        {
            using var conn = dataSource.OpenConnection();
            using var cmd = new NpgsqlCommand(context.Sql, conn);
            if (context.Parameters != null)
                foreach (var parameter in context.Parameters)
                    cmd.Parameters.Add(parameter);
            if (context.Prepare)
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

    internal static T? Execute<T>(this NpgsqlDataSource dataSource, NpgsqlCommandContext context)
    {
        using var conn = dataSource.OpenConnection();
        using var cmd = new NpgsqlCommand(context.Sql, conn);
        if (context.Parameters != null)
            foreach (var parameter in context.Parameters)
                cmd.Parameters.Add(parameter);
        if (context.Prepare)
            cmd.Prepare();
        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
            return default;
        var value = reader.GetFieldValue<T>(0);
        return value;
    }

    internal static async Task<int> ExecuteAsync(this NpgsqlDataSource dataSource, NpgsqlCommandContext context, CancellationToken token)
    {
        try
        {
            await using var conn = await dataSource.OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand(context.Sql, conn);
            if (context.Parameters != null)
                foreach (var parameter in context.Parameters)
                    cmd.Parameters.Add(parameter);
            if (context.Prepare)
                await cmd.PrepareAsync(token);
            return await cmd.ExecuteNonQueryAsync(token);
        }
        catch (PostgresException e)
        {
            if (e.SqlState == PostgresErrorCodes.UniqueViolation)
                return 0;
            else throw;
        }
    }

    internal static async Task<T?> ExecuteAsync<T>(this NpgsqlDataSource dataSource, NpgsqlCommandContext context, CancellationToken token)
    {
        await using var conn = await dataSource.OpenConnectionAsync(token);
        await using var cmd = new NpgsqlCommand(context.Sql, conn);
        if (context.Parameters != null)
            foreach (var parameter in context.Parameters)
                cmd.Parameters.Add(parameter);
        if (context.Prepare)
            await cmd.PrepareAsync(token);
        await using var reader = await cmd.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token))
            return default;
        var value = await reader.GetFieldValueAsync<T>(0);
        return value;
    }

    internal static async IAsyncEnumerable<T> ExecuteListAsync<T>(this NpgsqlDataSource dataSource, NpgsqlCommandContext context, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(context.Sql, conn);
        if (context.Parameters != null)
            foreach (var parameter in context.Parameters)
                cmd.Parameters.Add(parameter);
        if (context.Prepare)
            await cmd.PrepareAsync(token);
        await using var reader = await cmd.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
            yield return await reader.GetFieldValueAsync<T>(0);
    }
}