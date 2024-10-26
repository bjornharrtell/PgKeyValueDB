using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace Wololo.PgKeyValueDB;

internal class SqlExpressionVisitor(Type documentType) : ExpressionVisitor
{
    private readonly List<NpgsqlParameter> parameters = [];
    private readonly StringBuilder whereClause = new();
    private int parameterIndex;
    private readonly Type documentType = documentType;

    public string WhereClause => whereClause.ToString();
    public NpgsqlParameter[] Parameters => parameters.ToArray();

    protected override Expression VisitBinary(BinaryExpression node)
    {
        whereClause.Append('(');
        Visit(node.Left);

        string op = node.NodeType switch
        {
            ExpressionType.Equal => " = ",
            ExpressionType.NotEqual => " != ",
            ExpressionType.GreaterThan => " > ",
            ExpressionType.GreaterThanOrEqual => " >= ",
            ExpressionType.LessThan => " < ",
            ExpressionType.LessThanOrEqual => " <= ",
            ExpressionType.AndAlso => " AND ",
            ExpressionType.OrElse => " OR ",
            _ => throw new NotSupportedException($"Operation {node.NodeType} is not supported")
        };

        whereClause.Append(op);
        Visit(node.Right);
        whereClause.Append(')');

        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(string))
        {
            switch (node.Method.Name)
            {
                case nameof(object.Equals) or nameof(string.Equals):
                    // Handle both instance and static method calls
                    if (node.Object != null)
                    {
                        // Instance method: str.Equals(other)
                        Visit(node.Object);
                        whereClause.Append(" = ");
                        Visit(node.Arguments[0]);
                    }
                    else
                    {
                        // Static method: string.Equals(str1, str2)
                        Visit(node.Arguments[0]);
                        whereClause.Append(" = ");
                        Visit(node.Arguments[1]);
                    }
                    return node;

                case nameof(string.StartsWith):
                    Visit(node.Object);
                    whereClause.Append(" LIKE ");
                    Visit(node.Arguments[0]);
                    whereClause.Append(" || '%'");
                    return node;

                case nameof(string.EndsWith):
                    Visit(node.Object);
                    whereClause.Append(" LIKE '%' || ");
                    Visit(node.Arguments[0]);
                    return node;

                case nameof(string.Contains):
                    Visit(node.Object);
                    whereClause.Append(" LIKE '%' || ");
                    Visit(node.Arguments[0]);
                    whereClause.Append(" || '%'");
                    return node;

                case nameof(string.ToLower):
                    whereClause.Append("LOWER(");
                    Visit(node.Object);
                    whereClause.Append(")");
                    return node;

                case nameof(string.ToUpper):
                    whereClause.Append("UPPER(");
                    Visit(node.Object);
                    whereClause.Append(")");
                    return node;

                case nameof(string.Trim):
                    whereClause.Append("TRIM(");
                    Visit(node.Object);
                    whereClause.Append(")");
                    return node;

                case nameof(string.TrimStart):
                    whereClause.Append("LTRIM(");
                    Visit(node.Object);
                    whereClause.Append(")");
                    return node;

                case nameof(string.TrimEnd):
                    whereClause.Append("RTRIM(");
                    Visit(node.Object);
                    whereClause.Append(")");
                    return node;

                case nameof(string.Replace):
                    whereClause.Append("REPLACE(");
                    Visit(node.Object);
                    whereClause.Append(", ");
                    Visit(node.Arguments[0]);
                    whereClause.Append(", ");
                    Visit(node.Arguments[1]);
                    whereClause.Append(")");
                    return node;

                case nameof(string.Length):
                    whereClause.Append("LENGTH(");
                    Visit(node.Object);
                    whereClause.Append(")");
                    return node;
            }
        }

        throw new NotSupportedException($"Method {node.Method.Name} is not supported");
    }

    private static Type GetMemberType(MemberInfo member) => member switch
    {
        PropertyInfo prop => prop.PropertyType,
        FieldInfo field => field.FieldType,
        _ => throw new NotSupportedException($"Member type {member.MemberType} is not supported")
    };

    private static (NpgsqlDbType npgsqlType, string postgresType) GetTypeMapping(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        if (type.IsEnum)
            return (NpgsqlDbType.Text, "text");

        return type switch
        {
            Type t when t == typeof(int) => (NpgsqlDbType.Integer, "integer"),
            Type t when t == typeof(long) => (NpgsqlDbType.Bigint, "bigint"),
            Type t when t == typeof(short) => (NpgsqlDbType.Smallint, "smallint"),
            Type t when t == typeof(decimal) => (NpgsqlDbType.Numeric, "numeric"),
            Type t when t == typeof(double) => (NpgsqlDbType.Double, "double precision"),
            Type t when t == typeof(float) => (NpgsqlDbType.Real, "real"),
            Type t when t == typeof(bool) => (NpgsqlDbType.Boolean, "boolean"),
            Type t when t == typeof(string) => (NpgsqlDbType.Text, "text"),
            Type t when t == typeof(DateTime) => (NpgsqlDbType.Timestamp, "timestamp"),
            Type t when t == typeof(DateTimeOffset) => (NpgsqlDbType.TimestampTz, "timestamptz"),
            Type t when t == typeof(Guid) => (NpgsqlDbType.Uuid, "uuid"),
            _ => (NpgsqlDbType.Text, "text")
        };
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        parameterIndex++;
        var value = node.Value;
        var (npgsqlType, _) = GetTypeMapping(node.Type);

        // Convert enum values to their string representation for parameters
        if (value != null && value.GetType().IsEnum)
        {
            value = value.ToString();
        }

        var parameter = new NpgsqlParameter
        {
            ParameterName = $"p{parameterIndex}",
            Value = value ?? DBNull.Value,
            NpgsqlDbType = npgsqlType
        };
        parameters.Add(parameter);
        whereClause.Append($"@{parameter.ParameterName}");
        return node;
    }

    private static string BuildJsonPath(MemberInfo member)
    {
        var memberType = member switch
        {
            PropertyInfo prop => prop.PropertyType,
            FieldInfo field => field.FieldType,
            _ => throw new NotSupportedException($"Member type {member.MemberType} is not supported")
        };

        var (_, postgresType) = GetTypeMapping(memberType);
        return $"cast(value ->> '{member.Name}' as {postgresType})";
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // Handle property access on a constant
        if (node.Expression?.NodeType == ExpressionType.Constant)
        {
            var value = Expression.Lambda(node).Compile().DynamicInvoke();
            parameterIndex++;
            var (npgsqlType, _) = GetTypeMapping(node.Type);

            var parameter = new NpgsqlParameter
            {
                ParameterName = $"p{parameterIndex}",
                Value = value ?? DBNull.Value,
                NpgsqlDbType = npgsqlType
            };
            parameters.Add(parameter);
            whereClause.Append($"@{parameter.ParameterName}");
            return node;
        }

        // Handle property access on the parameter (p => p.Value)
        if (node.Expression?.NodeType == ExpressionType.Parameter)
        {
            whereClause.Append(BuildJsonPath(node.Member));
            return node;
        }

        // Handle property access in method chains (p => p.Value.Length)
        if (node.Expression is MemberExpression memberExpr)
        {
            if (node.Member.DeclaringType == typeof(string))
            {
                Visit(node.Expression);
                return node;
            }

            whereClause.Append(BuildJsonPath(node.Member));
            return node;
        }

        throw new NotSupportedException($"Unsupported member access: {node.Member.Name} on {node.Expression?.NodeType}");
    }

    private Expression BuildPropertyAccess(PropertyInfo property)
    {
        var jsonPath = $"cast(value ->> '{property.Name}' as {GetPostgresType(property.PropertyType)})";
        whereClause.Append(jsonPath);
        return Expression.Property(null, property);
    }

    private static bool IsSimpleType(Type type)
    {
        return type.IsPrimitive
            || type == typeof(string)
            || type == typeof(decimal)
            || type == typeof(DateTime)
            || type == typeof(DateTimeOffset)
            || type == typeof(Guid)
            || (Nullable.GetUnderlyingType(type)?.IsPrimitive ?? false);
    }

    private static NpgsqlDbType GetNpgsqlType(Type type)
    {
        // Handle enum types
        if (type.IsEnum)
        {
            return NpgsqlDbType.Integer; // Store enums as integers
        }

        return Type.GetTypeCode(Nullable.GetUnderlyingType(type) ?? type) switch
        {
            TypeCode.Int16 or TypeCode.Int32 or TypeCode.Int64 => NpgsqlDbType.Bigint,
            TypeCode.Decimal => NpgsqlDbType.Numeric,
            TypeCode.Double or TypeCode.Single => NpgsqlDbType.Double,
            TypeCode.Boolean => NpgsqlDbType.Boolean,
            TypeCode.String or TypeCode.Char => NpgsqlDbType.Text,
            _ => NpgsqlDbType.Text
        };
    }

    private static string GetPostgresType(Type type)
    {
        // Handle enum types
        if (type.IsEnum)
        {
            return "text"; // Compare as text since that's how it's stored in JSONB
        }

        return Type.GetTypeCode(Nullable.GetUnderlyingType(type) ?? type) switch
        {
            TypeCode.Int16 or TypeCode.Int32 or TypeCode.Int64 => "numeric",
            TypeCode.Decimal or TypeCode.Double or TypeCode.Single => "numeric",
            TypeCode.Boolean => "boolean",
            TypeCode.String or TypeCode.Char => "text",
            _ => "text"
        };
    }
}