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

    protected override Expression VisitMember(MemberExpression node)
    {
        // Handle property access on a constant (e.g., accessing a variable in the closure)
        if (node.Expression?.NodeType == ExpressionType.Constant)
        {
            // Compile and evaluate the expression to get the actual value
            var value = Expression.Lambda(node).Compile().DynamicInvoke();
            parameterIndex++;
            var parameter = new NpgsqlParameter
            {
                ParameterName = $"p{parameterIndex}",
                Value = value ?? DBNull.Value,
                NpgsqlDbType = GetNpgsqlType(node.Type)
            };
            parameters.Add(parameter);
            whereClause.Append($"@{parameter.ParameterName}");
            return node;
        }

        // Handle property access on the parameter (e.g., p => p.Value)
        if (node.Expression?.NodeType == ExpressionType.Parameter)
        {
            var property = node.Member as PropertyInfo;
            if (property == null)
                throw new NotSupportedException("Only properties are supported");

            // Allow both document type properties and string properties (for string methods)
            var isDocumentProperty = property.DeclaringType == documentType;
            var isStringProperty = property.PropertyType == typeof(string);

            if (!isDocumentProperty && !isStringProperty)
                throw new NotSupportedException($"Property '{property.Name}' must be either from the document type or a string type");

            var path = BuildJsonPath(property);
            whereClause.Append(path);
            return node;
        }

        // Handle property access in method chains (e.g., p.Value.Length)
        if (node.Expression is MemberExpression)
        {
            var property = node.Member as PropertyInfo;
            if (property == null)
                throw new NotSupportedException("Only properties are supported");

            // For method chains, allow string properties
            if (property.DeclaringType != typeof(string))
                throw new NotSupportedException("Only string properties are supported in method chains");

            whereClause.Append(node.Member.Name.ToLower());
            return node;
        }

        throw new NotSupportedException($"Unsupported member expression type: {node.Expression?.NodeType}");
    }

    private static string BuildJsonPath(PropertyInfo property)
    {
        // For simple types, we can directly cast to text
        if (IsSimpleType(property.PropertyType))
        {
            return $"cast(value ->> '{property.Name}' as {GetPostgresType(property.PropertyType)})";
        }

        throw new NotSupportedException($"Complex property types are not supported: {property.PropertyType}");
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

    private static string GetPostgresType(Type type)
    {
        return Type.GetTypeCode(Nullable.GetUnderlyingType(type) ?? type) switch
        {
            TypeCode.Int16 or TypeCode.Int32 or TypeCode.Int64 => "numeric",
            TypeCode.Decimal or TypeCode.Double or TypeCode.Single => "numeric",
            TypeCode.Boolean => "boolean",
            TypeCode.String or TypeCode.Char => "text",
            _ => "text" // Default to text for unknown types
        };
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        parameterIndex++;
        var parameter = new NpgsqlParameter
        {
            ParameterName = $"p{parameterIndex}",
            Value = node.Value ?? DBNull.Value,
            NpgsqlDbType = GetNpgsqlType(node.Type)
        };
        parameters.Add(parameter);
        whereClause.Append($"@{parameter.ParameterName}");
        return node;
    }

    private static NpgsqlDbType GetNpgsqlType(Type type)
    {
        return Type.GetTypeCode(Nullable.GetUnderlyingType(type) ?? type) switch
        {
            TypeCode.Int16 or TypeCode.Int32 or TypeCode.Int64 => NpgsqlDbType.Bigint,
            TypeCode.Decimal => NpgsqlDbType.Numeric,
            TypeCode.Double or TypeCode.Single => NpgsqlDbType.Double,
            TypeCode.Boolean => NpgsqlDbType.Boolean,
            TypeCode.String or TypeCode.Char => NpgsqlDbType.Text,
            _ => NpgsqlDbType.Text // Default to text for unknown types
        };
    }
}