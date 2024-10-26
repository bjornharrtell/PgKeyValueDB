using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace Wololo.PgKeyValueDB;

public class SqlExpressionVisitor(Type documentType) : ExpressionVisitor
{
    private readonly List<NpgsqlParameter> parameters = [];
    private readonly StringBuilder whereClause = new();
    private int parameterIndex;
    private readonly Type documentType = documentType;

    public string WhereClause => whereClause.ToString();
    public NpgsqlParameter[] Parameters => parameters.ToArray();

    // Add this method to get the complete SQL for debugging
    public string GetDebugSql()
    {
        var sql = new StringBuilder();
        sql.AppendLine("Generated SQL:");
        sql.AppendLine(WhereClause);
        sql.AppendLine("\nParameters:");
        foreach (var param in Parameters)
        {
            sql.AppendLine($"  @{param.ParameterName} = {param.Value} ({param.NpgsqlDbType}) [{param.Value?.GetType()}]");
        }
        return sql.ToString();
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        whereClause.Append('(');

        // Special case for direct boolean comparisons with constants
        if (node.NodeType == ExpressionType.Equal &&
            (node.Left.Type == typeof(bool) || node.Right.Type == typeof(bool)))
        {
            if (node.Left.NodeType == ExpressionType.MemberAccess)
            {
                Visit(node.Left);
                whereClause.Append("::boolean = ");
                Visit(node.Right);
            }
            else
            {
                Visit(node.Left);
                whereClause.Append(" = ");
                Visit(node.Right);
                whereClause.Append("::boolean");
            }
        }
        else
        {
            Visit(node.Left);
            whereClause.Append(GetOperator(node.NodeType));
            Visit(node.Right);
        }

        whereClause.Append(')');
        return node;
    }

    private static bool IsNumericType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type == typeof(int) || type == typeof(long) || type == typeof(decimal) ||
               type == typeof(double) || type == typeof(float) || type == typeof(short);
    }

    private static string GetCommonNumericType(Type type1, Type type2)
    {
        if (type1 == typeof(decimal) || type2 == typeof(decimal)) return "numeric";
        if (type1 == typeof(double) || type2 == typeof(double)) return "double precision";
        if (type1 == typeof(float) || type2 == typeof(float)) return "real";
        if (type1 == typeof(long) || type2 == typeof(long)) return "bigint";
        return "integer";
    }

    private static string GetOperator(ExpressionType nodeType) => nodeType switch
    {
        ExpressionType.Equal => " = ",
        ExpressionType.NotEqual => " != ",
        ExpressionType.GreaterThan => " > ",
        ExpressionType.GreaterThanOrEqual => " >= ",
        ExpressionType.LessThan => " < ",
        ExpressionType.LessThanOrEqual => " <= ",
        ExpressionType.AndAlso => " AND ",
        ExpressionType.OrElse => " OR ",
        _ => throw new NotSupportedException($"Operation {nodeType} is not supported")
    };

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Special handling for enum ToString()
        if (node.Method.Name == nameof(ToString) && node.Object?.Type.IsEnum == true)
        {
            whereClause.Append($"CASE (");
            Visit(node.Object);
            whereClause.Append(")");

            // Add WHEN clauses for each enum value
            var enumType = node.Object.Type;
            var enumValues = Enum.GetValues(enumType);
            foreach (var enumValue in enumValues)
            {
                whereClause.Append($" WHEN {Convert.ToInt32(enumValue)} THEN '{enumValue}'");
            }
            whereClause.Append(" END");

            return node;
        }

        if (node.Method.DeclaringType == typeof(string))
        {
            switch (node.Method.Name)
            {
                case nameof(string.Contains):
                    bool isCaseInsensitive = false;

                    // Check for case insensitive comparison
                    if (node.Arguments.Count == 2 && node.Arguments[1].Type == typeof(StringComparison))
                    {
                        var comparisonValue = Expression.Lambda(node.Arguments[1]).Compile().DynamicInvoke();
                        isCaseInsensitive = comparisonValue is StringComparison comparison &&
                            (comparison == StringComparison.OrdinalIgnoreCase ||
                             comparison == StringComparison.CurrentCultureIgnoreCase);
                    }

                    Visit(node.Object);
                    whereClause.Append(isCaseInsensitive ? " ILIKE " : " LIKE ");

                    // Get the search value and create pattern parameter
                    var searchValue = Expression.Lambda(node.Arguments[0]).Compile().DynamicInvoke()?.ToString();
                    parameterIndex++;
                    var paramName = $"p{parameterIndex}";
                    parameters.Add(new NpgsqlParameter
                    {
                        ParameterName = paramName,
                        Value = $"%{searchValue}%",
                        NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text
                    });
                    whereClause.Append($"@{paramName}");

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

                case nameof(object.Equals) or nameof(string.Equals):
                    if (node.Object != null)
                    {
                        Visit(node.Object);
                        whereClause.Append(" = ");
                        Visit(node.Arguments[0]);
                    }
                    else
                    {
                        Visit(node.Arguments[0]);
                        whereClause.Append(" = ");
                        Visit(node.Arguments[1]);
                    }
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
            }
        }

        throw new NotSupportedException($"Method {node.Method.Name} is not supported");
    }

    private static string BuildJsonPath(MemberInfo member, string parentPath = "value")
    {
        var memberType = GetMemberType(member);
        string cast;
        if (memberType.IsEnum)
        {
            cast = "::integer";
        }
        else if (IsNumericType(memberType))
        {
            cast = memberType switch
            {
                Type t when t == typeof(decimal) => "::numeric",
                Type t when t == typeof(double) => "::double precision",
                Type t when t == typeof(float) => "::real",
                Type t when t == typeof(long) => "::bigint",
                _ => "::integer"
            };
        }
        else if (memberType == typeof(bool) || memberType == typeof(bool?))
        {
            cast = "::bool";
        }
        else
        {
            cast = "::text";
        }
        if (parentPath != "value")
        {
            return $"({parentPath} ->> '{member.Name}'){cast}";
        }
        return $"(value ->> '{member.Name}'){cast}";
    }

    private static Type GetMemberType(MemberInfo member) => member switch
    {
        PropertyInfo prop => prop.PropertyType,
        FieldInfo field => field.FieldType,
        _ => throw new NotSupportedException($"Member type {member.MemberType} is not supported")
    };

    protected override Expression VisitMember(MemberExpression node)
    {
        // Handle closure/captured variables
        if (node.Expression?.NodeType == ExpressionType.Constant)
        {
            var value = Expression.Lambda(node).Compile().DynamicInvoke();
            AddParameter(value, node.Type);
            return node;
        }

        // Handle nested properties in the JSON document
        if (node.Expression is MemberExpression parentMember)
        {
            // Check if this is accessing a closure field
            if (parentMember.Expression?.NodeType == ExpressionType.Constant)
            {
                var value = Expression.Lambda(node).Compile().DynamicInvoke();
                AddParameter(value, node.Type);
                return node;
            }

            var parentPath = BuildNestedJsonPath(parentMember);
            whereClause.Append(BuildJsonPath(node.Member, parentPath));
            return node;
        }

        // Handle root-level properties in the JSON document
        if (node.Expression?.NodeType == ExpressionType.Parameter)
        {
            whereClause.Append(BuildJsonPath(node.Member));
            return node;
        }

        throw new NotSupportedException($"Unsupported member access: {node.Member.Name} on {node.Expression?.NodeType}");
    }

    private string BuildNestedJsonPath(MemberExpression expression)
    {
        var path = new Stack<string>();
        var current = expression;

        while (current != null)
        {
            if (current.Expression?.NodeType == ExpressionType.Parameter)
            {
                path.Push(current.Member.Name);
                break;
            }
            else if (current.Expression is MemberExpression parent)
            {
                path.Push(current.Member.Name);
                current = parent;
            }
            else
                break;
        }

        var jsonPath = "value";
        foreach (var segment in path.Reverse())
        {
            jsonPath = $"({jsonPath} -> '{segment}')";
        }
        return jsonPath;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        AddParameter(node.Value, node.Type);
        return node;
    }

    private static bool IsToStringCall(Expression expr)
    {
        return expr is MethodCallExpression methodCall &&
               methodCall.Method.Name == nameof(ToString);
    }

    private void AddParameter(object? value, Type type)
    {
        parameterIndex++;
        var paramName = $"p{parameterIndex}";

        if (value == null)
        {
            parameters.Add(new NpgsqlParameter
            {
                ParameterName = paramName,
                Value = DBNull.Value,
                NpgsqlDbType = GetNpgsqlType(type)
            });
            whereClause.Append($"@{paramName}");
            return;
        }

        if (type == typeof(bool))
        {
            parameters.Add(new NpgsqlParameter
            {
                ParameterName = paramName,
                Value = value,
                NpgsqlDbType = NpgsqlDbType.Boolean
            });
            whereClause.Append($"@{paramName}::boolean");
            return;
        }

        if (type.IsEnum)
        {
            // Store enum as integer if not using ToString
            var lastExpression = whereClause.ToString().TrimEnd();
            if (!lastExpression.Contains("CASE"))
            {
                parameters.Add(new NpgsqlParameter
                {
                    ParameterName = paramName,
                    Value = Convert.ToInt32(value),
                    NpgsqlDbType = NpgsqlDbType.Integer
                });
            }
            else
            {
                // For ToString comparisons, use the string value
                parameters.Add(new NpgsqlParameter
                {
                    ParameterName = paramName,
                    Value = value.ToString(),
                    NpgsqlDbType = NpgsqlDbType.Text
                });
            }
            whereClause.Append($"@{paramName}");
            return;
        }

        var npgsqlType = GetNpgsqlType(type);
        parameters.Add(new NpgsqlParameter
        {
            ParameterName = paramName,
            Value = value,
            NpgsqlDbType = npgsqlType
        });
        whereClause.Append($"@{paramName}");
    }

    private static NpgsqlDbType GetNpgsqlType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        if (type.IsEnum)
            return NpgsqlDbType.Text;

        return Type.GetTypeCode(type) switch
        {
            TypeCode.Int16 => NpgsqlDbType.Smallint,
            TypeCode.Int32 => NpgsqlDbType.Integer,
            TypeCode.Int64 => NpgsqlDbType.Bigint,
            TypeCode.Decimal => NpgsqlDbType.Numeric,
            TypeCode.Double => NpgsqlDbType.Double,
            TypeCode.Single => NpgsqlDbType.Real,
            TypeCode.Boolean => NpgsqlDbType.Boolean,
            TypeCode.String => NpgsqlDbType.Text,
            TypeCode.DateTime => NpgsqlDbType.Timestamp,
            _ => NpgsqlDbType.Text
        };
    }
}