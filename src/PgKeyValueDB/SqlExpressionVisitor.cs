using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace Wololo.PgKeyValueDB;

public class SqlExpressionVisitor(Type documentType, JsonSerializerOptions jsonSerializerOptions) : ExpressionVisitor
{
    private readonly List<NpgsqlParameter> parameters = [];
    private readonly StringBuilder whereClause = new();
    private int parameterIndex;
    private readonly Type documentType = documentType;
    private readonly JsonSerializerOptions jsonSerializerOptions = jsonSerializerOptions;
    private readonly JsonNamingPolicy propertyNamingPolicy = jsonSerializerOptions.PropertyNamingPolicy ?? JsonNamingPolicy.CamelCase;
    
    // Context for handling array element predicates in Any()
    private ParameterExpression? arrayElementParameter;
    private string? arrayElementAlias;

    public string WhereClause => whereClause.ToString();
    public NpgsqlParameter[] Parameters => [.. parameters];

    protected override Expression VisitBinary(BinaryExpression node)
    {
        whereClause.Append('(');

        // Special handling for null comparisons: convert == null to IS NULL and != null to IS NOT NULL
        if ((node.NodeType == ExpressionType.Equal || node.NodeType == ExpressionType.NotEqual) &&
            (IsNullConstant(node.Left) || IsNullConstant(node.Right)))
        {
            var nonNullSide = IsNullConstant(node.Left) ? node.Right : node.Left;
            Visit(nonNullSide);
            whereClause.Append(node.NodeType == ExpressionType.Equal ? " is null" : " is not null");
            whereClause.Append(')');
            return node;
        }

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

    private static bool IsNullConstant(Expression expr)
    {
        return expr is ConstantExpression ce && ce.Value == null;
    }

    private static bool IsNumericType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type == typeof(int) || type == typeof(long) || type == typeof(decimal) ||
               type == typeof(double) || type == typeof(float) || type == typeof(short);
    }

    private static bool IsSimpleType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(decimal) || type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(Guid);
    }

    private static string GetOperator(ExpressionType nodeType) => nodeType switch
    {
        ExpressionType.Equal => " = ",
        ExpressionType.NotEqual => " != ",
        ExpressionType.GreaterThan => " > ",
        ExpressionType.GreaterThanOrEqual => " >= ",
        ExpressionType.LessThan => " < ",
        ExpressionType.LessThanOrEqual => " <= ",
        ExpressionType.AndAlso => " and ",
        ExpressionType.OrElse => " or ",
        _ => throw new NotSupportedException($"Operation {nodeType} is not supported")
    };

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Handle IsNullOrWhiteSpace extension method calls
        if (node.Method.Name == nameof(string.IsNullOrWhiteSpace) &&
            node.Method.IsStatic &&
            node.Arguments.Count == 1 &&
            node.Arguments[0].Type == typeof(string))
        {
            var argument = node.Arguments[0];

            // Check if it's a constant/closure variable (not a property access on the entity)
            if (argument.NodeType == ExpressionType.Constant ||
                (argument.NodeType == ExpressionType.MemberAccess &&
                 ((MemberExpression)argument).Expression?.NodeType == ExpressionType.Constant))
            {
                // For constants/variables, evaluate the IsNullOrWhiteSpace call and use the result
                var value = Expression.Lambda(argument).Compile().DynamicInvoke();
                var isNullOrWhiteSpace = string.IsNullOrWhiteSpace(value?.ToString());
                AddParameter(isNullOrWhiteSpace, typeof(bool));
                return node;
            }
            else
            {
                // For property access on the entity, generate the JSON path check
                // This handles cases like string.IsNullOrWhiteSpace(u.SomeProperty)
                whereClause.Append("(");
                Visit(argument);
                whereClause.Append(" is null or trim(");
                Visit(argument);
                whereClause.Append(") = '')");
                return node;
            }
        }

        // Special handling for enum ToString()
        if (node.Method.Name == nameof(ToString) && node.Object?.Type.IsEnum == true)
        {
            whereClause.Append($"case (");
            Visit(node.Object);
            whereClause.Append(")");

            // Add WHEN clauses for each enum value
            var enumType = node.Object.Type;
            var enumValues = Enum.GetValues(enumType);
            foreach (var enumValue in enumValues)
            {
                whereClause.Append($" when {Convert.ToInt32(enumValue)} then '{enumValue}'");
            }
            whereClause.Append(" end");

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
                    whereClause.Append(isCaseInsensitive ? " ilike " : " like ");

                    // Get the search value and create pattern parameter
                    var searchValue = Expression.Lambda(node.Arguments[0]).Compile().DynamicInvoke()?.ToString();
                    parameterIndex++;
                    var paramName = $"p{parameterIndex}";
                    parameters.Add(new NpgsqlParameter
                    {
                        ParameterName = paramName,
                        Value = $"%{searchValue}%",
                        NpgsqlDbType = NpgsqlDbType.Text
                    });
                    whereClause.Append($"@{paramName}");

                    return node;

                case nameof(string.StartsWith):
                    Visit(node.Object);
                    whereClause.Append(" like ");
                    Visit(node.Arguments[0]);
                    whereClause.Append(" || '%'");
                    return node;

                case nameof(string.EndsWith):
                    Visit(node.Object);
                    whereClause.Append(" like '%' || ");
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
                    whereClause.Append("lower(");
                    Visit(node.Object);
                    whereClause.Append(")");
                    return node;

                case nameof(string.ToUpper):
                    whereClause.Append("upper(");
                    Visit(node.Object);
                    whereClause.Append(")");
                    return node;

                case nameof(string.IsNullOrWhiteSpace):
                    whereClause.Append("(");
                    Visit(node.Arguments[0]);
                    whereClause.Append(" is null or trim(");
                    Visit(node.Arguments[0]);
                    whereClause.Append(") = '')");
                    return node;
            }
        }

        // Handle Contains for collections
        if (node.Method.Name == nameof(Enumerable.Contains) && node.Arguments.Count == 1)
        {
            var collection = node.Object;
            var item = node.Arguments[0];

            if (collection is MemberExpression memberExpression)
            {
                var parentPath = BuildNestedJsonPath(memberExpression);
                whereClause.Append($"jsonb_exists({parentPath}, ");
                Visit(item);
                whereClause.Append(")");
                return node;
            }
        }

        // Handle Count() for collections
        if (node.Method.Name == nameof(Enumerable.Count))
        {
            var collection = node.Arguments[0];
            return HandleCollectionAggregateQuery(node, collection, null, "count(*)", "jsonb_array_length({0})", "0");
        }

        // Handle Any() for collections
        if (node.Method.Name == nameof(Enumerable.Any))
        {
            // Any() without predicate
            if (node.Arguments.Count == 1)
            {
                var collection = node.Arguments[0];
                return HandleCollectionAggregateQuery(node, collection, null, "1", "jsonb_array_length({0}) > 0", "false", useExists: true);
            }
            // Any() with predicate: collection.Any(x => <predicate>)
            else if (node.Arguments.Count == 2 && node.Arguments[1] is LambdaExpression lambda)
            {
                var collection = node.Arguments[0];
                return HandleCollectionAggregateQuery(node, collection, lambda, "1", null, null, useExists: true);
            }
            
            throw new NotSupportedException($"Any() with {node.Arguments.Count} arguments is not supported");
        }

        throw new NotSupportedException($"Method {node.Method.Name} is not supported");
    }

    private string BuildJsonPath(MemberInfo member, string parentPath = "value")
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
        var name = propertyNamingPolicy.ConvertName(member.Name);
        
        // Check if parentPath is an array element alias (starts with __elem)
        if (parentPath.StartsWith("__elem"))
        {
            // For array elements from jsonb_array_elements, use ->> operator directly on the alias
            return $"({parentPath}->>'{name}'){cast}";
        }
        else if (parentPath != "value")
        {
            return $"({parentPath}->>'{name}'){cast}";
        }
        return $"(value ->> '{name}'){cast}";
    }

    private static Type GetMemberType(MemberInfo member) => member switch
    {
        PropertyInfo prop => prop.PropertyType,
        FieldInfo field => field.FieldType,
        _ => throw new NotSupportedException($"Member type {member.MemberType} is not supported")
    };

    protected override Expression VisitMember(MemberExpression node)
    {
        // Handle HasValue property on nullable types
        if (node.Member.Name == "HasValue" && node.Expression is MemberExpression nullableMember)
        {
            // Check if this is accessing a closure field
            if (nullableMember.Expression?.NodeType == ExpressionType.Constant)
            {
                var value = Expression.Lambda(node).Compile().DynamicInvoke();
                AddParameter(value, node.Type);
                return node;
            }

            // For nullable properties, HasValue means the JSON field is not null
            if (nullableMember.Expression is MemberExpression nestedMember)
            {
                var parentPath = BuildNestedJsonPath(nestedMember);
                var jsonPath = BuildJsonPath(nullableMember.Member, parentPath);
                // Remove the cast since we're checking for null
                var pathWithoutCast = jsonPath.Substring(1, jsonPath.LastIndexOf(')') - 1);
                whereClause.Append($"({pathWithoutCast}) is not null");
            }
            else if (nullableMember.Expression?.NodeType == ExpressionType.Parameter)
            {
                var jsonPath = BuildJsonPath(nullableMember.Member);
                // Remove the cast since we're checking for null
                var pathWithoutCast = jsonPath.Substring(1, jsonPath.LastIndexOf(')') - 1);
                whereClause.Append($"({pathWithoutCast}) is not null");
            }
            return node;
        }

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
            // Check if this is accessing a property on an array element parameter
            if (arrayElementParameter != null && node.Expression == arrayElementParameter)
            {
                whereClause.Append(BuildJsonPath(node.Member, arrayElementAlias!));
                return node;
            }
            
            whereClause.Append(BuildJsonPath(node.Member));
            return node;
        }

        // Return convert expressions as is
        if (node.Expression?.NodeType == ExpressionType.Convert)
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
            else if (current.Expression?.NodeType == ExpressionType.Convert)
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
            var name = propertyNamingPolicy.ConvertName(segment);
            jsonPath = $"({jsonPath}->'{name}')";
        }
        return jsonPath;
    }

    /// <summary>
    /// Handles collection aggregate queries like Count() and Any(), including Where().Count()/Any() patterns.
    /// </summary>
    /// <param name="node">The method call expression node</param>
    /// <param name="collection">The collection expression to query</param>
    /// <param name="predicate">Optional predicate lambda (for Any(x => ...) pattern)</param>
    /// <param name="selectClause">What to select in the subquery (e.g., "count(*)" or "1")</param>
    /// <param name="directCollectionSql">SQL template for direct collection access without predicate (e.g., "jsonb_array_length({0})")</param>
    /// <param name="directCollectionElse">The else clause value when array is not actually an array</param>
    /// <param name="useExists">Whether to wrap the subquery in exists()</param>
    private Expression HandleCollectionAggregateQuery(
        MethodCallExpression node, 
        Expression collection, 
        LambdaExpression? predicate,
        string selectClause,
        string? directCollectionSql,
        string? directCollectionElse,
        bool useExists = false)
    {
        // Handle Where().Count()/Any() pattern
        if (collection is MethodCallExpression whereCall && 
            whereCall.Method.Name == nameof(Enumerable.Where) &&
            whereCall.Arguments.Count == 2 &&
            whereCall.Arguments[1] is LambdaExpression whereLambda)
        {
            var actualCollection = whereCall.Arguments[0];
            
            if (actualCollection is MemberExpression memberExpression)
            {
                GenerateArraySubquery(memberExpression, whereLambda, selectClause, useExists);
                return node;
            }
            
            throw new NotSupportedException($"Where().{node.Method.Name}() is only supported on member expressions. Expression type: {actualCollection.NodeType}");
        }
        
        // Handle direct collection query with predicate (e.g., Any(x => x.Value > 5))
        if (predicate != null && collection is MemberExpression predicateMemberExpr)
        {
            GenerateArraySubquery(predicateMemberExpr, predicate, selectClause, useExists);
            return node;
        }
        
        // Handle direct collection query without predicate (e.g., Count() or Any())
        if (predicate == null && collection is MemberExpression directMemberExpression)
        {
            if (directCollectionSql == null)
                throw new InvalidOperationException("directCollectionSql must be provided for direct collection queries without predicate");
                
            var parentPath = BuildNestedJsonPath(directMemberExpression);
            var sql = string.Format(directCollectionSql, parentPath);
            whereClause.Append($"(case when jsonb_typeof({parentPath}) = 'array' then {sql} else {directCollectionElse} end)");
            return node;
        }
        
        throw new NotSupportedException($"{node.Method.Name}() is only supported on member expressions or Where() results. Expression type: {collection.NodeType}");
    }

    /// <summary>
    /// Generates a subquery using jsonb_array_elements to filter and aggregate array elements.
    /// </summary>
    private void GenerateArraySubquery(MemberExpression memberExpression, LambdaExpression lambda, string selectClause, bool useExists)
    {
        var parentPath = BuildNestedJsonPath(memberExpression);
        
        // Save and set up array element context
        var previousArrayParam = arrayElementParameter;
        var previousArrayAlias = arrayElementAlias;
        
        arrayElementParameter = lambda.Parameters[0];
        parameterIndex++;
        arrayElementAlias = $"__elem{parameterIndex}";
        
        // Determine if we're dealing with simple types or complex objects
        var elementType = lambda.Parameters[0].Type;
        var isSimple = IsSimpleType(elementType);
        var arrayElementsFunc = isSimple ? "jsonb_array_elements_text" : "jsonb_array_elements";
        
        // Generate SQL: [exists](select <selectClause> from jsonb_array_elements(...) where predicate)
        // Note: for Count(), we wrap in parentheses; for Any()/exists(), the exists() itself provides grouping
        if (useExists)
        {
            whereClause.Append($"exists(select {selectClause} from {arrayElementsFunc}({parentPath}) as {arrayElementAlias} where ");
        }
        else
        {
            whereClause.Append($"(select {selectClause} from {arrayElementsFunc}({parentPath}) as {arrayElementAlias} where ");
        }
        Visit(lambda.Body);
        whereClause.Append(")");
        
        // Restore previous context
        arrayElementParameter = previousArrayParam;
        arrayElementAlias = previousArrayAlias;
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

    protected override Expression VisitUnary(UnaryExpression node)
    {
        if (node.NodeType == ExpressionType.Not)
        {
            whereClause.Append("not (");
            Visit(node.Operand);
            whereClause.Append(")");
            return node;
        }

        return base.VisitUnary(node);
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        // Handle array element parameter references in Any() predicates
        if (arrayElementParameter != null && node == arrayElementParameter)
        {
            whereClause.Append(arrayElementAlias);
            return node;
        }

        return base.VisitParameter(node);
    }

    protected override Expression VisitConditional(ConditionalExpression node)
    {
        // Translate conditional expressions (ternary operator ?: ) to PostgreSQL CASE WHEN syntax
        // Example: condition ? ifTrue : ifFalse becomes (case when condition then ifTrue else ifFalse end)
        whereClause.Append("(case when ");
        Visit(node.Test);
        whereClause.Append(" then ");
        Visit(node.IfTrue);
        whereClause.Append(" else ");
        Visit(node.IfFalse);
        whereClause.Append(" end)");
        return node;
    }
}