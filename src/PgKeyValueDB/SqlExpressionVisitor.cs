using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace Wololo.PgKeyValueDB;

public partial class PgKeyValueDB
{
    private class SqlExpressionVisitor : ExpressionVisitor
    {
        private readonly List<NpgsqlParameter> parameters = new();
        private readonly StringBuilder whereClause = new();
        private int parameterIndex;
        private readonly Type documentType;

        public SqlExpressionVisitor(Type documentType)
        {
            this.documentType = documentType;
        }

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

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression?.NodeType != ExpressionType.Parameter)
                throw new NotSupportedException("Only direct property access is supported");
            
            var property = node.Member as PropertyInfo;
            if (property == null)
                throw new NotSupportedException("Only properties are supported");
            
            if (property.DeclaringType != documentType)
                throw new NotSupportedException("Only properties from the document type are supported");
                
            var path = BuildJsonPath(property);
            whereClause.Append(path);
            return node;
        }

        private string BuildJsonPath(PropertyInfo property)
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
                // Let Npgsql handle the type mapping
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
}
