using System.Linq.Expressions;
using System.Text;
using Npgsql;

namespace Wololo.PgKeyValueDB;

public partial class PgKeyValueDB
{
    private class SqlExpressionVisitor : ExpressionVisitor
    {
        private readonly List<NpgsqlParameter> parameters = new();
        private readonly StringBuilder whereClause = new();
        private int parameterIndex;

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
            whereClause.Append("cast(value ->> '");
            whereClause.Append(node.Member.Name);  // Remove ToLower()
            whereClause.Append("' as text)");
            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            parameterIndex++;
            var parameter = new NpgsqlParameter
            {
                ParameterName = $"p{parameterIndex}",
                Value = node.Value ?? DBNull.Value
            };
            parameters.Add(parameter);
            whereClause.Append($"@{parameter.ParameterName}");
            return node;
        }
    }
}
