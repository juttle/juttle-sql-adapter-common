//override filter compilation with these functions.

var BINARY_OPS_TO_SQL_OPS = {
    '==':  '=',
    '!=':  '<>',
    '=~':  'LIKE',
    '!~':  'NOT LIKE'
};

module.exports = {
    initialize: function(options) {
        this.baseQuery = options.baseQuery;
    },
    compile: function(node) {
        return this.visit(node, this.baseQuery);
    },
    visitExpressionFilterTerm: function(node, sql_query) {
        return this.visit(node.expression, sql_query);
    },
    visitBinaryExpression: function(node, sql_query) {
        var self = this;
        var SQL_OP = BINARY_OPS_TO_SQL_OPS[node.operator] || node.operator;

        if (SQL_OP === 'OR' || SQL_OP === 'AND') {
            var whereSpecfic = SQL_OP === 'OR' ? 'orWhere' : 'where';

            return sql_query.where(function() {
                return self.visit(node.left, this)[whereSpecfic](function() {
                    return self.visit(node.right, this);
                });
            });
        }

        if (/LIKE/.test(SQL_OP)) {
            node.right.value = node.right.value.replace(/\*/g, '%').replace(/\?/g, '_');
        }

        return sql_query.where(this.visit(node.left), SQL_OP, this.visit(node.right));
    },
    visitUnaryExpression: function(node, sql_query) {
        var self = this;

        switch (node.operator) {
            case 'NOT':
                return sql_query.whereNot(function() {
                    return self.visit(node.expression, this);
                });
            case '*':
                return this.visit(node.expression);
            default:
                throw new Error('Invalid operator: ' + node.operator + '.');
        }
    },
    visitStringLiteral: function(node) {
        return String(node.value);
    },
    visitArrayLiteral: function(node) {
        var self = this;
        return node.elements.map(function(e) {
            return self.visit(e);
        });
    },
    visitMomentLiteral: function(node) {
        return new Date(node.value);
    }
};
