'use strict';

/* global JuttleAdapterAPI */
var FilterJSCompiler = JuttleAdapterAPI.compiler.FilterJSCompiler;

const BINARY_OPS_TO_SQL_OPS = {
    '==':  '=',
    '!=':  '<>',
    '=~':  'LIKE',
    '!~':  'NOT LIKE'
};

class FilterSQLCompiler extends FilterJSCompiler {

    constructor(options) {
        super(options);
        this.baseQuery = options.baseQuery;
    }

    compile(node) {
        return this.visit(node, this.baseQuery);
    }

    visitExpressionFilterTerm(node, sql_query) {
        return this.visit(node.expression, sql_query);
    }

    visitBinaryExpression(node, sql_query) {
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
    }

    visitUnaryExpression(node, sql_query) {
        var self = this;

        switch (node.operator) {
            case 'NOT':
                return sql_query.whereNot(function() {
                    return self.visit(node.argument, this);
                });
            default:
                throw new Error('Invalid operator: ' + node.operator + '.');
        }
    }

    visitField(node) {
        return node.name;
    }

    visitStringLiteral(node) {
        return String(node.value);
    }

    visitArrayLiteral(node) {
        var self = this;
        return node.elements.map(function(e) {
            return self.visit(e);
        });
    }

    visitMomentLiteral(node) {
        return new Date(node.value);
    }
}

module.exports = FilterSQLCompiler;
