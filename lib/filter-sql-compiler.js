'use strict';

var StaticFilterCompilerBase = require('./static-filter-compiler-base');

var BINARY_OPS_TO_SQL_OPS = {
    '==':  '=',
    '!=':  '<>',
    '=~':  'LIKE',
    '!~':  'NOT LIKE'
};

class FilterSQLCompiler extends StaticFilterCompilerBase {
    compileLiteral(node) {
        switch (node.type) {
            case 'NullLiteral':
                return null;

            case 'BooleanLiteral':
            case 'NumericLiteral':
                return node.value;

            case 'StringLiteral':
                return String(node.value);

            case 'ArrayLiteral':
                var self = this;

                return node.elements.map(function(e) {
                    return self.compile(e);
                });

            case 'MomentLiteral':
                return new Date(node.value);

            default:
                super.compileLiteral(node);
        }
    }

    compileField(node) {
        return node.name;
    }

    compileExpressionTerm(node, sql_query) {
        var SQL_OP = BINARY_OPS_TO_SQL_OPS[node.operator] || node.operator;

        if (/LIKE/.test(SQL_OP)) {
            node.right.value = node.right.value.replace(/\*/g, '%').replace(/\?/g, '_');
        }

        return sql_query.where(this.compile(node.left), SQL_OP, this.compile(node.right));
    }

    compileAndExpression(node, sql_query) {
        var self = this;

        return sql_query.where(function() {
            return self.compile(node.left, this);
        }).where(function() {
            return self.compile(node.right, this);
        });
    }

    compileOrExpression(node, sql_query) {
        var self = this;

        return sql_query.where(function() {
            return self.compile(node.left, this);
        }).orWhere(function() {
            return self.compile(node.right, this);
        });
    }

    compileNotExpression(node, sql_query) {
        var self = this;

        return sql_query.whereNot(function() {
            return self.compile(node.expression, this);
        });
    }
}

module.exports = FilterSQLCompiler;
