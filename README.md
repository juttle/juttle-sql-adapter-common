# Juttle SQL Adapter

[![Build Status](https://travis-ci.org/juttle/juttle-sql-adapter-common.svg?branch=master)](https://travis-ci.org/juttle/juttle-sql-adapter-common)

Code from the `/lib` and `/test` directory shared among
[PostgreSQL](https://github.com/juttle/juttle-postgres-adapter/),
[MySQL](https://github.com/juttle/juttle-mysql-adapter/)
and [SQLite](https://github.com/juttle/juttle-sqlite-adapter/) adapters.

#### List of optimized operations

* any filter expression `read sql` (note: `read sql | filter ...` is not optimized)
* `head` or `tail`
* `sort` when used without a `groupby` (note the `time` key will be deleted from any point) 
* `reduce count()`, `sum()`, `min()`, `max()`, `sum()`, `avg()`, `count_unique()`
* `reduce by fieldname`
* `reduce -every :interval:`

In case of unexpected behavior with optimized reads, add `-optimize false` option to `read sql` to disable optimizations, and kindly report the problem as a GitHub issue.

## Contributing

Contributions are welcome! Please file an issue or open a pull request.

To check code style and run unit tests:
```
npm test && npm run lint
```

Both are run automatically by Travis.
