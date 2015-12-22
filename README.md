# Juttle SQL Adapter

[![Build Status](https://travis-ci.org/juttle/juttle-sql-adapter-common.svg)](https://travis-ci.org/juttle/juttle-sql-adapter-common)

Common code shared among
[PostgreSQL](https://github.com/juttle/juttle-postgres-adapter/),
[MySQL](https://github.com/juttle/juttle-mysql-adapter/)
and [SQLite](https://github.com/juttle/juttle-sqlite-adapter/) adapters.

## Contributing

Contributions are welcome! Please file an issue or open a pull request.

To check code style and run unit tests:
```
npm test
```

Both are run automatically by Travis.

When developing you may run into failures during linting where jscs complains
about your coding style and an easy way to fix those files is to simply run
`jscs --fix test` or `jscs --fix lib` from the root directory of the project.
After jscs fixes things you should proceed to check that those changes are
reasonable as auto-fixing may not produce the nicest of looking code.
