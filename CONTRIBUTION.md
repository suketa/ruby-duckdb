# Contribution Guide

## Environment setup

### With docker

1. Fork the repository and `git clone` to your local machine.
2. Build and access to docker container

```
docker compose build ubuntu
docker compose run --rm ubuntu bash
```

In case you want custom ruby or duckdb versions, use `--build-arg` options
```
docker compose build ubuntu --build-arg RUBY_VERSION=3.1.3 --build-arg DUCKDB_VERSION=0.6.0
```

### Without docker

1. Install [Ruby](https://www.ruby-lang.org/) into your local machine.
2. Install [duckdb](https://duckdb.org/) into your local machine.
3. Fork the repository and `git clone` to your local machine.
4. Run `bundle install`
5. Run `rake build`
   or you might run with C duckdb header and library directories:
   `rake build -- --with-duckdb-include=/duckdb_header_directory --with-duckdb-lib=/duckdb_library_directory`


## Issue

If you spot a problem, [search if an issue already exists](https://github.com/suketa/ruby-duckdb/issues).
If a related issue doesn't exist, you can open a [new issue](https://github.com/suketa/ruby-duckdb/issues/new).


## Fix Issues or Add New Features.

1. Run `rake test`
2. Create new branch to change the code.
3. Change the code.
4. Write test.
5. Run `rake test` and confirm all tests pass.
6. Git push.
7. Create PR.
