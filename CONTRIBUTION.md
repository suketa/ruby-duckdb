# Contribution Guide

## Issue

If you spot a problem, [search if an issue already exists](https://github.com/suketa/ruby-duckdb/issues).
If a related issue doesn't exist, you can open a [new issue](https://github.com/suketa/ruby-duckdb/issues/new).


## Fix Issues or Add New Features.

1. install [Ruby](https://www.ruby-lang.org/) into your local machine.
2. install [duckdb](https://duckdb.org/) into your local machine.
3. fork the repository and `git clone` to your local machine.
4. run `bundle install`
5. run `rake build`
   or you might run with C duckdb header and library directories:
   `rake build -- --with-duckdb-include=/duckdb_header_directory --with-duckdb-lib=/duckdb_library_directory`
6. run `rake test`
7. create new branch to change the code.
8. change the code.
9. write test.
10. run `rake test` and confirm all tests pass.
11. git push.
12. create PR.
