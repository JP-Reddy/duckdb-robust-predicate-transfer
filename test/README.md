# Testing this extension
This directory contains all the tests for Robust extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most of its tests in this format as SQL statements, so for the Robust extension, this should probably be the goal too.

The current test files are:
- `test/sql/correctness.test`: checks that several representative join shapes return the expected results across Robust setting combinations.
- `test/sql/plan_positive.test`: checks that Robust filter operators are inserted for several eligible joins across Robust setting combinations.
- `test/sql/plan_negative.test`: checks that Robust filter operators are not inserted for serveral ineligible plans across Robust setting combinations.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```