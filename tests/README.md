# `tests` folder
Here, I write random tests for functions I use (either in my `helpers` package, or any of the `cli_scripts`).

I deliberately chose to not test _everything_ and instead test only stuff that was non-trivial for me, or where I encountered bugs when executing the code. The codes should be run with `pytest`.

The test folder follows the same structure as the files that are being tested. Each test file is prefixed with `test_` which allows `pytest` to automatically detect them.

## Run tests
To run the tests, you need to install `pytest` first. Then, you can run the tests from the root of this project directory with the following command:
```bash
pytest tests/
```