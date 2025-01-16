# message-agent
# message-agent

## Running Tests

To run the tests for the message-agent project, follow these steps:

1. **Install Dependencies**: Make sure you have Python and pip installed. Then, install the required packages by running:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run Tests**: You can run the tests using pytest. If you have pytest installed, simply execute:
   ```bash
   pytest
   ```
   This will discover and run all the tests in the `tests` directory.

3. **View Test Results**: After running the tests, you will see the results in the terminal. If any tests fail, pytest will provide detailed output to help you debug the issues.

4. **Running Specific Tests**: If you want to run a specific test file or test case, you can specify it like this:
   ```bash
   pytest tests/test_message_service.py
   ```
   or for a specific test function:
   ```bash
   pytest tests/test_message_service.py::test_send_message_success
   ```

5. **Check Coverage**: To check the test coverage, you can use the `pytest-cov` plugin. Install it via pip if you haven't already:
   ```bash
   pip install pytest-cov
   ```
   Then run:
   ```bash
   pytest --cov=src
   ```

Make sure to replace `src` with the appropriate directory if your source code is located elsewhere.
