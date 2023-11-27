Feature: Execution Data Analysis
  As a financial analyst
  I want to analyze execution data from a parquet file
  So that I can get the number of executions, unique venues, and execution dates

  Scenario: Counting executions and venues, and extracting dates from a mock parquet file
    Given I have a mock parquet file with the following execution data
      | ISIN         | Currency | Venue | TradeTime                    | Price | Trade_id | Phase           | Quantity |
      | DE0006305006 | EUR      | XETA  | 2022-09-02 07:00:09.160      | 3.606 | 0        | OPENING_AUCTION | -150     |
      | DE0006305006 | EUR      | XETA  | 2022-09-02 07:02:32.790      | 3.624 | 1        | OPENING_AUCTION | -198     |
      | DE0006305006 | EUR      | XETS  | 2022-09-02 07:02:32.790      | 3.624 | 1        | OPENING_AUCTION | -198     |
    When I analyze the mock parquet file
    Then I should get the correct count of 3 executions, 2 unique venue, and the formatted execution date 2022-09-02
