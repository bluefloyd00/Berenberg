Feature: Execution Data Analysis with Slippage and Price Metrics Calculation
  As a financial analyst
  I want to calculate the slippage and price metrics for each trade
  So that I can analyze the execution performance and market conditions of trades
  Scenario: Analyzing execution data and calculating metrics from mock parquet files
    Given I have a mock parquet file with the following execution data merged with reference data
      | ISIN         | Currency | Venue | TradeTime                 | Price | Trade_id | Phase              | Quantity | side | listing_id | primary_ticker | primary_mic |
      | BE0003851681 | EUR      | XBRU  | 2022-09-02 07:43:05.795   | 91.90 | 101      | CONTINUOUS_TRADING | 16       | 1    | 328336     | AED            | XBRU        |
      
    Given I have a mock parquet file with the following market data
      | event_timestamp             | best_bid_price | best_ask_price | best_bid_size | best_ask_size | market_state       | primary_mic | listing_id |
      | 2022-09-02 07:43:00.780403102 | 91.90         | 91.95         | 16            | 95            | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.795558330 | 91.85         | 91.95         | 39            | 95            | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.795669096 | 91.85         | 91.90         | 39            | 80            | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.795716362 | 91.85         | 91.95         | 39            | 150           | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.798700164 | 91.85         | 91.90         | 4             | 150           | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.800432847 | 91.85         | 91.90         | 4             | 70            | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.800438003 | 91.85         | 91.95         | 4             | 126           | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.800489579 | 91.85         | 91.95         | 4             | 95            | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.800572772 | 91.85         | 91.90         | 4             | 80            | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.836259726 | 91.85         | 91.90         | 4             | 135           | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:05.863581863 | 91.80         | 91.90         | 87            | 135           | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:06.689282110 | 91.80         | 91.90         | 7             | 135           | CONTINUOUS_TRADING | XBRU        | 328336     |
      | 2022-09-02 07:43:08.697180112 | 91.80         | 91.90         | 7             | 80            | CONTINUOUS_TRADING | XBRU        | 328336     |

    When I calculate the trading metrics for each trade
    Then I should get the following results
      | ISIN         | Currency | Venue | TradeTime                     | Price | Trade_id | Phase              | Quantity | side | listing_id | primary_ticker | primary_mic | best_bid_price_min_1s | best_ask_price_min_1s | mid_price_min_1s | best_bid_price | best_ask_price | mid_price | best_bid_price_1s | best_ask_price_1s | mid_price_1s | slippage |            
      | BE0003851681 | EUR      | XBRU  | 2022-09-02 07:43:05.795999998 | 91.90 | 101      | CONTINUOUS_TRADING | 16       | 1    | 328336     | AED            | XBRU        | 91.90                 | 91.95                 | 91.925           | 91.85          | 91.95          | 91.9      | 91.80             | 91.90             | 91.85        | 0.5      |