# Business Specification

## 1. Project Overview
This project aims to build an analytics platform for a retail banking environment. The platform supports historical reporting, customer behavior analysis, card lifecycle monitoring, and merchant performance analysis.

## 2. Business Objectives

### Customer Analytics
- Which customer segments generate the highest transaction volume?
- Which income groups contribute the most transaction value?
- How do spending patterns vary by geography?
- How do customer demographics affect transaction behavior?

### Card Portfolio Analytics
- What percentage of customers use chip-enabled cards?
- How frequently are cards reissued?
- How does card technology impact transaction success rates?
- How has the card portfolio evolved over time?

### Merchant Analytics
- Which merchants generate the highest transaction volume?
- Which merchant categories generate the highest transaction value?
- Which merchants experience the highest transaction failure rates?
- How do transaction patterns vary by merchant category?

## 3. Analytical Requirements
### Historical Analysis
- Customer attributes
- Card attributes
- Merchant attributes
- Transaction activity

### Point-in-Time Analysis
Support historical attribute tracking.

### Segmentation Analysis
- Income Bracket
- Gender
- Geography
- Retirement Age

### Geographic Analysis
- State
- City

### Merchant Category Analysis
- Merchant
- Merchant Category Code (MCC)

## 4. Dashboard Requirements

### Dashboard A - Customer Spending Overview
- Total Transaction Value
- Total Transactions
- Active Customers
- Average Transaction Amount

### Dashboard B - Card Portfolio Dashboard
- Number of Active Cards
- Chip Card Adoption Rate
- Card Reissue Count
- Transaction Success Rate by Card Type

### Dashboard C - Merchant Performance Dashboard
- Transaction Volume
- Transaction Value
- Average Transaction Amount
- Error Rate

## 5. Data Modeling Implications

### Customer Dimension (SCD Type 2)
- Income Bracket
- Address

### Card Dimension (SCD Type 2)
- Chip Availability
- CVV Availability
- Expiration Information
- Number of Card Reissues

### Merchant Dimension (SCD Type 2)
* Merchant's Information

### fact_account_transaction
Grain: One row per transaction.

### fact_account_monthly_snapshot
Grain: One row per card per month per currency.

### fact_daily_transaction_trend
Grain: One row per Date × Merchant Category (MCC).

Measures:
- transaction_count
- transaction_amount
- successful_transaction_count
- failed_transaction_count
- error_rate
- unique_cards
- unique_customers

## 6. Success Criteria
- Support customer segmentation analysis
- Support historical reporting across multiple years
- Support point-in-time analysis
- Support merchant performance reporting
- Support card lifecycle reporting
- Support daily transaction trend analysis
- Provide curated datasets suitable for future machine learning initiatives
"""