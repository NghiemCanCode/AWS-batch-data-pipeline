# Business Specification

## 1. Project Overview
Provides near real-time visibility into potentially suspicious transaction activities using rule-based monitoring.

## 2. Business Objectives
- Detect suspicious transaction activity within minutes
- Monitor transaction anomalies
- Detect merchant activity spikes
- Detect abnormal customer behavior
- Support fraud analyst investigations
- Provide operational visibility

## 3. Data Freshness Requirements
- Dashboard Refresh Frequency: Every 5 Minutes
- Maximum Data Latency: 5 Minutes
- Historical Reporting Refresh: Daily

## 4. Rule-Based Monitoring Framework

### Velocity Rules
VR001: More than 5 transactions from the same card within 5 minutes.

VR002: More than 10 transactions from the same customer within 10 minutes.

VR003: Transactions at more than 3 different merchants within 2 minutes.

### Amount Rules
AR001: Transaction Amount > 3 × Average Customer Transaction (last 30 days)

AR002: Transaction Amount > 95th Percentile of Customer History

AR003: Transaction Amount > 80% of Card Limit

### Geographic Rules
GR001: Same card used in multiple states within 30 minutes.

GR002: Transaction state differs from customer home state.

GR003: Transaction occurs in a state not present in customer history.

### Merchant Rules
MR001: Current 5-minute volume > 3 × historical average.

MR002: Current failure rate > 2 × historical failure rate.

MR003: New card ratio exceeds configured threshold.

### Behavioral Rules
BR001: Current spending > 3 × average daily spending.

BR002: MCC not present in customer history.

BR003: Transaction occurs outside normal customer usage patterns.

## 5. Fraud Operations Dashboard
- Transactions (Last 5 Minutes)
- Transaction Value (Last 5 Minutes)
- Alert Count
- Rule Violations
- High-Risk Cards
- High-Risk Customers
- High-Risk Merchants
- Transaction Failure Rate

## 6. Data Modeling Implications

### fact_fraud_operations_snapshot
Grain:
- 5-Minute Window
- Rule
- Merchant
- State

Measures:
- transaction_count
- transaction_amount
- unique_cards
- unique_customers
- rule_violation_count
- failed_transaction_count
- high_value_transaction_count

Refresh Frequency:
Every 5 Minutes