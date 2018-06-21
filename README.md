Predicting Credit Card Defaulters
=


![](https://blog.bankbazaar.com/wp-content/uploads/2016/03/Surviving-a-Credit-Card-Default.png)
##### Image from [here](https://blog.bankbazaar.com).

Overview
-

Banks and credit card clients often have a high risk in that they don't know if an indvidual will default on their credit card payment or not. The 10 features they have collected are:

* CUSTID: Unique Customer ID
* LIMIT_BAL: Maximum Spending Limit for the customer
* SEX: Sex of the customer. Some records have M and F to indicate sex. Some records have 1 ( Male) and 2 (Female)
* education: Education Level of the customer. The values are 1 (Graduate), 2 (University), 3 (High School) and 4 (Others)
* MARRIAGE: Marital Status of the customer. The values are 1 (Single), 2 ( Married) and 3 ( Others)
* AGE: Age of customer
* PAY_1 to PAY_6: Payment status for the last 6 months, one column for each month. This indicates the number of months (delay)                  the customer took to pay that month’s bill
* BILL_AMT1 to BILL_AMT6: The Billed amount for credit card for each of the last 6 months.
* PAY_AMT1 to PAY_AMT6: The actual amount the customer paid for each of the last 6 months
* DEFAULTED: Whether the customer defaulted or not on the 7th month. The values are 0 (did not default) and 1 (defaulted)

#### Questions to be answered:
1. **Loading and processing the data** : Do Data cleansing and making it to be enhanced to answer the required questions.
2. **Do the Analysis** : Perform the basic analysis in spark
3. **Use of spark machine learning lib** : build models using pyspark.ml
