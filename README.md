# features-factory-demo

## How to use it

### Step1
Clone this repo to Databricks Workspace of your liking.

### Step2
In Databricks run `_setup/init` notebook which will create raw example tables (customer, card_transactions, web_visits).

### Step3
Run `orchestrator` notebook. It will create `odap_features.customer` table with features defined in `features/` folder.

### Step4
Check the segment `segments/customers_who_might_be_interested_in_investments` by running this notebook.
