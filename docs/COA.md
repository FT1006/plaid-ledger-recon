# Chart of Accounts (coa.yaml)

The transform step maps Plaid account types/subtypes into ledger accounts using `etl/coa.yaml`.

## Account Mappings Structure

Maps Plaid account `type/subtype` combinations to General Ledger account names:

```yaml
account_mappings:
  # Depository accounts (checking, savings, etc.)
  depository:
    checking: "Assets:Bank:Checking"
    savings: "Assets:Bank:Savings"
    money_market: "Assets:Bank:MoneyMarket"
    cd: "Assets:Bank:CertificateOfDeposit"
    
  # Credit accounts (credit cards, lines of credit)
  credit:
    credit_card: "Liabilities:CreditCard"
    line_of_credit: "Liabilities:LineOfCredit"
    
  # Investment accounts
  investment:
    brokerage: "Assets:Investment:Brokerage"
    ira: "Assets:Investment:IRA"
    "401k": "Assets:Investment:401k"
    
  # Loan accounts
  loan:
    mortgage: "Liabilities:Mortgage"
    student: "Liabilities:StudentLoan"
    auto: "Liabilities:AutoLoan"
    personal: "Liabilities:PersonalLoan"
```

## Category Mappings Structure

Maps Plaid transaction categories to expense/income accounts with hierarchical structure:

```yaml
category_mappings:
  # Food and dining
  "Food and Drink":
    Restaurants: "Expenses:Dining:Restaurants"
    Coffee: "Expenses:Dining:Coffee"
    Groceries: "Expenses:Groceries"
    
  # Transportation
  Transportation:
    Gas: "Expenses:Transportation:Gas" 
    Parking: "Expenses:Transportation:Parking"
    Public: "Expenses:Transportation:Public"
    
  # Shopping
  Shops:
    Clothing: "Expenses:Shopping:Clothing"
    Electronics: "Expenses:Shopping:Electronics"
    General: "Expenses:Shopping:General"
    
  # Income categories
  Deposit:
    Salary: "Income:Salary"
    Interest: "Income:Interest"
    Refund: "Income:Refund"
    
  # Default fallback
  default: "Expenses:Miscellaneous"
```

## Transform Logic

* **Normalization**: Account types/subtypes are normalized (lowercase, spaces→underscores) before lookup
* **Credit card handling**: 
  - Positive amounts = Purchases (expense account + liability credit)
  - Negative amounts = Payments/refunds (liability debit + checking/income credit)
  - Payment detection: Categories containing "transfer" or "payment" → Assets:Bank:Checking
  - Refunds: Other negative amounts → Income:Refund
* **Income detection**: Uses "Deposit" main category with subcategory mapping, or defaults to Income:Miscellaneous
* **Transform versioning**: Each entry records `transform_version` (currently = 1) for reproducibility
* **Source integrity**: All entries include deterministic `source_hash` (SHA256 of canonical JSON)
* **Fallback**: Unknown categories default to "Expenses:Miscellaneous"
* **Missing mappings**: Raise `Unmapped Plaid account type/subtype` errors

## Usage

To support a new Plaid type/subtype, add entries to the appropriate section in `etl/coa.yaml`. The transform code handles normalization automatically.