BEGIN;
INSERT INTO accounts (code, name, type, is_cash)
VALUES
  ('Assets:Bank:Checking',            'Bank Checking Account',         'asset',     TRUE),
  ('Assets:Bank:Savings',             'Bank Savings Account',          'asset',     TRUE),
  ('Assets:Bank:MoneyMarket',         'Money Market Account',          'asset',     TRUE),
  ('Assets:Bank:CashManagement',      'Cash Management',               'asset',     TRUE),
  ('Assets:Bank:CertificateOfDeposit','Certificate of Deposit',        'asset',     FALSE),
  ('Assets:Bank:HSA',                 'Health Savings Account',        'asset',     FALSE),
  ('Liabilities:CreditCard',          'Credit Card',                   'liability', FALSE),
  ('Liabilities:LineOfCredit',        'Line of Credit',                'liability', FALSE),
  ('Liabilities:Mortgage',            'Mortgage',                      'liability', FALSE),
  ('Liabilities:StudentLoan',         'Student Loan',                  'liability', FALSE),
  ('Assets:Investment:Brokerage',     'Brokerage',                     'asset',     FALSE),
  ('Assets:Investment:IRA',           'IRA',                           'asset',     FALSE),
  ('Assets:Investment:401k',          '401k',                          'asset',     FALSE),
  ('Expenses:Dining:Restaurants',     'Restaurant Expenses',           'expense',   FALSE),
  ('Expenses:Miscellaneous',          'Miscellaneous Expenses',        'expense',   FALSE),
  ('Income:Salary',                   'Salary Income',                 'revenue',   FALSE),
  ('Income:Miscellaneous',            'Miscellaneous Income',          'revenue',   FALSE)
ON CONFLICT (code) DO NOTHING;
COMMIT;