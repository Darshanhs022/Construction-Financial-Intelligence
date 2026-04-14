import os
from datetime import date

BASE_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR=os.path.join(BASE_DIR,'raw_data')

FILES = {
    "project_master":    "project_master.csv",
    "budget_cost_codes": "budget_cost_codes.csv",
    "vendor_contracts":  "vendor_contracts.csv",
    "purchase_orders":   "purchase_orders.csv",
    "vendor_invoices":   "vendor_invoices.csv",
    "accruals":          "accruals.csv",
}
DB_CONFIG={
    "host":"localhost",
    "port":5432,
    "dbname":"construction_cost_analytics",    
    "user":"postgres",
    "password":"PASSWORD"
}

THRESHOLDS = {
    "budget_utilization_warning": 0.95,   
    "variation_spike_pct":        0.20,   
    "material_spike_pct":         0.10,   
    "overdue_cutoff_date":        date.today().isoformat(),
}

CURRENCY     = "INR"
CR_DIVISOR   = 10000000   
RUN_DATE      = date.today().isoformat()