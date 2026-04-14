create table if not exists stg_project_master (
    project_id              text primary key,
    project_name            text,
    location                text,
    project_type            text,
    planned_start_date      date,
    planned_end_date        date,
    revised_end_date        date,
    currency                text,
    status                  text,
    total_original_budget   numeric(18, 2)
);

create table if not exists stg_budget_cost_codes (
    project_id              text,
    cost_code               text,
    cost_description        text,
    original_budget         numeric(18, 2),
    approved_variations     numeric(18, 2),
    revised_budget          numeric(18, 2),
    revision_date           date,
    revision_reason         text,
    cost_category           text,
    primary key (project_id, cost_code)
);

create table if not exists stg_vendor_contracts (
    contract_id             text primary key,
    project_id              text,
    cost_code               text,
    vendor_id               integer,
    vendor_name             text,
    original_contract_value numeric(18, 2),
    approved_amendments     numeric(18, 2),
    revised_contract_value  numeric(18, 2),
    contract_start_date     date,
    contract_end_date       date,
    payment_terms_days      integer,
    contract_status         text
);

create table if not exists stg_purchase_orders (
    po_number               text primary key,
    contract_id             text,
    project_id              text,
    cost_code               text,
    vendor_id               integer,
    po_amount               numeric(18, 2),
    po_date                 date,
    po_status               text,
    expected_delivery_date  date
);

create table if not exists stg_vendor_invoices (
    invoice_number          text,
    po_number               text,
    project_id              text,
    cost_code               text,
    vendor_id               integer,
    invoice_amount          numeric(18, 2),
    invoice_date            date,
    payment_status          text,
    payment_date            date,
    due_date                date
);

create table if not exists stg_accruals (
    accrual_id              text primary key,
    project_id              text,
    cost_code               text,
    vendor_id               integer,
    accrual_month           date,
    accrued_amount          numeric(18, 2),
    accrual_reason          text,
    reversal_month          date
);

create table if not exists exceptions_summary (
    id               serial primary key,
    rule             text,
    category         text,
    violation_count  integer,
    exposure_cr_inr  numeric(12, 2),
    status           text,
    run_date         date
);