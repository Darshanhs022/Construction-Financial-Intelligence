-- 1A.Contract totals per project + cost code

create view vw_contract_totals as 
select 
    project_id,
    cost_code,
    count(contract_id) as contract_count,
    sum(original_contract_value) as total_original_contract,
    sum(approved_amendments) as total_amendments,
    sum(revised_contract_value) as total_revised_contract
from stg_vendor_contracts
group by project_id,cost_code;    

-- 1B. PO totals with remaining (uninvoiced) balance

create view vw_po_totals as
select
    p.project_id,
    p.cost_code,
    count(p.po_number) as po_count,
    sum(p.po_amount) as total_po,
    coalesce(sum(i.total_inv),0) as total_invoiced_against_po,
    sum(p.po_amount)-coalesce(sum(i.total_inv),0) as remaining_po_balance
from stg_purchase_orders p
left join(
    select po_number,sum(invoice_amount)as total_inv
    from stg_vendor_invoices
    group by po_number
) i on p.po_number=i.po_number
group by p.project_id,p.cost_code;  

-- 1C. Invoice totals split by payment status

create view vw_invoice_totals as
select
    project_id,
    cost_code,
    count(invoice_number) as invoice_count,
    sum(invoice_amount)as total_invoiced,
    sum(case when payment_status='Paid'
             then invoice_amount else 0 end) as total_paid,
    sum(case when payment_status ='Unpaid'
             then invoice_amount else 0 end)as total_unpaid
    from stg_vendor_invoices
    group by project_Id,cost_code;                  

-- 1D. Accrual totals per project + cost code

create view vw_accrual_totals as
select
    project_id,
    cost_code,
    count(accrual_id) as accrual_count,
    sum(accrued_amount) as total_accrued
from stg_accruals
group by project_id,cost_code;   

-- =============================================================================
-- UNIFIED COST VIEW
-- Single source of truth — budget vs contracts vs POs vs invoices vs accruals.
-- Every downstream view is built on top of this.
-- =============================================================================

create view vw_unified_cost as 
select
    b.project_id,
	b.cost_code,
	b.cost_description,
	b.cost_category,
	b.original_budget,
	b.approved_variations,
	b.revised_budget,
	coalesce(c.total_revised_contract,0) as total_contract,
	coalesce(c.total_revised_contract,0) -b.revised_budget as contract_vs_budget,
	round(
        coalesce(c.total_revised_contract,0)*100
		/nullif(b.revised_budget,0),2
	) as contract_utilisation_pct,
	coalesce(p.total_po,0) as total_po,
	coalesce(p.remaining_po_balance,0) as remaining_po_balance,
	coalesce(i.total_invoiced,0) as total_invoiced,
	coalesce(i.total_paid,0) as total_paid,
	coalesce(i.total_unpaid,0)as total_unpaid,                                 
    coalesce(a.total_accrued, 0)as total_accrued,
	coalesce(i.total_invoiced,0)+coalesce(a.total_accrued,0)+coalesce(p.remaining_po_balance,0)as forecast_cost
from stg_budget_cost_codes b
left join vw_contract_totals  c  on b.project_id = c.project_id
and b.cost_code   = c.cost_code
left join vw_po_totals p  on b.project_id = p.project_id
and b.cost_code   = p.cost_code
left join vw_invoice_totals  i  on b.project_id = i.project_id
and b.cost_code   = i.cost_code
left join vw_accrual_totals  a on b.project_id = a.project_id
and b.cost_code   = a.cost_code;

-- =============================================================================
-- FORECAST SUMMARY
-- Aggregates unified_cost to project level.
-- Calculates variance vs budget and classifies risk.
-- =============================================================================

create view vw_forecast_summary as
select
    u.project_id,
    p.project_name,
    p.location,
    p.project_type,
    p.status,
    round(sum(u.revised_budget) / 10000000.0, 2)  as revised_budget_cr,
    round(sum(u.total_contract) / 10000000.0, 2)  as total_contract_cr,
    round(sum(u.total_po) / 10000000.0, 2)  as total_po_cr,
    round(sum(u.total_invoiced) / 10000000.0, 2)  as total_invoiced_cr,
    round(sum(u.total_accrued)/ 10000000.0, 2)  as total_accrued_cr,
    round(sum(u.forecast_cost)/ 10000000.0, 2)  as forecast_cost_cr,
    round(
        (sum(u.forecast_cost) - sum(u.revised_budget)) / 10000000.0,2)as variance_cr,
    round(
        (sum(u.forecast_cost) - sum(u.revised_budget)) * 100.0
        / nullif(sum(u.revised_budget), 0),2)as variance_pct,
    case
        when (sum(u.forecast_cost) - sum(u.revised_budget)) * 100.0
             / nullif(sum(u.revised_budget), 0) <= 5  then 'Low'
        when (sum(u.forecast_cost) - sum(u.revised_budget)) * 100.0
             / nullif(sum(u.revised_budget), 0) <= 10 then 'Medium'
        else 'High'
    end as cost_risk
from vw_unified_cost u
join stg_project_master p on u.project_id = p.project_id
group by u.project_id, p.project_name, p.location, p.project_type, p.status;

-- =============================================================================
-- CASHFLOW PROJECTION 
-- What cash has been spent, what is still owed, what is committed.
-- =============================================================================

create view vw_cashflow_projection as
select
    pm.project_id,
    pm.project_name,
    pm.location,
    pm.status,
    round(coalesce(paid.cash_spent, 0) / 10000000.0, 2) as cash_spent_cr,
    round(coalesce(unpaid.unpaid_total, 0) / 10000000.0, 2) as unpaid_invoices_cr,
    round(coalesce(overdue.overdue_total, 0) / 10000000.0, 2) as overdue_unpaid_cr, 
    round(coalesce(po_bal.remaining_po, 0) / 10000000.0, 2) as open_po_balance_cr,
    round(coalesce(acc.open_accruals, 0) / 10000000.0, 2) as open_accruals_cr,
    round(
        ( coalesce(unpaid.unpaid_total, 0) + coalesce(po_bal.remaining_po, 0)+ coalesce(acc.open_accruals, 0)
        ) / 10000000.0,2) as projected_cash_need_cr,
    case
        when (coalesce(unpaid.unpaid_total, 0)
              +coalesce(po_bal.remaining_po, 0)
              +coalesce(acc.open_accruals, 0)
             ) > 500000000 then 'High'    -- > ₹50 Cr
        when (coalesce(unpaid.unpaid_total, 0)
              +coalesce(po_bal.remaining_po, 0)
              +coalesce(acc.open_accruals, 0)
             ) > 200000000 then 'Medium'  -- > ₹20 Cr
        else 'Low'
    end as cash_risk
from stg_project_master pm
left join (
    select project_id, sum(invoice_amount)as cash_spent
    from stg_vendor_invoices
    where payment_status = 'Paid'
    group by project_id
) paid on pm.project_id = paid.project_id
left join (
    select project_id, sum(invoice_amount)as unpaid_total
    from stg_vendor_invoices
    where payment_status = 'Unpaid'
    group by project_id
) unpaid on pm.project_id = unpaid.project_id
left join (
    select project_id, sum(invoice_amount)as overdue_total
    from stg_vendor_invoices
    where payment_status = 'Unpaid'
      AND due_date < current_date
    group by project_id
) overdue on pm.project_id = overdue.project_id
left join (
    select project_id,sum(remaining_po_balance)as remaining_po
    from vw_po_totals
    group by project_id
) po_bal on pm.project_id = po_bal.project_id
left join (
    select project_id,sum(accrued_amount)as open_accruals
    from stg_accruals
    where reversal_month > current_date
    group by project_id
) acc on pm.project_id = acc.project_id;


-- =============================================================================
-- INVOICE AGING
-- Every unpaid invoice bucketed by how many days overdue it is.
-- Used for the Aging waterfall chart in Power BI.
-- =============================================================================

create view vw_invoice_aging as
select
    i.project_id,
    pm.project_name,
    i.invoice_number,
    i.vendor_id,
    i.invoice_amount,
    round(i.invoice_amount / 10000000.0,4) as invoice_amount_cr,
    i.invoice_date,
    i.due_date,
    current_date - i.due_date::date as days_overdue,
    case
        when current_date <= i.due_date::date then 'Not Yet Due'
        when (current_date - i.due_date::date) <=30 then '0-30 Days'
        when (current_date - i.due_date::date) <= 60 then '31-60 Days'
        when (current_date - i.due_date::date) <=90 then '61-90 Days'
        else '90+ Days'
    end as aging_bucket
from stg_vendor_invoices i
join stg_project_master pm
on i.project_id = pm.project_id
where i.payment_status = 'Unpaid';


-- =============================================================================
-- PORTFOLIO RISK 
-- One row per project. Used for the executive risk matrix in Power BI.
-- =============================================================================

create view vw_portfolio_risk as
select
    f.project_id,
    f.project_name,
    f.location,
    f.project_type,
    f.status,
    f.revised_budget_cr,
    f.forecast_cost_cr,
    f.variance_cr,
    f.variance_pct,
    f.cost_risk,
    c.cash_risk,
    c.projected_cash_need_cr,
    c.overdue_unpaid_cr,
    case
        when f.variance_pct >10 and c.cash_risk = 'High' then 'Critical'
        when f.variance_pct >10 or c.cash_risk = 'High' then 'High'
        when f.variance_pct >5 or c.cash_risk = 'Medium' then 'Medium'
        else 'Low'
    end as combined_risk,
    round(f.forecast_cost_cr * 1.10,2) as stressed_forecast_cr,
    round(f.forecast_cost_cr * 1.10 - f.revised_budget_cr,2) as stressed_variance_cr,
    rank() over (order by f.variance_pct desc nulls last) as risk_rank
from vw_forecast_summary f
join vw_cashflow_projection c
on f.project_id = c.project_id;

