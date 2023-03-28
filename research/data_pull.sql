--npi writing distribution
with cte1 AS (
        select RX_NPI, NDC_DRUG_TYPE, count(*) as rx_count
        from TBLREF_RXMX_EXT_REFERRALS
        group by RX_NPI, NDC_DRUG_TYPE
    ),
     cte2 as (
       select * from cte1
       PIVOT(SUM(RX_COUNT) FOR NDC_DRUG_TYPE IN ('NSAIDs', 'Other Pain', 'Strong Opioid','Neuropathic', 'APAP', 'Weak Opioid'))
    ),
     cte3 (RX_NPI, NSAID, OTHER_PAIN, STRONG_OPIOID, NEUROPATHIC, APAP, WEAK_OPIOID) as(
       select * from cte2
    ),
     cte4 as(
       select RX_NPI,
              COALESCE(NSAID, 0) as NSAID,
              COALESCE(OTHER_PAIN, 0) as OTHER_PAIN,
              COALESCE(STRONG_OPIOID, 0) as STRONG_OPIOID,
              COALESCE(NEUROPATHIC, 0) as NEUROPATHIC,
              COALESCE(APAP, 0) as APAP,
              COALESCE(WEAK_OPIOID, 0) as WEAK_OPIOID
       from cte3
    ),
     rx_hcps as(
       select distinct RX_NPI, REFERRING_NPI
       from TBLREF_RXMX_EXT_REFERRALS
       where RX_NPI != REFERRING_NPI
       order by RX_NPI
    ),
     rx_hcps_distr as(
       select a.RX_NPI, a.REFERRING_NPI,
              b.NSAID as RX_NPI_NSAID,
              b.OTHER_PAIN as RX_NPI_OTHER_PAIN,
              b.STRONG_OPIOID as RX_NPI_STRONG_OPIOID,
              b.NEUROPATHIC as RX_NPI_NEUROPATHIC,
              b.APAP as RX_NPI_APAP,
              b.WEAK_OPIOID as RX_NPI_WEAK_OPIOID
       from rx_hcps a
       left join cte4 b on a.RX_NPI = b.RX_NPI
    ),
     rx_refer_hcps_distr as(
       select a.*,
              b.NSAID as REF_NPI_NSAID,
              b.OTHER_PAIN as REF_NPI_OTHER_PAIN,
              b.STRONG_OPIOID as REF_NPI_STRONG_OPIOID,
              b.NEUROPATHIC as REF_NPI_NEUROPATHIC,
              b.APAP as REF_NPI_APAP,
              b.WEAK_OPIOID as REF_NPI_WEAK_OPIOID
       from rx_hcps_distr a
       left join cte4 b on a.REFERRING_NPI = b.RX_NPI
    ),
     specs as(
       select RX_NPI as NPI, RX_SPEC as SPECIALTY
       from(
         select RX_NPI, RX_SPEC from TBLREF_RXMX_EXT_REFERRALS
         union
         select MX_NPI, MX_SPEC from TBLREF_RXMX_EXT_REFERRALS
         union
         select REFERRING_NPI, REFERRING_SPEC from TBLREF_RXMX_EXT_REFERRALS
       )
    ),
     rx_refer_hcps_distr_rxspec as(
       select a.*, b.SPECIALTY as RX_SPEC
       from rx_refer_hcps_distr a
       left join specs b on a.RX_NPI = b.NPI
    ),
     rx_refer_hcps_distr_rx_ref_spec as(
       select a.*, b.SPECIALTY as REF_SPEC
       from rx_refer_hcps_distr_rxspec a
       left join specs b on a.REFERRING_NPI = b.NPI
    ),
    chisqr1 as(
       select *,
              RX_NPI_NSAID+RX_NPI_OTHER_PAIN+RX_NPI_STRONG_OPIOID+RX_NPI_NEUROPATHIC+RX_NPI_APAP+RX_NPI_WEAK_OPIOID as RX_SUM_TOT,
              REF_NPI_NSAID+REF_NPI_OTHER_PAIN+REF_NPI_STRONG_OPIOID+REF_NPI_NEUROPATHIC+REF_NPI_APAP+REF_NPI_WEAK_OPIOID as REF_SUM_TOT,
              RX_NPI_NSAID+REF_NPI_NSAID as NSAID_SUM_TOT,
              RX_NPI_OTHER_PAIN+RX_NPI_OTHER_PAIN as OTHER_PAIN_SUM_TOT,
              RX_NPI_STRONG_OPIOID+REF_NPI_STRONG_OPIOID as STRONG_OPIOID_SUM_TOT,
              RX_NPI_NEUROPATHIC+REF_NPI_NEUROPATHIC as NEUROPATHIC_SUM_TOT,
              RX_NPI_APAP+REF_NPI_APAP as APAP_SUM_TOT,
              RX_NPI_WEAK_OPIOID+REF_NPI_WEAK_OPIOID as WEAK_OPIOID_SUM_TOT
       from rx_refer_hcps_distr_rx_ref_spec
    ),
    chisqr2 as(
       select *,
              DIV0((NSAID_SUM_TOT*RX_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e1,
              DIV0((OTHER_PAIN_SUM_TOT*RX_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e2,
              DIV0((STRONG_OPIOID_SUM_TOT*RX_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e3,
              DIV0((NEUROPATHIC_SUM_TOT*RX_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e4,
              DIV0((APAP_SUM_TOT*RX_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e5,
              DIV0((WEAK_OPIOID_SUM_TOT*RX_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e6,
              DIV0((NSAID_SUM_TOT*REF_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e7,
              DIV0((OTHER_PAIN_SUM_TOT*REF_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e8,
              DIV0((STRONG_OPIOID_SUM_TOT*REF_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e9,
              DIV0((NEUROPATHIC_SUM_TOT*REF_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e10,
              DIV0((APAP_SUM_TOT*REF_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e11,
              DIV0((WEAK_OPIOID_SUM_TOT*REF_SUM_TOT),(RX_SUM_TOT+REF_SUM_TOT)) as e12
       from chisqr1
    ),
    chisqr3 as(
       select *,
              DIV0(POWER(RX_NPI_NSAID-e1,2),e1)+DIV0(POWER(RX_NPI_OTHER_PAIN-e2,2),e2)+DIV0(POWER(RX_NPI_STRONG_OPIOID-e3,2),e3)+
              DIV0(POWER(RX_NPI_NEUROPATHIC-e4,2),e4)+DIV0(POWER(RX_NPI_APAP-e5,2),e5)+DIV0(POWER(RX_NPI_WEAK_OPIOID-e6,2),e6)+
              DIV0(POWER(REF_NPI_NSAID-e7,2),e7)+DIV0(POWER(REF_NPI_OTHER_PAIN-e8,2),e8)+DIV0(POWER(REF_NPI_STRONG_OPIOID-e9,2),e9)+
              DIV0(POWER(REF_NPI_NEUROPATHIC-e10,2),e10)+DIV0(POWER(REF_NPI_APAP-e11,2),e11)+DIV0(POWER(REF_NPI_WEAK_OPIOID-e12,2),e12) as chisqr
       from chisqr2
    ),
    results_spec as(
       select RX_SPEC, RX_NPI,
              AVG(chisqr) as avg_chisqr,
              count(*) as num_refs
       from chisqr3
       --where RX_SPEC in ('Nurse Practitioner','Physician Assistant','Emergency Medicine','Family Medicine','Internal Medicine','Orthopaedic Surgery','Surgery')
       group by RX_SPEC, RX_NPI
       order by RX_SPEC
    ),
    results_rx_npi as(
       select RX_NPI,
              AVG(chisqr) as avg_chisqr,
              count(*) as num_refs
       from chisqr3
       group by RX_NPI
    ),
    results_ref_npi as(
       select REFERRING_NPI,
              AVG(chisqr) as avg_chisqr,
              count(*) as num_refs
       from chisqr3
       group by REFERRING_NPI
    )


--this is the final table for analysis
--the population here is all HCPS from TBLREF_RXMX_EXT_REFERRALS table
--their writing behavior was also taken from the same table
--there are some nulls here which should be removed
--df = (6-1)*(6-1) = 25
--alpha = 0.05
--critical value = 37.652

--select RX_SPEC,
--       AVG(AVG_CHISQR) as avg_avg_chisqr
--from results_spec
--group by RX_SPEC
--order by 2 desc;

--lets also view by npi level autonomy instead of just specialty at rx npi and referral npi
--select * from results_rx_npi
--where avg_chisqr is not NULL
--order by avg_chisqr desc;

select * from results_ref_npi
where avg_chisqr is not NULL
order by avg_chisqr desc;