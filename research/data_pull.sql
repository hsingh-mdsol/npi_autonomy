select RX_NPI, REFERRING_NPI, NDC_DRUG_TYPE, count(*) as rx_count from TBLREF_RXMX_EXT_REFERRALS
group by RX_NPI, REFERRING_NPI, NDC_DRUG_TYPE
order by RX_NPI;

select * from TBLREF_RXMX_EXT_REFERRALS;


--get master distribution table of npis and their rx count for each ndc drug type
--get referral combinations
--join master distribution table on referral combinations table on rx'ing npi and referring npi separately!

select RX_NPI as NPI, RX_SPEC as SPECIALTY from (
  select RX_NPI, RX_SPEC from TBLREF_RXMX_EXT_REFERRALS
  union
  select MX_NPI, MX_SPEC from TBLREF_RXMX_EXT_REFERRALS
  union
  select REFERRING_NPI, REFERRING_SPEC from TBLREF_RXMX_EXT_REFERRALS
);

---- every rx npi has one unique specialty
--select RX_NPI, count(distinct RX_SPEC) as num_spec from TBLREF_RXMX_EXT_REFERRALS
--group by RX_NPI
--order by 2 desc;
--
---- every mx npi has one unique specialty
--select MX_NPI, count(distinct MX_SPEC) as num_spec from TBLREF_RXMX_EXT_REFERRALS
--group by MX_NPI
--order by 2 desc;
--
---- every referring npi has one unique specialty
--select REFERRING_NPI, count(distinct REFERRING_SPEC) as num_spec from TBLREF_RXMX_EXT_REFERRALS
--group by REFERRING_NPI
--order by 2 desc;

--14,293,201
select * from TBLREF_RXMX_EXT_REFERRALS
where RX_NPI in (select distinct REFERRING_NPI from TBLREF_RXMX_EXT_REFERRALS);

--referral rx combinations
select distinct RX_NPI, REFERRING_NPI from TBLREF_RXMX_EXT_REFERRALS
where RX_NPI != REFERRING_NPI
order by RX_NPI;

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
    )
--this is the final table for analysis
--the population here is all HCPS from TBLREF_RXMX_EXT_REFERRALS table 
--their writing behavior was also taken from the same table
select * from rx_refer_hcps_distr_rx_ref_spec;
       



