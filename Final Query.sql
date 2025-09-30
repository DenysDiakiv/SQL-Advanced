-- CTE: Метрики по акаунтах (з дати сесії як проксі для створення)
with accounts as (
  select
    s.date as date,
    p.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    count(distinct a.id) as account_cnt
  from `DA.account` a
  join `DA.account_session` ase on a.id = ase.account_id
  left join `DA.session` s on ase.ga_session_id = s.ga_session_id
  left join `DA.session_params` p on s.ga_session_id = p.ga_session_id
  group by s.date, p.country, a.send_interval, a.is_verified, a.is_unsubscribed
),


-- CTE: Об'єднання метрик по email подіях
emails as (
  select
    date_add(date(s.date), interval e.sent_date day) as date,
    p.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    count(distinct e.id_message) as sent_msg,
    0 as open_msg,
    0 as visit_msg
  from `DA.email_sent` e
  join `DA.account` a on e.id_account = a.id
  join `DA.account_session` ase on a.id = ase.account_id
  left join `DA.session` s on ase.ga_session_id = s.ga_session_id
  left join `DA.session_params` p on s.ga_session_id = p.ga_session_id
  where e.sent_date is not null
  group by date, p.country, a.send_interval, a.is_verified, a.is_unsubscribed


  union all


  select
    date_add(date(s.date), interval e.open_date day) as date,
    p.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    0,
    count(distinct e.id_message),
    0
  from `DA.email_open` e
  join `DA.account` a on e.id_account = a.id
  join `DA.account_session` ase on a.id = ase.account_id
  left join `DA.session` s on ase.ga_session_id = s.ga_session_id
  left join `DA.session_params` p on s.ga_session_id = p.ga_session_id
  where e.open_date is not null
  group by date, p.country, a.send_interval, a.is_verified, a.is_unsubscribed


  union all


  select
    date_add(date(s.date), interval e.visit_date day) as date,
    p.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    0,
    0,
    count(distinct e.id_message)
  from `DA.email_visit` e
  join `DA.account` a on e.id_account = a.id
  join `DA.account_session` ase on a.id = ase.account_id
  left join `DA.session` s on ase.ga_session_id = s.ga_session_id
  left join `DA.session_params` p on s.ga_session_id = p.ga_session_id
  where e.visit_date is not null
  group by date, p.country, a.send_interval, a.is_verified, a.is_unsubscribed
),


--  CTE: Агрегація емейл метрик
full_email as (
  select
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    sum(sent_msg) as sent_msg,
    sum(open_msg) as open_msg,
    sum(visit_msg) as visit_msg
  from emails
  group by date, country, send_interval, is_verified, is_unsubscribed
),


--  Об’єднання акаунтів і емейлів з total + rank
result as (
  select
    coalesce(a.date, f.date) as date,
    coalesce(a.country, f.country) as country,
    coalesce(a.send_interval, f.send_interval) as send_interval,
    coalesce(a.is_verified, f.is_verified) as is_verified,
    coalesce(a.is_unsubscribed, f.is_unsubscribed) as is_unsubscribed,
    ifnull(a.account_cnt, 0) as account_cnt,
    ifnull(f.sent_msg, 0) as sent_msg,
    ifnull(f.open_msg, 0) as open_msg,
    ifnull(f.visit_msg, 0) as visit_msg,
    sum(ifnull(a.account_cnt, 0)) over(partition by coalesce(a.country, f.country)) as total_country_account_cnt,
    sum(ifnull(f.sent_msg, 0)) over(partition by coalesce(a.country, f.country)) as total_country_sent_cnt
  from accounts a
  full outer join full_email f
    on a.date = f.date
    and a.country = f.country
    and a.send_interval = f.send_interval
    and a.is_verified = f.is_verified
    and a.is_unsubscribed = f.is_unsubscribed
),


--  Рейтинги та фільтр ТОП-10
ranked as (
  select *,
    dense_rank() over(order by total_country_account_cnt desc) as rank_total_country_account_cnt,
    dense_rank() over(order by total_country_sent_cnt desc) as rank_total_country_sent_cnt
  from result
)


--  Фінальний результат
select *
from ranked
where rank_total_country_account_cnt <= 10 or rank_total_country_sent_cnt <= 10
