
create function public.datediff_business_days (date, date )
  returns bigint
  stable
as $$
   SELECT (
     DATEDIFF('day', $1, $2) -
     (
       (
         FLOOR(DATEDIFF('day', $1, $2) / 7) * 2
       )::INTEGER +
       CASE WHEN DATE_PART(dow, $1) - DATE_PART(dow, $2) IN (1, 2, 3, 4, 5) AND DATE_PART(dow, $2) != 0
         THEN 2
       ELSE 0
       END::INTEGER +

       CASE WHEN DATE_PART(dow, $1) != 0 AND DATE_PART(dow, $2) = 0
         THEN 1
       ELSE 0
       END::INTEGER +

       CASE WHEN DATE_PART(dow, $1) = 0 AND DATE_PART(dow, $2) != 0
         THEN 1
       ELSE 0
       END::INTEGER
     )
   )::BIGINT
$$ language sql;

grant execute on function public.datediff_business_days(date,date) to public;


CREATE TABLE IF NOT EXISTS registration.lead (
  lead_id VARCHAR(128) PRIMARY KEY NOT NULL DISTKEY ENCODE ZSTD ,
  acquired_at TIMESTAMP NOT NULL SORTKEY ENCODE ZSTD
) DISTSTYLE KEY ;


CREATE TABLE IF NOT EXISTS registration.visitor (
  visitor_id VARCHAR(36) NOT NULL PRIMARY KEY DISTKEY ENCODE ZSTD,
  event_id   VARCHAR(36) NOT NULL ENCODE ZSTD,
  event_fingerprint VARCHAR(36) ENCODE ZSTD,
  acquired_at TIMESTAMP NOT NULL SORTKEY ENCODE ZSTD
) DISTSTYLE KEY;