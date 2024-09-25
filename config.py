import os
from datetime import datetime, timedelta

class Config:
   db_config = {
    'user': 'root',
    'password': 'RoboShop@1',
    'host': '107.20.47.134',  # Public IPv4 address
    'database': 'test_db'
}
   startdate = datetime.now() - timedelta(days=1)
   enddate = startdate + timedelta(days=1)
   startdate_str = startdate.strftime('%Y-%m-%d %H:%M:%S')
   enddate_str = enddate.strftime('%Y-%m-%d %H:%M:%S')
   query = f'''
select EXTRACT(HOUR from CAST(ie.created_date_time AS TIMESTAMP)) as "SCAN HOUR"
       ,ed.event_type_id as "SCAN EVENT"
       ,et.event_type_en_desc as "SCAN EVENT TYPE"
       ,count(ED.NUMBER_OF_ITEMS) as "NUMBER OF SCANS"
       ,count(DISTINCT(ED.event_id)) as "NUMBER OF PAYLOADS"
  from DELIVERY.ITEM_EVENTS ie
  inner join delivery.event_details ed
     on ed.event_id = ie.event_id
  inner join delivery.event_type et
     on et.event_type_id = ed.event_type_id
  where ie.created_date_time >= {startdate_str}
    and ie.created_date_time < {enddate_str}
group by EXTRACT(HOUR from CAST(ie.created_date_time AS TIMESTAMP)), ed.event_type_id, et.event_type_en_desc
UNION
select EXTRACT(HOUR from CAST(rlse.created_date_time AS TIMESTAMP)) as "SCAN HOUR"
       ,ed.event_type_id as "SCAN EVENT"
       ,et.event_type_en_desc as "SCAN EVENT TYPE"
       ,count(ED.NUMBER_OF_ITEMS) as "NUMBER OF SCANS"
       ,count(DISTINCT(ED.event_id)) as "NUMBER OF PAYLOADS"
  from DELIVERY.RSMC_LOG_SHEET_EVENTS rlse
  inner join delivery.event_details ed
     on ed.event_id = rlse.event_id
  inner join delivery.event_type et
     on et.event_type_id = ed.event_type_id
  where rlse.created_date_time >= {startdate_str}
    and rlse.created_date_time < {enddate_str}
group by EXTRACT(HOUR from CAST(rlse.created_date_time AS TIMESTAMP)), ed.event_type_id, et.event_type_en_desc
UNION
select EXTRACT(HOUR from CAST(IEO.created_date_time AS TIMESTAMP)) as "SCAN HOUR"
       ,ed.event_type_id as "SCAN EVENT"
       ,et.event_type_en_desc as "SCAN EVENT TYPE"
       ,count(ED.NUMBER_OF_ITEMS) as "NUMBER OF SCANS"
       ,count(DISTINCT(ED.event_id)) as "NUMBER OF PAYLOADS"
  from DELIVERY.ITEM_EVENTS_OTHERS IEO
  INNER JOIN DELIVERY.EVENT_DETAILS ED
     ON ED.EVENT_ID = IEO.EVENT_ID
  inner join delivery.event_type et
     on et.event_type_id = ed.event_type_id
  where IEO.created_date_time >= {startdate_str}
    and IEO.created_date_time < {enddate_str}
group by EXTRACT(HOUR from CAST(IEO.created_date_time AS TIMESTAMP)), ed.event_type_id, et.event_type_en_desc
union
select EXTRACT(HOUR from CAST(fv.modified_date_time AS TIMESTAMP)) as "SCAN HOUR"
       ,fv.furniture_visit_status_id as "SCAN EVENT"
       ,case
          when fv.furniture_visit_status_id = 1 then 'New SLB Clearance'
          when fv.furniture_visit_status_id = 2 then 'Uploaded SLB Clearance'
          when fv.furniture_visit_status_id = 3 then 'Downloaded SLB Clearance'
          when fv.furniture_visit_status_id = 4 then 'Missed SLB Clearance'
        end as "SCAN EVENT TYPE"
       ,count(*) as "NUMBER OF SCANS"
       ,COUNT(*) AS "NUMBER OF PAYLOADS"
  from FURNITURE.FURNITURE_VISIT fv
  inner join FURNITURE.FURNITURE_VISIT_STATUS fvs
     on fvs.furniture_visit_status_id = fv.furniture_visit_status_id
  where fv.created_date_time >= {startdate_str}
    and fv.created_date_time < {enddate_str}
group by EXTRACT(HOUR from CAST(fv.modified_date_time AS TIMESTAMP))
         ,fv.furniture_visit_status_id
         ,case
          when fv.furniture_visit_status_id = 1 then 'New SLB Clearance'
          when fv.furniture_visit_status_id = 2 then 'Uploaded SLB Clearance'
          when fv.furniture_visit_status_id = 3 then 'Downloaded SLB Clearance'
          when fv.furniture_visit_status_id = 4 then 'Missed SLB Clearance'
        end
union
select EXTRACT(HOUR from CAST(pr.created_date_time AS TIMESTAMP)) as "SCAN HOUR"
       ,cast(pr.pickup_type_id as number) as "SCAN EVENT"
       ,case
          when pr.pickup_type_id = '01' then 'Scheduled Pickup'
          when pr.pickup_type_id = '02' then 'On-Demand Pickup'
          when pr.pickup_type_id = '03' then 'Adhoc Pickup'
          when pr.pickup_type_id = '05' then 'Manual Pickup'
        end as "SCAN EVENT TYPE"
       ,count(*) as "NUMBER OF SCANS"
       ,COUNT(*) AS "NUMBER OF PAYLOADS"
  from despatch_pickup.pickup_request pr
  where pr.created_date_time >= {startdate_str}
    and pr.created_date_time < {enddate}
group by EXTRACT(HOUR from CAST(pr.created_date_time AS TIMESTAMP))
         ,pr.pickup_type_id
         ,case
          when pr.pickup_type_id = '01' then 'Scheduled Pickup'
          when pr.pickup_type_id = '02' then 'On-Demand Pickup'
          when pr.pickup_type_id = '03' then 'Adhoc Pickup'
          when pr.pickup_type_id = '05' then 'Manual Pickup'
        end
union
select EXTRACT(HOUR from CAST(IEI.created_date_time AS TIMESTAMP)) as "SCAN HOUR"
       ,i.event_type_id as "SCAN EVENT"
       ,et.event_type_en_desc as "SCAN EVENT TYPE"
       ,count(I.NUMBER_OF_ITEMS) as "NUMBER OF SCANS"
       ,COUNT(DISTINCT(I.IDB_ID)) AS "NUMBER OF PAYLOADS"
  from DELIVERY.ITEM_EVENTS_IDB IEI
  INNER JOIN DELIVERY.IDB I
     ON I.IDB_ID = IEI.IDB_ID
  inner join delivery.event_type et
     on et.event_type_id = i.event_type_id
  where IEI.created_date_time >= {startdate_str}
    and IEI.created_date_time < {enddate_str}
group by EXTRACT(HOUR from CAST(IEI.created_date_time AS TIMESTAMP)), i.event_type_id, et.event_type_en_desc
union
select EXTRACT(HOUR from CAST(IDS.created_date_time AS TIMESTAMP)) as "SCAN HOUR"
       ,DSED.EVENT_TYPE_ID as "SCAN EVENT"
       ,et.event_type_en_desc as "SCAN EVENT TYPE"
       ,COUNT(DSED.NUMBER_OF_ITEMS) as "NUMBER OF SCANS"
       ,COUNT(DISTINCT(DSED.EVENT_ID)) AS "NUMBER OF PAYLOADS"
  from DELIVERY.ITEM_DSA_SORT IDS
  INNER JOIN DELIVERY.DSA_SORT_EVENT_DETAILS DSED
     ON DSED.EVENT_ID = IDS.EVENT_ID
  inner join delivery.event_type et
     on et.event_type_id = DSED.EVENT_TYPE_ID
  where IDS.created_date_time >= {startdate_str}
    and IDS.created_date_time < {enddate_str}
group by EXTRACT(HOUR from CAST(IDS.created_date_time AS TIMESTAMP)), DSED.EVENT_TYPE_ID, et.event_type_en_desc;
'''
