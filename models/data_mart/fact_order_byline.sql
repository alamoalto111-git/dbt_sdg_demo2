{{ config(materialized='table') }}

select
 LINK_ORDER_LINEITEM_HK
,HUB_ORDER_HK
,HUB_PART_HK
,HUB_SUPPLIER_HK
,HUB_CUSTOMER_HK

,ORDER_KEY
,LINENUMBER_KEY
,PART_KEY
,SUPPLIER_KEY
,CUSTOMER_KEY



---dates
,bv_cal_1.DATE_KEY AS ORDERDATE_key
,bv_cal_2.DATE_KEY AS LINESHIPDATE_key
,bv_cal_3.DATE_KEY AS LINECOMMITDATE_key
,bv_cal_4.DATE_KEY AS LINERECEIPTDATE_key


,O_ORDERSTATUS as order_status_key
,L_LINESTATUS as line_status_key

,L_RETURNFLAG as line_returnflag_key
,O_TOTALPRICE
--,L_EXTENDEDPRICE
,QUANTITY
,UNIT_COST
,UNIT_PRICE
,TOTAL_GROSS_SALES
,DISCOUNT_PCT
,DISCOUNT_AMOUNT
,TOTAL_NET_SALES
,TAX_PCT
,TAX_AMOUNT
,TOTAL_INVOICE_AMOUNT
,TOTAL_COST
,TOTAL_GROSS_PROFIT
,TOTAL_GROSS_PROFIT_MARGIN_PCT
,FLAG_WITH_DISCOUNT
,FLAG_WITH_TAX
,L_COMMENT
--,CUSTOMER_NAME
--,SUPPLIER_NAME
--,PART_NAME
--,P_BRAND
,P_TYPE
,P_CONTAINER

from {{ ref('buv_order_byline') }} bv_base

  join  {{ ref('buv_calendar') }}  bv_cal_1
  on bv_base.o_orderdate = bv_cal_1.date_ymd

  join  {{ ref('buv_calendar') }}  bv_cal_2
  on bv_base.L_SHIPDATE = bv_cal_2.date_ymd

  join  {{ ref('buv_calendar') }}  bv_cal_3
  on bv_base.L_COMMITDATE = bv_cal_3.date_ymd

  join  {{ ref('buv_calendar') }}  bv_cal_4
  on bv_base.L_RECEIPTDATE = bv_cal_4.date_ymd





--join  {{ ref('buv_customer') }}  bv_cust
--on bv_base.HUB_CUSTOMER_HK = bv_cust.HUB_CUSTOMER_HK
