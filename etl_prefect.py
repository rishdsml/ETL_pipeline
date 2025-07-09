from prefect import flow, task, get_run_logger
import pandas as pd
import pyodbc
from datetime import datetime
import PostgresConPref
import calendar
today_date = str(datetime.today())


@task
def CompleteSuperDashboardTask():
    try:
        logger = get_run_logger()
        print("Starting SuperDashboard Task")

        start = datetime(2025, 7, 1)
        end = datetime(2025, 7, 9)

        current = start 
        date = []
        while current < end:
            date.append(current)

            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)


        connection_string_sql_prod = (
            'Driver={ODBC Driver 17 for SQL Server};'
            'Server=SQLDBTest.Medvol.in;'
            'Database=MedvolDB_TestQC;'
            'UID=ashok;'
            'PWD=Khyati@123456'
        )
        print("Connecting to SQL Server...")
        conn_sql = pyodbc.connect(connection_string_sql_prod)
        print("SQL Server connected.")


        
        queryInsert = '''
            with psr as (
                SELECT
                    psr_code,
                    PSR_Shortcode,
                    PSR_Name,
                    Designation,
                    L1_EC,
                    L1_EmployeeName,
                    Company_Code,
                    PSR_UniqueId,
                    Active_Status,
                    HeadQuarterID,
                    Position_code
                FROM
                    PSRMaster_Hdr
                WHERE
                    Active_Status = 0
                    and psr_name not like 'test%'
                    and psr_name is not null
                    and psr_name not in ('', ' ')
            ),
            L0 as (
                select
                    psr.*,
                    Company_Divisioncode
                from
                    psr
                    JOIN PSRWiseDivision_lnk AS pslink on psr.psr_code = pslink.psr_code
                where
                    psr.Designation = 'PSR'
            ),
            L1 as (
                select
                    L0.PSR_Shortcode as L0_Code,
                    L0.PSR_Name as L0_Name,
                    L0.Designation as L0_Designation,
                    L0.Position_Code,
                    L0.Company_Divisioncode,
                    L0.L1_EC as L1_Code,
                    L0.Company_Code,
                    L0.HeadQuarterID,
                    L0.Position_code as L0_PositionCode,
                    psr.L1_EC as L2_Code,
                    psr.PSR_Name as L1_Name
                from
                    L0
                    left join psr on L0.L1_EC = psr.PSR_Shortcode
            ),
            L2 as (
                select
                    L1.*,
                    psr.L1_EC as L3_Code,
                    psr.PSR_Name as L2_Name
                from
                    L1
                    left join psr on L1.L2_code = psr.PSR_Shortcode
            ),
            L3 as (
                select
                    L2.*,
                    psr.L1_EC as L4_Code,
                    psr.PSR_Name as L3_Name
                from
                    L2
                    left join psr on L2.L3_code = psr.PSR_Shortcode
            ),
            L4 as (
                select
                    L3.*,
                    psr.L1_EC as L5_Code,
                    psr.PSR_Name as L4_Name
                from
                    L3
                    left join psr on L3.L4_code = psr.PSR_Shortcode
            ),
            L5 as (
                select
                    L4.*,
                    psr.L1_EC as L6_Code,
                    psr.PSR_Name as L5_Name
                from
                    L4
                    left join psr on L4.L5_code = psr.PSR_Shortcode
            ),
            L6 as (
                select
                    L5.*,
                    psr.L1_EC as L7_Code,
                    psr.PSR_Name as L6_Name
                from
                    L5
                    left join psr on L5.L6_code = psr.PSR_Shortcode
            ),
            L7 as (
                select
                    L6.*,
                    psr.L1_EC as L8_Code,
                    psr.PSR_Name as L7_Name
                from
                    L6
                    left join psr on L6.L7_code = psr.PSR_Shortcode
            ),
            L8 as (
                select
                    L7.*,
                    psr.L1_EC as L9_Code,
                    psr.PSR_Name as L8_Name
                from
                    L7
                    left join psr on L7.L8_code = psr.PSR_Shortcode
            ),
            psr_level as (
                select
                    L8.*,
                    psr.L1_EC as L10_Code,
                    psr.PSR_Name as L9_Name
                from
                    L8
                    left join psr on L8.L9_code = psr.PSR_Shortcode
            ),
            emp_tree as (
                select
                    psr_level.*,
                    Doctor_Code,
                    Location_Name,
                    DoctorHeadQuarter_Lnk.Location_Code,
                    DoctorHeadQuarter_Lnk.Company_DivisionCode as division_code1,
                    DoctorHeadQuarter_Lnk.HeadQuarterID as HQ1
                from
                    psr_level
                    left join DoctorHeadQuarter_Lnk on DoctorHeadQuarter_Lnk.Position_Code = psr_level.Position_Code
                    and DoctorHeadQuarter_Lnk.Company_DivisionCode = psr_level.Company_DivisionCode
                    left join CounterLocation_Hdr on DoctorHeadQuarter_Lnk.Location_Code = CounterLocation_Hdr.Location_Code
            ),
            counter_view as (
                select
                    emp_tree.*,
                    Cast(Registration_Date as varchar) as Registration_Date,
                    headquater_name as Headquater_Name,
                    Counter_LinkType,
                    Doctor_Name,
                    co.company_name as Company_Name,
                    co.Company_Shortcode,
                    co.Company_Shortname,
                    cd.division_name as Division_Name,
                    doc.City_code as City_Code,
                    city.city_name as City,
                    dist.District_Name as District_Name,
                    st.state_code,
                    st.state_name as State_Name,
                    CounterCompany_Lnk.CreatedDate as CCL_CreatedDate,
                    CounterCompany_Lnk.ModifiedDate as CCL_ModifiedDate,
                    doc.CreatedDate as DDH_CreatedDate,
                    doc.ModifiedDate AS DDH_ModifiedDate,
                    dist.Created_Date as DISTRICT_CreatedDate,
                    dist.modified_date AS DISTRICT_ModifiedDate,
                    co.Created_Date as COMPANY_CreatedDate,
                    co.ModifieDate as COMPANY_ModifiedDate,
                    cd.CreatedDate as Division_CreatedDate,
                    cd.ModifieDate AS Division_ModifiedDate,
                    HeadQuarter_Hdr.Created_Date as HQ_CreatedDate,
                    HeadQuarter_Hdr.Modified_date as HQ_ModifiedDate
                from
                    emp_tree
                    join CounterCompany_Lnk on CounterCompany_Lnk.counter_code = emp_tree.doctor_code
                    and CounterCompany_Lnk.Company_Code = emp_tree.Company_Code
                    and CounterCompany_Lnk.location_code = emp_tree.location_code
                    join DoctorDetails_Hdr as doc on doc.Doctor_Code = emp_tree.Doctor_Code
                    join CityMaster_Hdr as city on city.city_code = doc.city_code
                    join DistrictMaster_Hdr as dist on dist.District_code = city.District_code
                    join StateMaster_Hdr as st on st.state_code = dist.state_code
                    join company_hdr as co on co.company_code = emp_tree.company_code
                    join CompanyDivision_dtl cd on cd.Company_Divisioncode = emp_tree.Company_Divisioncode
                    left join HeadQuarter_Hdr on HeadQuarter_Hdr.headquater_id = emp_tree.HeadQuarterID
            ),
            hdr as (
                select
                    *
                from
                    OrderDetails_Hdr 
            ),
            order_hdr as (
                select
                    idt.Invoicedtl_Code,
                    dtl.Orderdtl_Code,
                    hdr.OrderHeader_Code,
                    Cast(order_prefix AS VARCHAR(20)) + RIGHT(
                        '0000' + CONVERT(VARCHAR(10), order_number),
                        4
                    ) AS Order_Number,
                    CAST(hdr.Order_Date as varchar) as Order_Date,
                    hdr.OrderPlaced_Date,
                    hdr.Doctor_code,
                    hdr.Stockist_Code,
                    hdr.Product_Count,
                    hdr.Ordered_By,
                    hdr.Drph_LinkType,
                    hdr.Pharmacist_Code,
                    dc.Doctor_Name as Pharmacy,
                    hdr.Order_Status,
                    hdr.Company_Code,
                    hdr.location_code,
                    hdr.Company_DivisionCode,
                    idt.order_item_code as order_Item_Code,
                    dtl.Item_Code,
                    dtl.Item_Description,
                    (
                        case
                            when hdr.Order_Status in (2, 3) then idt.Quantity
                            else dtl.Quantity
                        end
                    ) as Quantity,
                    dtl.Quantity as OrderDetailQuantity,
                    dtl.Price,
                    brand,
                    Price_Identifier,
                    DiscountOnPTS,
                    case
                        when dtl.price_identifier = 0 then Discountonpts
                        when dtl.price_identifier = 2 then IIF(
                            dtl.scheme_freeqty = 0,
                            0,
                            round(
                                (dtl.scheme_freeqty * 100) /(dtl.scheme_freeqty + dtl.scheme_qty),
                                0
                            )
                        )
                        when dtl.price_identifier = 1 then IIF(
                            dtl.PTR = 0,
                            0,
                            round((dtl.PTR - dtl.fixedprice) * 100 / dtl.PTR, 0)
                        )
                    end as discount,
                    dtl.PTS,
                    dtl.PTR,
                    dtl.FixedPrice,
                    dtl.Scheme_Qty,
                    dtl.Scheme_FreeQty,
                    orderstatus_desc as OrderStatus_Desc
                from
                    hdr
                    join OrderDetails_dtl as dtl on Hdr.OrderHeader_Code = dtl.OrderHeader_Code
                    left join InvoiceDetails_dtl as idt on dtl.OrderHeader_Code = idt.OrderHeader_Code
                        and dtl.Item_Code = idt.order_item_code
                    join OrderStatus_Hdr on OrderStatus_Hdr.orderstatus_code = hdr.Order_Status
                    join CompanyProduct_Hdr as product on product.item_code = dtl.item_code
                    left join DoctorDetails_Hdr as dc on dc.Doctor_Code = hdr.Pharmacist_Code
                where 
                    hdr.OrderPlaced_Date IS NOT NULL
                    and hdr.OrderPlaced_Date between '{0}-{1:02d}-{2:02d}' and '{0}-{1:02d}-{3:02d}'
            )

            ,stockist as  
            (
                select sdh.Stockist_Code,sdh.Stockist_UniqueId, sdc.StockistName as Stockist_Name, sdh.Stockist_City_code as stockist_city_code, sdh.companycode, sdc.Medvol_Field_UserID
                ,sdc.Ph_Code, city.City_Name as Stockist_City , dist.District_code as stockist_district_code, dist.District_Name as stockist_district, st.state_code as stockist_state_code, st.state_name as stockist_state
                from StockistDetails_Hdr as sdh left join StockistDetails_Cfg sdc on sdh.Stockist_UniqueId = sdc.Stockist_UniqueId
                join CityMaster_Hdr as city on city.city_code = sdh.Stockist_City_code
                join DistrictMaster_Hdr as dist on dist.District_code = city.District_code
                join StateMaster_Hdr as st on st.state_code = dist.state_code     
            )
            select
                Invoicedtl_Code,
                Orderdtl_Code,
                cast(isnull(Invoicedtl_Code, 0) AS VARCHAR(20)) + '_' + cast(isnull(Orderdtl_Code, 0) AS VARCHAR(20)) as DeltaKey,
                order_Item_Code,
                Item_Code,
                order_hdr.Stockist_Code,
                stockist.Stockist_Name,
                Product_Count,
                Ordered_By,
                Drph_LinkType,
                Pharmacist_Code,
                Pharmacy,
                Order_Status,
                OrderDetailQuantity,
                Price_Identifier,
                DiscountOnPTS,
                PTS,
                FixedPrice,
                Scheme_Qty,
                Scheme_FreeQty,
                OrderHeader_Code,
                Order_Number,
                Order_Date,
                ISNULL(OrderPlaced_Date, CAST(NULL AS DATE)) as OrderPlaced_Date,     
                OrderStatus_Desc,
                Item_Description,
                Brand,
                Quantity,
                Price,
                CAST(discount AS DECIMAL(7, 2)) as Discount,
                PTR,
                counter_view.L0_Code,
                counter_view.L0_Name,
                counter_view.L0_Designation,
                counter_view.Position_Code,
                counter_view.Company_Divisioncode,
                counter_view.L1_Code,
                counter_view.L1_Name,
                counter_view.Company_Code,
                counter_view.HeadQuarterID,
                counter_view.L0_PositionCode,
                counter_view.L2_Code,
                counter_view.L2_Name,
                counter_view.L3_Code,
                counter_view.L3_Name,
                counter_view.L4_Name,
                counter_view.L4_Code,
                counter_view.L5_Code,
                counter_view.L5_Name,
                counter_view.L6_Code,
                counter_view.L6_Name,
                counter_view.L7_Code,
                counter_view.L7_Name,
                counter_view.L8_Code,
                counter_view.L8_Name,
                counter_view.L9_Code,
                counter_view.L9_name,
                counter_view.Doctor_Code,
                counter_view.Location_Name,
                counter_view.Location_Code,
                counter_view.division_code1,
                counter_view.HQ1,
                counter_view.Registration_Date,
                counter_view.Headquater_Name,
                counter_view.Counter_LinkType,
                counter_view.Doctor_Name,
                counter_view.Company_Name,
                counter_view.Company_Shortcode,
                counter_view.Company_Shortname,
                counter_view.Division_Name,
                counter_view.City_Code,
                counter_view.City,
                District_Name,
                State_Code,
                State_Name,
                Medvol_Field_UserID,
                User_SSID as Medvol_field_ucode,
                User_Lnk_UniqueID,
                Ph_Code,
                stockist_city_code,
                Stockist_City, 
                stockist_district_code,
                stockist_district, 
                stockist_state_code,
                stockist_state,
                Cast(GETDATE() as date) as Mgtn_InsertDate,
                null as Mgtn_UpdateDate
            from
                counter_view
                join order_hdr on counter_view.Doctor_code = order_hdr.Doctor_Code
                and counter_view.Company_Divisioncode = order_hdr.Company_Divisioncode
                and counter_view.location_code = order_hdr.location_code
                left join stockist on stockist.Stockist_Code = order_hdr.Stockist_Code
                and stockist.companycode = order_hdr.company_code
                left join UserConsoleLoginDetails ucld on stockist.Medvol_Field_UserID = ucld.User_ID 
            where
                OrderPlaced_Date is not null
                and order_hdr.stockist_code not in ('1123232','900001','Dum4001')
        '''
        

        for dt in date:
          year = dt.year
          month = dt.month

          print(f"Processing year {year}, month {month}")

          last_day = calendar.monthrange(year, month)[1]

          # First half of the month
          print("Running SQL for 1st–15th")
          query = queryInsert.format(year, month, 1, 15)
          df = pd.read_sql_query(query, conn_sql)
          print(df.dtypes)
          print(df.describe(include='all'))
          print("Rows fetched:", df.shape[0])

          df['OrderPlaced_Date'] = pd.to_datetime(df['OrderPlaced_Date'], utc=True)

          print("Inserting into Postgres: SuperDashboardMedvol")
          PostgresConPref.save_to_postgres_insert(df, "SuperDashboardMedvol")
          print("Insert completed for first half.")
          df = pd.DataFrame()

    # Second half of the month
          print("Running SQL for 16th–end")
          query = queryInsert.format(year, month, 16, last_day)
          df = pd.read_sql_query(query, conn_sql)
          print("Rows fetched:", df.shape[0])

          df['OrderPlaced_Date'] = pd.to_datetime(df['OrderPlaced_Date'], utc=True)

          print("Inserting into Postgres: SuperDashboardMedvol")
          PostgresConPref.save_to_postgres_insert(df, "SuperDashboardMedvol")
          print("Insert completed for second half.")
          df = pd.DataFrame()
        conn_sql.close()
        print("SQL Server connection closed.")
        print("Migration completed on", today_date)

    except Exception as e:
        print("Error:", e)
        raise ValueError("OrderView Migration failed")
    
@task
def InsertUpdateOrderViewTask():   
    try:
        logger = get_run_logger()          
        connection_string_sql_prod = ( 
            'Driver={ODBC Driver 17 for SQL Server};'
            'Server=SQLDBTest.Medvol.in;'
            'Database=MedvolDB_TestQC;'
            'UID=ashok;'
            'PWD=Khyati@123456')
        conn_sql = pyodbc.connect(connection_string_sql_prod)  
        # querySnglDyDlt = ''' with psr as (      SELECT      psr_code,      PSR_Shortcode,      PSR_Name,      Designation,      L1_EC,      L1_EmployeeName,      Company_Code,      PSR_UniqueId,      Active_Status,      HeadQuarterID,      Position_code      FROM      PSRMaster_Hdr      WHERE      Active_Status = 0      and psr_name not like 'test%'      and psr_name is not null      and psr_name not in ('', ' ')      ),      L0 as (      select      psr.*,      Company_Divisioncode      from      psr      JOIN PSRWiseDivision_lnk AS pslink on psr.psr_code = pslink.psr_code      where      psr.Designation = 'PSR'      ),      L1 as (      select      L0.PSR_Shortcode as L0_Code,      L0.PSR_Name as L0_Name,      L0.Designation as L0_Designation,      L0.Position_Code,      L0.Company_Divisioncode,      L0.L1_EC as L1_Code,      L0.Company_Code,      L0.HeadQuarterID,      L0.Position_code as L0_PositionCode,      psr.L1_EC as L2_Code,      psr.PSR_Name as L1_Name      from      L0      left join psr on L0.L1_EC = psr.PSR_Shortcode      ),      L2 as (      select      L1.*,      psr.L1_EC as L3_Code,      psr.PSR_Name as L2_Name      from      L1      left join psr on L1.L2_code = psr.PSR_Shortcode      ),      L3 as (      select      L2.*,      psr.L1_EC as L4_Code,      psr.PSR_Name as L3_Name      from      L2      left join psr on L2.L3_code = psr.PSR_Shortcode      ),      L4 as (      select      L3.*,      psr.L1_EC as L5_Code,      psr.PSR_Name as L4_Name      from      L3      left join psr on L3.L4_code = psr.PSR_Shortcode      ),      L5 as (      select      L4.*,      psr.L1_EC as L6_Code,      psr.PSR_Name as L5_Name      from      L4      left join psr on L4.L5_code = psr.PSR_Shortcode      ),      L6 as (      select      L5.*,      psr.L1_EC as L7_Code,      psr.PSR_Name as L6_Name      from      L5      left join psr on L5.L6_code = psr.PSR_Shortcode      ),      L7 as (      select      L6.*,      psr.L1_EC as L8_Code,      psr.PSR_Name as L7_Name      from      L6      left join psr on L6.L7_code = psr.PSR_Shortcode      ),      L8 as (      select      L7.*,      psr.L1_EC as L9_Code,      psr.PSR_Name as L8_Name      from      L7      left join psr on L7.L8_code = psr.PSR_Shortcode      ),      psr_level as (      select      L8.*,      psr.L1_EC as L10_Code,      psr.PSR_Name as L9_Name      from      L8      left join psr on L8.L9_code = psr.PSR_Shortcode      ),      emp_tree as (      select      psr_level.*,      Doctor_Code,      Location_Name,      DoctorHeadQuarter_Lnk.Location_Code,      DoctorHeadQuarter_Lnk.Company_DivisionCode as division_code1,      DoctorHeadQuarter_Lnk.HeadQuarterID as HQ1      from      psr_level      left join DoctorHeadQuarter_Lnk on DoctorHeadQuarter_Lnk.Position_Code = psr_level.Position_Code      and DoctorHeadQuarter_Lnk.Company_DivisionCode = psr_level.Company_DivisionCode      left join CounterLocation_Hdr on DoctorHeadQuarter_Lnk.Location_Code = CounterLocation_Hdr.Location_Code      ),      counter_view as (      select      emp_tree.*,      Cast(Registration_Date as varchar) as Registration_Date,      headquater_name as Headquater_Name,      Counter_LinkType,      Doctor_Name,      co.company_name as Company_Name,      co.Company_Shortcode,      co.Company_Shortname,      cd.division_name as Division_Name,      doc.City_code as City_Code,      city.city_name as City,      dist.District_Name as District_Name,      st.state_name as State_Name,      st.state_code,      CounterCompany_Lnk.CreatedDate as CCL_CreatedDate,      CounterCompany_Lnk.ModifiedDate as CCL_ModifiedDate,      doc.CreatedDate as DDH_CreatedDate,      doc.ModifiedDate AS DDH_ModifiedDate,      dist.Created_Date as DISTRICT_CreatedDate,      dist.modified_date AS DISTRICT_ModifiedDate,      co.Created_Date as COMPANY_CreatedDate,      co.ModifieDate as COMPANY_ModifiedDate,      cd.CreatedDate as Division_CreatedDate,      cd.ModifieDate AS Division_ModifiedDate,      HeadQuarter_Hdr.Created_Date as HQ_CreatedDate,      HeadQuarter_Hdr.Modified_date as HQ_ModifiedDate      from      emp_tree      join CounterCompany_Lnk on CounterCompany_Lnk.counter_code = emp_tree.doctor_code      and CounterCompany_Lnk.Company_Code = emp_tree.Company_Code      and CounterCompany_Lnk.location_code = emp_tree.location_code      join DoctorDetails_Hdr as doc on doc.Doctor_Code = emp_tree.Doctor_Code      join CityMaster_Hdr as city on city.city_code = doc.city_code      join DistrictMaster_Hdr as dist on dist.District_code = city.District_code      join StateMaster_Hdr as st on st.state_code = dist.state_code      join company_hdr as co on co.company_code = emp_tree.company_code      join CompanyDivision_dtl cd on cd.Company_Divisioncode = emp_tree.Company_Divisioncode      left join HeadQuarter_Hdr on HeadQuarter_Hdr.headquater_id = emp_tree.HeadQuarterID      ),      hdr as (      select      *      from      OrderDetails_Hdr      ),      order_hdr as (      select      idt.Invoicedtl_Code,      dtl.Orderdtl_Code,      hdr.OrderHeader_Code,      Cast(order_prefix AS VARCHAR(20)) + RIGHT('0000' + CONVERT(VARCHAR(10), order_number), 4) AS Order_Number,      CAST(hdr.Order_Date as varchar) as Order_Date,      hdr.OrderPlaced_Date,      hdr.Doctor_code,      hdr.Stockist_Code,      hdr.Product_Count,      hdr.Ordered_By,      hdr.Drph_LinkType,      hdr.Pharmacist_Code,      hdr.Order_Status,      hdr.Company_Code,      hdr.location_code,      hdr.Company_DivisionCode,      idt.order_item_code as order_Item_Code,      dtl.Item_Code,      dtl.Item_Description,      (      case      when hdr.Order_Status in (2, 3) then idt.Quantity      else dtl.Quantity      end      ) as Quantity,      dtl.Quantity as OrderDetailQuantity,      dtl.Price,      brand,      Price_Identifier,      DiscountOnPTS,      case      when dtl.price_identifier = 0 then Discountonpts      when dtl.price_identifier = 2 then IIF(      dtl.scheme_freeqty = 0,      0,      round(      (dtl.scheme_freeqty * 100) /(dtl.scheme_freeqty + dtl.scheme_qty),      0      )      )      when dtl.price_identifier = 1 then IIF(      dtl.PTR = 0,      0,      round((dtl.PTR - dtl.fixedprice) * 100 / dtl.PTR, 0)      )      end as discount,      dtl.PTS,      dtl.PTR,      dtl.FixedPrice,      dtl.Scheme_Qty,      dtl.Scheme_FreeQty,      orderstatus_desc as OrderStatus_Desc      from      hdr      join OrderDetails_dtl as dtl on Hdr.OrderHeader_Code = dtl.OrderHeader_Code      left join InvoiceDetails_dtl as idt on dtl.OrderHeader_Code = idt.OrderHeader_Code      and dtl.Item_Code = idt.order_item_code      join OrderStatus_Hdr on OrderStatus_Hdr.orderstatus_code = hdr.Order_Status      join CompanyProduct_Hdr as product on product.item_code = dtl.item_code      where      cast(idt.Created_Date AS DATE) >= cast(      CONVERT(datetime, CONVERT(date, GETDATE() - DAY(0))) AS DATE      )      or cast(hdr.CreatedDate AS DATE) >= cast(      CONVERT(datetime, CONVERT(date, GETDATE() - DAY(0))) AS DATE      )      or cast(hdr.ModifieDate AS DATE) >= cast(      CONVERT(datetime, CONVERT(date, GETDATE() - DAY(0))) AS DATE      )      ),      stockist as (      select      sdh.Stockist_Code,      sdh.Stockist_UniqueId,      sdh.Stockist_Name,      sdh.Stockist_City_code,      sdh.companycode,      sdc.Medvol_Field_UserID      ,sdc.Ph_Code,      city.City_Name as Stockist_City,      dist.District_code as stockist_district_code,      dist.District_Name as stockist_district,      st.state_code as stockist_state_code,      st.state_name as stockist_state      from      StockistDetails_Hdr as sdh      left join StockistDetails_Cfg sdc on sdh.Stockist_UniqueId = sdc.Stockist_UniqueId      join CityMaster_Hdr as city on city.city_code = sdh.Stockist_City_code      join DistrictMaster_Hdr as dist on dist.District_code = city.District_code      join StateMaster_Hdr as st on st.state_code = dist.state_code      where      lower(sdh.Stockist_Name) not like '%test%'      )      select      Invoicedtl_Code,      Orderdtl_Code,      cast(isnull(Invoicedtl_Code, 0) AS VARCHAR(20)) + '_' + cast(isnull(Orderdtl_Code, 0) AS VARCHAR(20)) as DeltaKey,      order_Item_Code,      Item_Code,      order_hdr.Stockist_Code,      stockist.Stockist_Name,      Product_Count,      Ordered_By,      Drph_LinkType,      Pharmacist_Code,      Order_Status,      OrderDetailQuantity,      Price_Identifier,      DiscountOnPTS,      PTS,      FixedPrice,      Scheme_Qty,      Scheme_FreeQty,      OrderHeader_Code,      Order_Number,      Order_Date,      (      case      when OrderPlaced_Date is null then 0000 -00 -00      else OrderPlaced_Date      end      ) as OrderPlaced_Date,      OrderStatus_Desc,      Item_Description,      Brand,      Quantity,      Price,      CAST(discount AS DECIMAL(7, 2)) as Discount,      PTR,      counter_view.L0_Code,      counter_view.L0_Name,      counter_view.L0_Designation,      counter_view.Position_Code,      counter_view.Company_Divisioncode,      counter_view.L1_Code,      counter_view.L1_Name,      counter_view.Company_Code,      counter_view.HeadQuarterID,      counter_view.L0_PositionCode,      counter_view.L2_Code,      counter_view.L2_Name,      counter_view.L3_Code,      counter_view.L3_Name,      counter_view.L4_Name,      counter_view.L4_Code,      counter_view.L5_Code,      counter_view.L5_Name,      counter_view.L6_Code,      counter_view.L6_Name,      counter_view.L7_Code,      counter_view.L7_Name,      counter_view.L8_Code,      counter_view.L8_Name,      counter_view.L9_Code,      counter_view.L9_name,      counter_view.Doctor_Code,      counter_view.Location_Name,      counter_view.Location_Code,      counter_view.division_code1,      counter_view.HQ1,      counter_view.Registration_Date,      counter_view.Headquater_Name,      counter_view.Counter_LinkType,      counter_view.Doctor_Name,      counter_view.Company_Name,      counter_view.Company_Shortcode,      counter_view.Company_Shortname,      counter_view.Division_Name,      counter_view.City_Code,      counter_view.City,      District_Name,      State_Name,      Medvol_Field_UserID,      State_Code,      Ph_Code,      stockist_city_code,      Stockist_City,      stockist_district_code,      stockist_district,      stockist_state_code,      stockist_state,      Cast(GETDATE () as date) as Mgtn_InsertDate,      null as Mgtn_UpdateDate      from      counter_view      join order_hdr on counter_view.Doctor_code = order_hdr.Doctor_Code      and counter_view.Company_Divisioncode = order_hdr.Company_Divisioncode      and counter_view.location_code = order_hdr.location_code      left join stockist on stockist.Stockist_Code = order_hdr.Stockist_Code      and stockist.companycode = order_hdr.company_code      where      OrderPlaced_Date is not null    '''
        querySnglDyDlt = '''
            with psr as (
                SELECT
                    psr_code,
                    PSR_Shortcode,
                    PSR_Name,
                    Designation,
                    L1_EC,
                    L1_EmployeeName,
                    Company_Code,
                    PSR_UniqueId,
                    Active_Status,
                    HeadQuarterID,
                    Position_code
                FROM
                    PSRMaster_Hdr
                WHERE
                    Active_Status = 0
                    and psr_name not like 'test%'
                    and psr_name is not null
                    and psr_name not in ('', ' ')
            ),
            L0 as (
                select
                    psr.*,
                    Company_Divisioncode
                from
                    psr
                    JOIN PSRWiseDivision_lnk AS pslink on psr.psr_code = pslink.psr_code
                where
                    psr.Designation = 'PSR'
            ),
            L1 as (
                select
                    L0.PSR_Shortcode as L0_Code,
                    L0.PSR_Name as L0_Name,
                    L0.Designation as L0_Designation,
                    L0.Position_Code,
                    L0.Company_Divisioncode,
                    L0.L1_EC as L1_Code,
                    L0.Company_Code,
                    L0.HeadQuarterID,
                    L0.Position_code as L0_PositionCode,
                    psr.L1_EC as L2_Code,
                    psr.PSR_Name as L1_Name
                from
                    L0
                    left join psr on L0.L1_EC = psr.PSR_Shortcode
            ),
            L2 as (
                select
                    L1.*,
                    psr.L1_EC as L3_Code,
                    psr.PSR_Name as L2_Name
                from
                    L1
                    left join psr on L1.L2_code = psr.PSR_Shortcode
            ),
            L3 as (
                select
                    L2.*,
                    psr.L1_EC as L4_Code,
                    psr.PSR_Name as L3_Name
                from
                    L2
                    left join psr on L2.L3_code = psr.PSR_Shortcode
            ),
            L4 as (
                select
                    L3.*,
                    psr.L1_EC as L5_Code,
                    psr.PSR_Name as L4_Name
                from
                    L3
                    left join psr on L3.L4_code = psr.PSR_Shortcode
            ),
            L5 as (
                select
                    L4.*,
                    psr.L1_EC as L6_Code,
                    psr.PSR_Name as L5_Name
                from
                    L4
                    left join psr on L4.L5_code = psr.PSR_Shortcode
            ),
            L6 as (
                select
                    L5.*,
                    psr.L1_EC as L7_Code,
                    psr.PSR_Name as L6_Name
                from
                    L5
                    left join psr on L5.L6_code = psr.PSR_Shortcode
            ),
            L7 as (
                select
                    L6.*,
                    psr.L1_EC as L8_Code,
                    psr.PSR_Name as L7_Name
                from
                    L6
                    left join psr on L6.L7_code = psr.PSR_Shortcode
            ),
            L8 as (
                select
                    L7.*,
                    psr.L1_EC as L9_Code,
                    psr.PSR_Name as L8_Name
                from
                    L7
                    left join psr on L7.L8_code = psr.PSR_Shortcode
            ),
            psr_level as (
                select
                    L8.*,
                    psr.L1_EC as L10_Code,
                    psr.PSR_Name as L9_Name
                from
                    L8
                    left join psr on L8.L9_code = psr.PSR_Shortcode
            ),
            emp_tree as (
                select
                    psr_level.*,
                    Doctor_Code,
                    Location_Name,
                    DoctorHeadQuarter_Lnk.Location_Code,
                    DoctorHeadQuarter_Lnk.Company_DivisionCode as division_code1,
                    DoctorHeadQuarter_Lnk.HeadQuarterID as HQ1
                from
                    psr_level
                    left join DoctorHeadQuarter_Lnk on DoctorHeadQuarter_Lnk.Position_Code = psr_level.Position_Code
                    and DoctorHeadQuarter_Lnk.Company_DivisionCode = psr_level.Company_DivisionCode
                    left join CounterLocation_Hdr on DoctorHeadQuarter_Lnk.Location_Code = CounterLocation_Hdr.Location_Code
            ),
            counter_view as (
                select
                    emp_tree.*,
                    Cast(Registration_Date as varchar) as Registration_Date,
                    headquater_name as Headquater_Name,
                    Counter_LinkType,
                    Doctor_Name,
                    co.company_name as Company_Name,
                    co.Company_Shortcode,
                    co.Company_Shortname,
                    cd.division_name as Division_Name,
                    doc.City_code as City_Code,
                    city.city_name as City,
                    dist.District_Name as District_Name,
                    st.state_code,
                    st.state_name as State_Name,
                    CounterCompany_Lnk.CreatedDate as CCL_CreatedDate,
                    CounterCompany_Lnk.ModifiedDate as CCL_ModifiedDate,
                    doc.CreatedDate as DDH_CreatedDate,
                    doc.ModifiedDate AS DDH_ModifiedDate,
                    dist.Created_Date as DISTRICT_CreatedDate,
                    dist.modified_date AS DISTRICT_ModifiedDate,
                    co.Created_Date as COMPANY_CreatedDate,
                    co.ModifieDate as COMPANY_ModifiedDate,
                    cd.CreatedDate as Division_CreatedDate,
                    cd.ModifieDate AS Division_ModifiedDate,
                    HeadQuarter_Hdr.Created_Date as HQ_CreatedDate,
                    HeadQuarter_Hdr.Modified_date as HQ_ModifiedDate
                from
                    emp_tree
                    join CounterCompany_Lnk on CounterCompany_Lnk.counter_code = emp_tree.doctor_code
                    and CounterCompany_Lnk.Company_Code = emp_tree.Company_Code
                    and CounterCompany_Lnk.location_code = emp_tree.location_code
                    join DoctorDetails_Hdr as doc on doc.Doctor_Code = emp_tree.Doctor_Code
                    join CityMaster_Hdr as city on city.city_code = doc.city_code
                    join DistrictMaster_Hdr as dist on dist.District_code = city.District_code
                    join StateMaster_Hdr as st on st.state_code = dist.state_code
                    join company_hdr as co on co.company_code = emp_tree.company_code
                    join CompanyDivision_dtl cd on cd.Company_Divisioncode = emp_tree.Company_Divisioncode
                    left join HeadQuarter_Hdr on HeadQuarter_Hdr.headquater_id = emp_tree.HeadQuarterID
            ),
            hdr as (
                select
                    *
                from
                    OrderDetails_Hdr 
            ),
            order_hdr as (
                select
                    idt.Invoicedtl_Code,
                    dtl.Orderdtl_Code,
                    hdr.OrderHeader_Code,
                    Cast(order_prefix AS VARCHAR(20)) + RIGHT(
                        '0000' + CONVERT(VARCHAR(10), order_number),
                        4
                    ) AS Order_Number,
                    CAST(hdr.Order_Date as varchar) as Order_Date,
                    hdr.OrderPlaced_Date,
                    hdr.Doctor_code,
                    hdr.Stockist_Code,
                    hdr.Product_Count,
                    hdr.Ordered_By,
                    hdr.Drph_LinkType,
                    hdr.Pharmacist_Code,
                    dc.Doctor_Name as Pharmacy,
                    hdr.Order_Status,
                    hdr.Company_Code,
                    hdr.location_code,
                    hdr.Company_DivisionCode,
                    idt.order_item_code as order_Item_Code,
                    dtl.Item_Code,
                    dtl.Item_Description,
                    (
                        case
                            when hdr.Order_Status in (2, 3) then idt.Quantity
                            else dtl.Quantity
                        end
                    ) as Quantity,
                    dtl.Quantity as OrderDetailQuantity,
                    dtl.Price,
                    brand,
                    Price_Identifier,
                    DiscountOnPTS,
                    case
                        when dtl.price_identifier = 0 then Discountonpts
                        when dtl.price_identifier = 2 then IIF(
                            dtl.scheme_freeqty = 0,
                            0,
                            round(
                                (dtl.scheme_freeqty * 100) /(dtl.scheme_freeqty + dtl.scheme_qty),
                                0
                            )
                        )
                        when dtl.price_identifier = 1 then IIF(
                            dtl.PTR = 0,
                            0,
                            round((dtl.PTR - dtl.fixedprice) * 100 / dtl.PTR, 0)
                        )
                    end as discount,
                    dtl.PTS,
                    dtl.PTR,
                    dtl.FixedPrice,
                    dtl.Scheme_Qty,
                    dtl.Scheme_FreeQty,
                    orderstatus_desc as OrderStatus_Desc
                from
                    hdr
                    join OrderDetails_dtl as dtl on Hdr.OrderHeader_Code = dtl.OrderHeader_Code
                    left join InvoiceDetails_dtl as idt on dtl.OrderHeader_Code = idt.OrderHeader_Code
                    and dtl.Item_Code = idt.order_item_code
                    join OrderStatus_Hdr on OrderStatus_Hdr.orderstatus_code = hdr.Order_Status
                    join CompanyProduct_Hdr as product on product.item_code = dtl.item_code
                    left join DoctorDetails_Hdr as dc on dc.Doctor_Code = hdr.Pharmacist_Code
                    where 
                cast(idt.Created_Date AS DATE) >= cast(
                CONVERT(
                    datetime, 
                    CONVERT(
                    date, 
                    GETDATE() - DAY(0)
                    )
                ) AS DATE
                ) 
                or cast(hdr.CreatedDate AS DATE) >= cast(
                CONVERT(
                    datetime, 
                    CONVERT(
                    date, 
                    GETDATE() - DAY(0)
                    )
                ) AS DATE
                ) 
                or cast(hdr.ModifieDate AS DATE) >= cast(
                CONVERT(
                    datetime, 
                    CONVERT(
                    date, 
                    GETDATE() - DAY(0)
                    )
                ) AS DATE
                )
            )
            ,stockist as  
            (
                select sdh.Stockist_Code,sdh.Stockist_UniqueId, sdc.StockistName as Stockist_Name, sdh.Stockist_City_code as stockist_city_code, sdh.companycode, sdc.Medvol_Field_UserID
                ,sdc.Ph_Code, city.City_Name as Stockist_City , dist.District_code as stockist_district_code, dist.District_Name as stockist_district, st.state_code as stockist_state_code, st.state_name as stockist_state
                from StockistDetails_Hdr as sdh left join StockistDetails_Cfg sdc on sdh.Stockist_UniqueId = sdc.Stockist_UniqueId
                join CityMaster_Hdr as city on city.city_code = sdh.Stockist_City_code
                join DistrictMaster_Hdr as dist on dist.District_code = city.District_code
                join StateMaster_Hdr as st on st.state_code = dist.state_code     
            )
            select
                Invoicedtl_Code,
                Orderdtl_Code,
                cast(isnull(Invoicedtl_Code, 0) AS VARCHAR(20)) + '_' + cast(isnull(Orderdtl_Code, 0) AS VARCHAR(20)) as DeltaKey,
                order_Item_Code,
                Item_Code,
                order_hdr.Stockist_Code,
                stockist.Stockist_Name,
                Product_Count,
                Ordered_By,
                Drph_LinkType,
                Pharmacist_Code,
                Pharmacy,
                Order_Status,
                OrderDetailQuantity,
                Price_Identifier,
                DiscountOnPTS,
                PTS,
                FixedPrice,
                Scheme_Qty,
                Scheme_FreeQty,
                OrderHeader_Code,
                Order_Number,
                Order_Date,
                (
                    case
                        when OrderPlaced_Date is null then 0000 -00 -00
                        else OrderPlaced_Date
                    end
                ) as OrderPlaced_Date,
                OrderStatus_Desc,
                Item_Description,
                Brand,
                Quantity,
                Price,
                CAST(discount AS DECIMAL(7, 2)) as Discount,
                PTR,
                counter_view.L0_Code,
                counter_view.L0_Name,
                counter_view.L0_Designation,
                counter_view.Position_Code,
                counter_view.Company_Divisioncode,
                counter_view.L1_Code,
                counter_view.L1_Name,
                counter_view.Company_Code,
                counter_view.HeadQuarterID,
                counter_view.L0_PositionCode,
                counter_view.L2_Code,
                counter_view.L2_Name,
                counter_view.L3_Code,
                counter_view.L3_Name,
                counter_view.L4_Name,
                counter_view.L4_Code,
                counter_view.L5_Code,
                counter_view.L5_Name,
                counter_view.L6_Code,
                counter_view.L6_Name,
                counter_view.L7_Code,
                counter_view.L7_Name,
                counter_view.L8_Code,
                counter_view.L8_Name,
                counter_view.L9_Code,
                counter_view.L9_name,
                counter_view.Doctor_Code,
                counter_view.Location_Name,
                counter_view.Location_Code,
                counter_view.division_code1,
                counter_view.HQ1,
                counter_view.Registration_Date,
                counter_view.Headquater_Name,
                counter_view.Counter_LinkType,
                counter_view.Doctor_Name,
                counter_view.Company_Name,
                counter_view.Company_Shortcode,
                counter_view.Company_Shortname,
                counter_view.Division_Name,
                counter_view.City_Code,
                counter_view.City,
                District_Name,
                State_Code,
                State_Name,
                Medvol_Field_UserID,
                User_SSID as Medvol_field_ucode,
                User_Lnk_UniqueID,
                Ph_Code,
                stockist_city_code,
                Stockist_City, 
                stockist_district_code,
                stockist_district, 
                stockist_state_code,
                stockist_state,
                Cast(GETDATE() as date) as Mgtn_InsertDate,
                null as Mgtn_UpdateDate
            from
                counter_view
                join order_hdr on counter_view.Doctor_code = order_hdr.Doctor_Code
                and counter_view.Company_Divisioncode = order_hdr.Company_Divisioncode
                and counter_view.location_code = order_hdr.location_code
                left join stockist on stockist.Stockist_Code = order_hdr.Stockist_Code
                and stockist.companycode = order_hdr.company_code
                left join UserConsoleLoginDetails ucld on stockist.Medvol_Field_UserID = ucld.User_ID 
            where
                OrderPlaced_Date is not null
                and order_hdr.stockist_code not in ('1123232','900001','Dum4001')
        '''

        df = pd.read_sql_query(querySnglDyDlt, conn_sql)
        df['OrderPlaced_Date'] = pd.to_datetime(df['OrderPlaced_Date'],utc=True)
        #logger.info("OrderInvoice Migration completed on " + today_date)
        conn_sql.close() 
        PostgresConPref.deleteDeltaInvoiceDetailsPostgresTableChunk(df, "SuperDashboardMedvol")          
        PostgresConPref.save_to_postgres_insert(df,"SuperDashboardMedvol")  
        logger.info("SUPERDASH CONGO . Rows = "+ str(df.shape[0]) +" , DATE = " + today_date)      
    except Exception as e:
        logger.error(" ## SUPERDASH ERROR : {}".format(e)) 
        raise ValueError(message="OrderView Migration failed")
         
        



@flow
def InsertUpdateRFMFlow():
    CompleteSuperDashboardTask()


