import mysql.connector
import pandas as pd

"""The method of connecting to the MySQL server and database.Returns the database connection object.
 If the database does not exist, creates a database"""

def connection_to_db(config, db_name):

    print('connect to mysql server')
    cnx = mysql.connector.connect(
        host=config['conversion_csv']['HOST'],
        user=config['conversion_csv']['USER'],
        password=config['conversion_csv']['PASSWORD'],

    )

    cursor = cnx.cursor()
    cursor.execute("SHOW DATABASES")
    databases = [database[0] for database in cursor]


    if db_name not in databases:
        cursor.execute(f"CREATE DATABASE {db_name}")
        print(f"Create database {db_name}")
    else:
        print(f'The database {db_name} already exists')
    cursor.close()
    cnx.close()

    cnx = mysql.connector.connect(
        host=config['conversion_csv']['HOST'],
        user=config['conversion_csv']['USER'],
        password=config['conversion_csv']['PASSWORD'],
        database=db_name  # Add the database name here
    )
    print('connect to db')
    return cnx

"""Creating a table for four checklist files"""

def create_table_four(cursor, cnx, table_name):
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (Product_Group VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Product VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Subproduct VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Policy_Number VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Status VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Gender VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Age VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Joint_Life VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Start_Date VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Lapse_Date VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "End_Date VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Annual_Premium VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Single_Premium VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Premium_Indexation VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Changing_Premium VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Fonds_Value VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "PVPrem VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "PVFP VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "PVCost VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "PVSCR VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Statutory_Res VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "SII_Res VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Death_Benefit_Sum VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Sum_of_Salary VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Disability_Annuity VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Benefit_Indexation VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Waiting_Period VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Coverage_Style VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Benefit_Duration VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "NBM VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "CoC VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Combined_Ratio VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "UNIQUE KEY unique_policy_product (Product,Policy_Number, Subproduct))")
    cnx.commit()

"""Method for creating a table for the additionaldata file"""

def create_table_additionaldata(cursor, cnx):
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS AdditionalData (ClosingDate VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "GainLossCarryForward  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "IntangibleAssets VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "MVAssets VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "MVBonds VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "MVMortgage VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "TransitoryAssets VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Zillmer VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "2312TotalRIShare VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "2322TotalRIShare VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "2332TotalRIShare VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "23TotalStatReserves VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "24TotalUL VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "196101RWBDE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "196201RWBAT VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "230601HRESolvencyCover VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "098003PBEUR VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "210301Frisby VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "081001Participations VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Risk_InitialPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Retail_InitialPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "NonRetailInitialPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Risk_RecurringPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Retail_RecurringPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "NonRetail_RecurringPPE VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Marketing VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Underwriting VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Administration_systems  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "SalariesWages VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "SocialSecurityRenumeration VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "StaffCar VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "OtherPersonnelCosts VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "DirectorsFeesCharges VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "TravelExpenses VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "OfficeCost VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Depreciation VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "TelephoneCommunication VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "OtherOfficeCosts VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "BankCharges VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "ITExpenses VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "OtherOperatingExpenses VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Audit VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Lawyers VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "OtherProfessionals VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "DutiesManagementFees VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "DevelopmentBudget VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "AmortizationSetUpCosts VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Total VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Note_Home TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Note_S2Figures TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "Note_Portfolio TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,"
        "UNIQUE KEY unique_additionaldata (ClosingDate,GainLossCarryForward, IntangibleAssets))")
    cnx.commit()

"""A method for writing to the database from the additionaldata file. Returns the number of entries made"""

def insert_additionaldata(current_file_path, cursor):
    records_inserted = 0
    df = pd.read_csv(current_file_path, low_memory=False, delimiter=';', encoding='cp1252')
    df = df.where(pd.notnull(df), '')

    for index, row in df.iterrows():
        sql = "INSERT INTO AdditionalData (ClosingDate, GainLossCarryForward, IntangibleAssets, MVAssets, MVBonds, MVMortgage, TransitoryAssets, Zillmer," \
              "2312TotalRIShare, 2322TotalRIShare, 2332TotalRIShare, 23TotalStatReserves, 24TotalUL, 196101RWBDE, 196201RWBAT, " \
              "230601HRESolvencyCover, 098003PBEUR, 210301Frisby, 081001Participations, Risk_InitialPPE, Retail_InitialPPE, NonRetailInitialPPE, Risk_RecurringPPE, Retail_RecurringPPE, " \
              "NonRetail_RecurringPPE,Marketing, Underwriting,Administration_systems , SalariesWages, SocialSecurityRenumeration, StaffCar, OtherPersonnelCosts, " \
              "DirectorsFeesCharges,TravelExpenses,OfficeCost,Depreciation,TelephoneCommunication,OtherOfficeCosts,BankCharges,ITExpenses," \
              "OtherOperatingExpenses, Audit,Lawyers,OtherProfessionals, DutiesManagementFees,DevelopmentBudget,AmortizationSetUpCosts," \
              "Total,Note_Home,Note_S2Figures,Note_Portfolio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
              "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" \
                "ON DUPLICATE KEY UPDATE " \
                "MVAssets = VALUES(MVAssets), " \
                "MVBonds = VALUES(MVBonds), " \
                "MVMortgage = VALUES(MVMortgage), " \
                "TransitoryAssets = VALUES(TransitoryAssets), " \
                "Zillmer = VALUES(Zillmer), " \
                "2312TotalRIShare = VALUES(2312TotalRIShare), " \
                "2322TotalRIShare = VALUES(2322TotalRIShare), " \
                "2332TotalRIShare = VALUES(2332TotalRIShare), " \
                "23TotalStatReserves = VALUES(23TotalStatReserves), " \
                "24TotalUL = VALUES(24TotalUL), " \
                "196101RWBDE = VALUES(196101RWBDE), " \
                "196201RWBAT = VALUES(196201RWBAT), " \
                "230601HRESolvencyCover = VALUES(230601HRESolvencyCover), " \
                "098003PBEUR = VALUES(098003PBEUR), " \
                "210301Frisby = VALUES(210301Frisby), " \
                "081001Participations = VALUES(081001Participations), " \
                "Risk_InitialPPE = VALUES(Risk_InitialPPE), " \
                "Retail_InitialPPE = VALUES(Retail_InitialPPE), " \
                "NonRetailInitialPPE = VALUES(NonRetailInitialPPE), " \
                "Risk_RecurringPPE = VALUES(Risk_RecurringPPE), " \
                "Retail_RecurringPPE = VALUES(Retail_RecurringPPE), " \
                "DirectorsFeesCharges = VALUES(DirectorsFeesCharges), " \
                "TravelExpenses = VALUES(TravelExpenses), " \
                "OfficeCost = VALUES(OfficeCost), " \
                "Depreciation = VALUES(Depreciation), " \
                "TelephoneCommunication = VALUES(TelephoneCommunication), " \
                "OtherOfficeCosts = VALUES(OtherOfficeCosts), " \
                "BankCharges = VALUES(BankCharges), " \
                "ITExpenses = VALUES(ITExpenses), " \
                "OtherOperatingExpenses = VALUES(OtherOperatingExpenses), " \
                "Audit = VALUES(Audit), " \
                "Lawyers = VALUES(Lawyers), " \
                "OtherProfessionals = VALUES(OtherProfessionals), " \
                "DutiesManagementFees = VALUES(DutiesManagementFees), " \
                "DevelopmentBudget = VALUES(DevelopmentBudget), " \
                "AmortizationSetUpCosts = VALUES(AmortizationSetUpCosts), " \
                "Total = VALUES(Total), " \
                "Note_Home = VALUES(Note_Home), " \
                "Note_S2Figures = VALUES(Note_S2Figures), " \
                "Note_Portfolio = VALUES(Note_Portfolio)," \
                "NonRetail_RecurringPPE = VALUES(NonRetail_RecurringPPE), " \
                "Marketing = VALUES(Marketing), " \
                "Underwriting = VALUES(Underwriting), " \
                "Administration_systems = VALUES(Administration_systems), " \
                "SalariesWages = VALUES(SalariesWages), " \
                "SocialSecurityRenumeration = VALUES(SocialSecurityRenumeration), " \
                "StaffCar = VALUES(StaffCar), " \
                "OtherPersonnelCosts = VALUES(OtherPersonnelCosts) "
        val = tuple(row)
        cursor.execute(sql, val)
        records_inserted += 1 if cursor.rowcount > 0 else 0

    print(f"TOTAL RECORDS MADE TO DATABASE: {records_inserted}")
    return records_inserted

"""Method for writing to a database of 4 checklist files. Returns the number of completed entries"""

def insert_four(current_file_path, cursor, table_name):
    records_inserted = 0

    df = pd.read_csv(current_file_path, low_memory=False)
    df = df.where(pd.notnull(df), '')

    for index, row in df.iterrows():
        sql = f"INSERT INTO {table_name} (Product_Group, Product, Subproduct, Policy_Number, Status, Gender, Age, Joint_Life," \
          " Start_Date, Lapse_Date, End_Date, Annual_Premium, Single_Premium, Premium_Indexation, Changing_Premium, " \
          "Fonds_Value, PVPrem, PVFP, PVCost, PVSCR, Statutory_Res, SII_Res, Death_Benefit_Sum, Sum_of_Salary, " \
          "Disability_Annuity, Benefit_Indexation, Waiting_Period, Coverage_Style, Benefit_Duration, NBM, CoC, " \
          "Combined_Ratio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
          "%s,%s,%s,%s,%s,%s, %s)" \
          "ON DUPLICATE KEY UPDATE " \
               "Product_Group = VALUES(Product_Group), " \
               "Status = VALUES(Status), " \
               "Gender = VALUES(Gender), " \
               "Age = VALUES(Age), " \
               "Joint_Life = VALUES(Joint_Life), " \
               "Start_Date = VALUES(Start_Date), " \
               "Lapse_Date = VALUES(Lapse_Date),"\
               "End_Date = VALUES(End_Date), " \
               "Annual_Premium = VALUES(Annual_Premium), " \
               "Single_Premium = VALUES(Single_Premium), " \
               "Premium_Indexation = VALUES(Premium_Indexation), " \
               "Changing_Premium = VALUES(Changing_Premium), " \
               "Fonds_Value = VALUES(Fonds_Value), " \
               "PVPrem = VALUES(PVPrem), " \
               "PVFP = VALUES(PVFP), " \
               "PVCost = VALUES(PVCost), " \
               "PVSCR = VALUES(PVSCR), " \
               "Statutory_Res = VALUES(Statutory_Res), " \
               "SII_Res = VALUES(SII_Res), " \
               "Death_Benefit_Sum = VALUES(Death_Benefit_Sum), " \
               "Sum_of_Salary = VALUES(Sum_of_Salary), " \
               "Disability_Annuity = VALUES(Disability_Annuity), " \
               "Benefit_Indexation = VALUES(Benefit_Indexation), " \
               "Waiting_Period = VALUES(Waiting_Period), " \
               "Coverage_Style = VALUES(Coverage_Style), " \
               "Benefit_Duration = VALUES(Benefit_Duration), " \
               "NBM = VALUES(NBM), " \
               "CoC = VALUES(CoC), " \
               "Combined_Ratio = VALUES(Combined_Ratio)"

        val = tuple(row)
        cursor.execute(sql, val)
        records_inserted += 1 if cursor.rowcount > 0 else 0

    print(f"TOTAL RECORDS MADE TO DATABASE: {records_inserted}")
    return records_inserted

"""The method of disconnection_to_db the database connection"""

def disconnection_to_db(cnx, cursor):
    print('discconnection_to_db')
    cnx.commit()
    cursor.close()
    cnx.close()
