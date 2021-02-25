
import os
import glob
import pandas as pd
from pandas import DataFrame 
import re
import numpy as np
import sqlite3
import pyodbc
from datetime import datetime


def connect_to_db():
	"""connect to SQL server 'SQL-HQ', db 'SCCJU'"""
	cre = (r"Driver={ODBC Driver 13 for SQL Server};"
	   r"Server=SQL-HQ;"
	   r"Database=SCCJU;"
	   r"Trusted_Connection=yes")
	cnxn = pyodbc.connect(cre)
	cs = cnxn.cursor() 
	return cs



def all_admits_queries(base_query, facility_list):
	"""create queries for admits in list of facilities"""	
	admits_queries = []
	for facil in facility_list:
		admit_query = base_query + f"""\nWHERE FldHousingName LIKE '%{facil}'
AND [fldDischargeDate/Max-out] IS NULL
ORDER BY fldEntrydateofhousing DESC"""
		admits_queries.append(admit_query)
	return admits_queries



def all_discharges_queries(base_query, facility_list):
	"""create queries for discharges in list of facilities"""
	disc_queries = []
	for facil in facility_list:
		disc_query = base_query + f"""\nWHERE FldHousingName LIKE '%{facil}'
ORDER BY [fldDischargeDate/Max-out] DESC"""
		disc_queries.append(disc_query)
	return disc_queries

def execute_query(cs,queries):
	"""execute queries and append results into a dataframe"""
	df = pd.DataFrame()
	for query in queries:
		res = cs.execute(query).fetchall()
		holder = DataFrame(res)
		df = df.append(holder)	
	df[0] = df[0].astype(str)
	df = df[0].str.split(pat=", ",expand=True)
	return df	

def reformat_df(df,type=""):
	"""reformat dataframe and reassign datetime data type to appropriate columns"""
	cols = ["UpdateStatus","FirstName","LastName","HousingName","EntryDate","DischargeDate"]
	total = pd.DataFrame(columns=cols)
	total["UpdateStatus"] = total["UpdateStatus"].astype("bool")
	total["DischargeDate"] = pd.to_datetime(total["DischargeDate"])
	
	total["FirstName"] = df.iloc[:,1]
	total["FirstName"] = total["FirstName"].str.slice(start=1,stop=-1)
	total["LastName"] = df.iloc[:,2]
	total["LastName"] = total["LastName"].str.slice(start=1,stop=-1)
	total["HousingName"] = df.iloc[:,4]
	total["HousingName"] = total["HousingName"].str.slice(start=1,stop=-1)
	total["EntryDate"] = df.iloc[:,5].map(str) + "-" + df.iloc[:,6].map(str) + "-" + df.iloc[:,7].map(str)
	total["EntryDate"] = total["EntryDate"].str.slice(start=18)
	total["EntryDate"] = pd.to_datetime(total["EntryDate"])

	
	if type=="admits":
		pass
	if type=="discharges":
		total["DischargeDate"] = df.iloc[:,10].map(str) + "-" + df.iloc[:,11].map(str) + "-" + df.iloc[:,12].map(str)
		total["DischargeDate"] = total["DischargeDate"].str.slice(start=18)
		total["DischargeDate"] = pd.to_datetime(total["DischargeDate"])
		total = total[~total["DischargeDate"].isnull()] 
	return total

def set_dir():
	"""set directory to current working directory"""
	os.chdir(os.getcwd())
	return os.getcwd()

def get_latest_sheet(wd):
	"""convert the latest excel sheet into a dataframe"""
	os.chdir(wd)
	files = glob.glob('*.xlsx')
	files = sorted(files, key=os.path.getmtime)
	latest_sheet = files[-1]
	print(f"taken from {latest_sheet}\n\n\n")
	sheet_df = pd.read_excel(latest_sheet,sheet_name="Individual Data")
	sheet_df.columns = ["Status","EntryDate","HousingName","SSN","FirstName","LastName","DOB","PriorHousing","PriorEntryDate","PriorDischargeDate","DischargeDate","DischargeReason","ResidenceDischargedTo"]
	bed_list_df = pd.read_excel(latest_sheet,sheet_name="Available County Beds")
	return sheet_df, bed_list_df

def split_bed_list(bed_list_df, facility_list):
	bed_list_df = bed_list_df["Community/Supportive Slot"]
	bed_list_df = bed_list_df.to_frame()
	bed_list_df["HousingName"] = bed_list_df["Community/Supportive Slot"].str.extract(r"(.+)\s\d+")
	bed_list_df["BedNumber"] = bed_list_df["Community/Supportive Slot"].str.extract(r"(\d+)")
	print(bed_list_df)

	bed_list_df = bed_list_df[bed_list_df["HousingName"].isin(facility_list)]
	
	print(bed_list_df)
	print(bed_list_df.dtypes)


def cross_check_admits(all_admits,sheet_df):
	all_admits = all_admits.assign(UpdateStatus = (all_admits.FirstName.isin(sheet_df.FirstName)) & (all_admits.LastName.isin(sheet_df.LastName)) & (all_admits.EntryDate.isin(sheet_df.EntryDate)))
	not_updated_df = all_admits.loc[all_admits["UpdateStatus"]==False]
	return not_updated_df

def cross_check_discharges(all_discharges,sheet_df):
	all_discharges = all_discharges.assign(UpdateStatus = (all_discharges.FirstName.isin(sheet_df.FirstName)) & (all_discharges.LastName.isin(sheet_df.LastName)) & (all_discharges.EntryDate.isin(sheet_df.EntryDate)) & (all_discharges.DischargeDate.isin(sheet_df.DischargeDate)))
	not_updated_df = all_discharges.loc[all_discharges["UpdateStatus"]==False]
	return not_updated_df




def main():
	"""main function to queue other functions"""
	###create cursor
	cs = connect_to_db()
	facility_list = ["Girard Recovery Center", "Horizon House - Old York DBT", "Gaudenzia RTFA", "Gaudenzia RTFA (BHJRS)", "New Vitae - South", "New Vitae - South (Non_ACLU)", "New Vitae - West I", "New Vitae - West II", "New Vitae - West I (Non-ACLU)", "New Vitae - West II (Non-ACLU)", "VOA - Roosevelt TBI", "VOA - Upsal TBI"]
	
	base_query = """SELECT [fldHousingEventID#]
	,clt.FirstName
	,clt.LastName
	,typ.[fldHousingLocationID#]
	,typ.FldHousingName
	,[fldEntrydateofhousing]
	,[fldDischargeDate/Max-out]
	,[CurrentEvent]
FROM [SCCJU].[dbo].[tblHousingEvents] as evnt
JOIN tblHousingType as typ ON evnt.fldHousingLocationID# = typ.fldHousingLocationID#
JOIN tblClient as clt ON evnt.fldClientID# = clt.ClientID#"""
	
	### export & reformat dataframe of all admits
	admits_queries = all_admits_queries(base_query, facility_list)
	all_admits_df = execute_query(cs, admits_queries)
	all_admits_df = reformat_df(all_admits_df, type="admits")	

	### export & reformat dataframe of al discharges
	disc_queries = all_discharges_queries(base_query, facility_list)
	all_discharges_df = execute_query(cs, disc_queries)
	all_discharges_df = reformat_df(all_discharges_df, type="discharges")

	### convert latest sheet to dataframe
	wd = set_dir()
	sheet_df, bed_list_df = get_latest_sheet(wd)

	not_updated_df = cross_check_admits(all_admits_df,sheet_df)
	not_updated_df = not_updated_df.append(cross_check_discharges(all_discharges_df,sheet_df))
	#print(not_updated_df)

	bed_list_df = split_bed_list(bed_list_df, facility_list)






if __name__=="__main__":
	main()



