/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [fldHousingEventID#]
      ,clt.FirstName
	  ,clt.LastName
      ,typ.[fldHousingLocationID#]
	  ,typ.FldHousingName
      ,[fldEntrydateofhousing]
      ,[fldDischargeDate/Max-out]
      ,[CurrentEvent]
  FROM [SCCJU].[dbo].[tblHousingEvents] as evnt
  JOIN tblHousingType as typ ON evnt.fldHousingLocationID# = typ.fldHousingLocationID#
  JOIN tblClient as clt ON evnt.fldClientID# = clt.ClientID#
  WHERE FldHousingName LIKE '%Gaudenzia RTFA%'
  AND [fldDischargeDate/Max-out] IS NULL
  ORDER BY fldEntrydateofhousing DESC