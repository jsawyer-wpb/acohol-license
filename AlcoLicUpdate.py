


import arcpy,traceback


# connections
arcpy.env.workspace = r"Database Connections\SDE@Planning.sde"
arcpy.env.overwriteOutput = True
db_conn = r"Database Connections\SDE@Planning.sde"
##publish_conn = r"GIS Servers\arcgis on gis.wpb.org (publisher)"


#   data
ComPlus_AlcoLic = r"Database Connections\GISUSER@comprod.sde\COMPLUS.WPB_GIS_ALCOHOL_LICENSES"
Planning_AlcoLic = r"Planning.SDE.WPB_GIS_ALCOHOL_LICENSES"
Planning_AlcoLic_BU = r"Planning.SDE.WPB_GIS_ALCOHOL_LICENSES_bu"
sql = "SELECT PARCELS.[OBJECTID],[OWNPARCELID] AS PARCELS_PCN,[SRCREF],[OWNTYPE],[GISdata_GISADMIN_OwnerParcel_AR],[LASTUPDATE],[LASTEDITOR],[Shape],[PARCEL_ID] AS COMPLUS_PCN,[BUSINESS_ID],[LICENSE],[CATEGORY],[CATEGORY_DESC],[STAT],[ISSUE],[EXPIRATION],[BUS_ENTITY_ID],[BUS_NAME],[BUS_PROD],[SERVICE],[ADRS1],[BUS_PHONE],[BUS_EMAIL] FROM [Planning].[sde].[CODEENFORCEMENT_PARCELS] PARCELS,[Planning].[sde].[WPB_GIS_ALCOHOL_LICENSES] ALCOLIC WHERE PARCELS.OWNPARCELID = ALCOLIC.PARCEL_ID"
query_layer = "ALCOLICENCE_QL"
alco_licence_poly = "Alco_licence_poly"
alco_license ="AlcoholLicense_complus"
spatialref = arcpy.Describe(r"Database Connections\SDE@Planning.sde\Planning.SDE.LandUsePlanning").spatialReference.exportToString()

try:
#   update table from ComPLus
    arcpy.AcceptConnections(db_conn,True)
    arcpy.DisconnectUser(db_conn,'ALL')
    arcpy.AcceptConnections(db_conn,False)

    if arcpy.Exists(Planning_AlcoLic) and arcpy.Exists(Planning_AlcoLic_BU):
        print 'from 1'
        arcpy.Delete_management(Planning_AlcoLic_BU)
        arcpy.CopyRows_management(Planning_AlcoLic,Planning_AlcoLic_BU)
        arcpy.Delete_management(Planning_AlcoLic)
        arcpy.CopyRows_management(ComPlus_AlcoLic,Planning_AlcoLic)
    elif arcpy.Exists(Planning_AlcoLic) and not arcpy.Exists(Planning_AlcoLic_BU):
        print 'from 2'
        arcpy.CopyRows_management(Planning_AlcoLic,Planning_AlcoLic_BU)
        arcpy.Delete_management(Planning_AlcoLic)
        arcpy.CopyRows_management(ComPlus_AlcoLic,Planning_AlcoLic)
    elif not arcpy.Exists(Planning_AlcoLic) and arcpy.Exists(Planning_AlcoLic_BU):
        print 'from 3'
        arcpy.Delete_management(Planning_AlcoLic_BU)
        arcpy.CopyRows_management(ComPlus_AlcoLic,Planning_AlcoLic)
        arcpy.CopyRows_management(Planning_AlcoLic,Planning_AlcoLic_BU)
    elif not arcpy.Exists(Planning_AlcoLic) and not arcpy.Exists(Planning_AlcoLic_BU):
        print 'from 4'
        arcpy.CopyRows_management(ComPlus_AlcoLic,Planning_AlcoLic)
    else:
        print 'How did I get here?'


    #   create query layer
    print 'mql'
##    arcpy.MakeQueryLayer_management(input_database=db_conn, out_layer_name=query_layer, query=sql, oid_fields="OBJECTID", shape_type="POLYGON", srid="2881", spatial_reference="PROJCS['NAD_1983_HARN_StatePlane_Florida_East_FIPS_0901_Feet',GEOGCS['GCS_North_American_1983_HARN',DATUM['D_North_American_1983_HARN',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',656166.6666666665],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-81.0],PARAMETER['Scale_Factor',0.9999411764705882],PARAMETER['Latitude_Of_Origin',24.33333333333333],UNIT['Foot_US',0.3048006096012192]];-17791300 -41645400 3048.00609601219;-100000 10000;-100000 10000;3.28083333333333E-03;0.001;0.001;IsHighPrecision")
    arcpy.MakeQueryLayer_management(input_database=db_conn, out_layer_name=query_layer, query=sql, oid_fields="OBJECTID", shape_type="POLYGON", srid="2881", spatial_reference=spatialref)
    arcpy.management.CopyFeatures(query_layer, alco_licence_poly, None, None, None, None)

    print 'mql done'
    #   polygons to points
    print 'feature to point'
    arcpy.FeatureToPoint_management(alco_licence_poly,alco_license,"INSIDE")
    print 'feature to point done'

    arcpy.management.Delete(Planning_AlcoLic_BU)
    arcpy.management.Delete(alco_licence_poly)

    arcpy.AcceptConnections(db_conn,True)
except Exception as E:
    print "game over man"
    arcpy.AcceptConnections(db_conn,True)

    log = traceback.format_exc()
    print "{}\n{}\n{}".format(type(E).__name__, E.args, log)