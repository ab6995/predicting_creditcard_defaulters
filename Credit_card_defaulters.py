#Load the file into a RDD
ccRaw = SpContext.textFile("/Users/ashishbansal/Downloads/credit-card-default-1000.csv")
ccRaw.take(5)

#Remove header row
dataLines = ccRaw.filter(lambda z: "EDUCATION" not in z)
dataLines.count()
dataLines.take(1000)


#Cleanup data. Remove lines that are not "CSV"
filteredLines = dataLines.filter(lambda x : x.find("aaaaaa") < 0 )
filteredLines.count()

#Remove double quotes that are present in few records.
cleanedLines = filteredLines.map(lambda x: x.replace("\"", ""))
cleanedLines.count()
cleanedLines.cache()

#Convert into SQL Dataframe.
from pyspark.sql import Row

def convertToRow(instr) :
    attributeList = instr.split(",")
 
    # rounding of age to range of 10s.    
    ageRound = round(float(attributeList[5]) / 10.0) * 10
    
    #Normalize sex to only 1 and 2.
    sex = attributeList[2]
    if sex =="M":
        sex=1
    elif sex == "F":
        sex=2
    
    #average billed Amount.
    avgBillAmt = (float(attributeList[12]) +  \
                    float(attributeList[13]) + \
                    float(attributeList[15]) + \
                    float(attributeList[16]) + \
                    float(attributeList[16]) + \
                    float(attributeList[17]) ) / 6.0
                    
    #average pay amount
    avgPayAmt = (float(attributeList[18]) +  \
                    float(attributeList[19]) + \
                    float(attributeList[20]) + \
                    float(attributeList[21]) + \
                    float(attributeList[22]) + \
                    float(attributeList[23]) ) / 6.0
                    
    #Find average pay duration. 
    #Make sure numbers are rounded and negative values are eliminated
    avgPayDuration = round((abs(float(attributeList[6])) + \
                        abs(float(attributeList[7])) + \
                        abs(float(attributeList[8])) +\
                        abs(float(attributeList[9])) +\
                        abs(float(attributeList[10])) +\
                        abs(float(attributeList[11]))) / 6)
    
    #Average percentage paid. add this as an additional field to see
    #if this field has any predictive capabilities. This is 
    #additional creative work that you do to see possibilities.                    
    perPay = round((avgPayAmt/(avgBillAmt+1) * 100) / 25) * 25
                    
    values = Row (  CUSTID = attributeList[0], \
                    LIMIT_BAL = float(attributeList[1]), \
                    SEX = float(sex),\
                    EDUCATION = float(attributeList[3]),\
                    MARRIAGE = float(attributeList[4]),\
                    AGE = float(ageRound), \
                    AVG_PAY_DUR = float(avgPayDuration),\
                    AVG_BILL_AMT = abs(float(avgBillAmt)), \
                    AVG_PAY_AMT = float(avgPayAmt), \
                    PER_PAID= abs(float(perPay)), \
                    DEFAULTED = float(attributeList[24]) 
                    )

    return values

#Cleanedup RDD    
ccRows = cleanedLines.map(convertToRow)
ccRows.take(60)
#Create a data frame.
ccDf = SpSession.createDataFrame(ccRows)
ccDf.cache()
ccDf.show(10)

#Enhance Data
import pandas as pd

#Add SEXNAME to the data using SQL Joins.
genderDict = [{"SEX" : 1.0, "SEX_NAME" : "Male"}, \
                {"SEX" : 2.0, "SEX_NAME" : "Female"}]                
genderDf = SpSession.createDataFrame(pd.DataFrame(genderDict, \
            columns=['SEX', 'SEX_NAME']))
genderDf.collect()
ccDf1 = ccDf.join( genderDf, ccDf.SEX== genderDf.SEX ).drop(genderDf.SEX)
ccDf1.take(5)

#Add ED_STR to the data with SQL joins.
eduDict = [{"EDUCATION" : 1.0, "ED_STR" : "Graduate"}, \
                {"EDUCATION" : 2.0, "ED_STR" : "University"}, \
                {"EDUCATION" : 3.0, "ED_STR" : "High School" }, \
                {"EDUCATION" : 4.0, "ED_STR" : "Others"}]                
eduDf = SpSession.createDataFrame(pd.DataFrame(eduDict, \
            columns=['EDUCATION', 'ED_STR']))
eduDf.collect()
ccDf2 = ccDf1.join( eduDf, ccDf1.EDUCATION== eduDf.EDUCATION ).drop(eduDf.EDUCATION)
ccDf2.take(5)

#Add MARR_DESC to the data. Required for PR#03
marrDict = [{"MARRIAGE" : 1.0, "MARR_DESC" : "Single"}, \
                {"MARRIAGE" : 2.0, "MARR_DESC" : "Married"}, \
                {"MARRIAGE" : 3.0, "MARR_DESC" : "Others"}]                
marrDf = SpSession.createDataFrame(pd.DataFrame(marrDict, \
            columns=['MARRIAGE', 'MARR_DESC']))
marrDf.collect()
ccFinalDf = ccDf2.join( marrDf, ccDf2.MARRIAGE== marrDf.MARRIAGE ).drop(marrDf.MARRIAGE)
ccFinalDf.cache()
ccFinalDf.take(5)