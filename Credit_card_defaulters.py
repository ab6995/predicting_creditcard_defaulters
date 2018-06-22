#Load the file into a RDD
ccRaw = SpContext.textFile("/Users/ashishbansal/Downloads/credit-card-default-1000.csv")
ccRaw.take(5)

#Remove header row
dataLines = ccRaw.filter(lambda z: "EDUCATION" not in z)
dataLines.count()
dataLines.take(1000)


#Remove double quotes that are present in few records.
cleanedLines = filteredLines.map(lambda x: x.replace("\"", ""))
cleanedLines.count()
cleanedLines.cache()

#Cleanup data. Remove lines that are not "CSV"
filteredLines = dataLines.filter(lambda x : x.find("aaaaaa") < 0 )
filteredLines.count()