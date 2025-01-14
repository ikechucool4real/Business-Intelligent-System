import datetime as dt
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import requests
import json
import re
import shutil

# Specify Directories to be used
BaseDir = os.getcwd()  # Gets the current working directory
RawFiles = os.path.join(BaseDir, "Raw/")
Staging = os.path.join(BaseDir, "Staging/")
StarSchema = os.path.join(BaseDir, "StarSchema/")


#Create Directories
os.makedirs(BaseDir, exist_ok=True)
os.makedirs(RawFiles, exist_ok=True)
os.makedirs(Staging, exist_ok=True)
os.makedirs(StarSchema, exist_ok=True)


#Create new files in the staging directory if they dont exist
def CreateStagingFiles():
    with open(Staging+'Outputshort.txt', 'w'):
        pass 
    with open(Staging+'Outputlong.txt', 'w'):
        pass  


#Combining files 
def Combine(filename):
    #Seperate the raw files based on length
    type=filename[-3:len(filename)]
    if (type=="log"):
        with open(RawFiles+filename, 'r') as inFile:
            lines = inFile.readlines()
    
        with open(Staging+'Outputshort.txt', 'a') as OutputFileShort, \
            open(Staging+'Outputlong.txt', 'a') as OutputFileLong:

            for line in lines:
                if not line.startswith("#"):
                    Split=line.split(" ")
                
                    if len(Split)==14: 
                        OutputFileShort.write(line)
                    else:
                        if len(Split)==18:
                            OutputFileLong.write(line)
                        else:
                            pass
                            #print ("Fault: "+ filename + str(len(Split)))


# Extract files from the raw directory and combining to the staging directory
def ExtractFiles():
    arr=os.listdir(RawFiles)
    if not arr:
        print('List arr is empty')
    else:
        # create new staging files 
        CreateStagingFiles()

        # combine files to the staging directory
        for file in arr:
            Combine(file)


#Build fact table from short file
def BuildFactShort():
    with open(Staging+'Outputshort.txt', 'r') as InFile, \
            open(Staging+'OutFact1.txt', 'a') as OutFact1:

        Lines= InFile.readlines()
    
        for line in Lines:
            split=line.split(" ")
            FileInfo=split[4].replace(",","")
            UserAgent=split[9].replace(",","")
            Out = split[0]+","+split[1]+","+FileInfo+","+split[8]+","+UserAgent+","+""+","+split[10]+",0,"+split[13]
            OutFact1.write(Out)


#Build fact table from long file
def BuildFactLong():
    with open(Staging+'Outputlong.txt', 'r') as InFile, \
        open(Staging+'OutFact1.txt', 'a') as OutFact1:

        Lines= InFile.readlines()

        for line in Lines:
            split=line.split(" ")
            FileInfo=split[4].replace(",","")
            UserAgent=split[9].replace(",","")
            Referer=split[11].replace(",","")
            Out = ",".join([split[0],split[1],FileInfo,split[8],UserAgent,Referer,split[12],split[15],split[17]])
            OutFact1.write(Out)


#Build fact table in the staging directory 
def FactFile():
    #file header
    with open(Staging+'OutFact1.txt', 'w') as file:
        file.write("Date,Time,FileInfo,IP,UserAgent,UserReferrer,HTTPStatus,FileSize,ResponseTime\n")
    
    #Create the fact file
    BuildFactShort()
    BuildFactLong()


#Create Unique Date in Staging directory
def getDate():
    Date = set()  # Using a set to store unique lines
    with open(Staging+'OutFact1.txt', 'r') as inFile:   
        lines = inFile.readlines()

        for line in lines:
            split=line.split(",")
            header=split[0].strip()
            if header != "Date":
                Date.add(split[0])  
    
    with open(Staging+'UniqDate.txt', 'w') as outputFile:
        outputFile.write("Date\n")
        for uniqueline in Date:
           outputFile.write(uniqueline + '\n') 


#Create Unique Time in Staging directory
def getTime():
    Time = set()  # Using a set to store unique lines
    with open(Staging+'OutFact1.txt', 'r') as inFile:   
        lines = inFile.readlines()

        for line in lines:
            split=line.split(",")
            header=split[1].strip()
            if header != "Time":
                Time.add(split[1])  
    
    with open(Staging+'UniqTime.txt', 'w') as outputFile:
        outputFile.write("Time\n")
        for uniqueline in Time:
            outputFile.write(uniqueline + '\n')  


#Create Unique FileInfo in staging directory
def getFileInfo():
    FileInfo = set()
    with open(Staging+'OutFact1.txt', 'r') as inFile:
        lines = inFile.readlines()

        for line in lines:
            split=line.split(",")
            header=split[2].strip()
            if header != "FileInfo":
                FileInfo.add(split[2])  
    
    with open(Staging+'UniqFileInfo.txt', 'w') as outputFile:
        outputFile.write("FileInfo\n")
        for uniqueline in FileInfo:
           outputFile.write(uniqueline + '\n')  


#Create FileInfo in staging directory
def getFileInfoDim():
    with open(Staging+'UniqFileInfo.txt', 'r') as inFile, \
        open(Staging+'DimFileInfo.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("File,Name,Type\n")

        for i, line in enumerate(lines[1:]):
            line=line.strip()   

            try:
                if not line:  # Skip empty lines
                    continue

                if len(line)>3 and line[-3] == ".":
                    name = line[:-3]
                    type = line[-3:]
                    out="{},{},{}".format(line,name,type)

                    # Add newline character to all but the last line
                    if i < len(lines) - 2:
                        out += "\n"
                    outputFile.write(out)

                elif len(line)>4 and line[-4] == ".":
                    name = line[:-4]
                    type = line[-4:]
                    out="{},{},{}".format(line,name,type)

                    # Add newline character to all but the last line
                    if i < len(lines) - 2:
                        out += "\n"
                    outputFile.write(out)

                elif len(line)>5 and line[-5] == ".":
                    name = line[:-5]
                    type = line[-5:]
                    out="{},{},{}".format(line,name,type)

                    # Add newline character to all but the last line
                    if i < len(lines) - 2:
                        out += "\n"
                    outputFile.write(out)

                else:
                    name = line
                    type = ""

                    if i == len(lines) - 2:
                        type = "/"

                    out="{},{},{}".format(line,name,type)

                    # Add newline character to all but the last line
                    if i < len(lines) - 2:
                        out += "\n"
                    outputFile.write(out)

            except Exception as e:
                print("Error with line:", e)

# Create Unique IP in staging directory
def getIPs():
    IPSet = set()  # Using a set to store unique lines
    with open(Staging+'OutFact1.txt', 'r') as inFile:
        lines = inFile.readlines()

        for line in lines:
            split=line.split(",")
            header=split[3].strip()
            if header != "IP":
                IPSet.add(split[3])  
    
    with open(Staging+'UniqIP.txt', 'w') as outputFile:
        outputFile.write("IP\n")
        for uniqueline in IPSet:
           outputFile.write(uniqueline + '\n')


#Create Location in Staging directory
def getLocation():
    with open(Staging+'UniqIP.txt', 'r') as inFile, \
        open(Staging+'DimLocation.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("{},{},{},{},{},{},{}\n".format("IP","country_code","country_name","city","state","latitude","longitude"))

        for i, line in enumerate(lines[1:]):
            line = str(line).strip()

            # Skip empty lines
            if not line:
                continue

            # URL to send the request to
            request_url = 'https://geolocation-db.com/jsonp/' + line      

            # Send request and decode the result
            try:
                response = requests.get(request_url)
                if response.status_code == 200:
                    result = response.content.decode()

                    # Clean the returned string so it just contains the dictionary data for the IP address
                    result = result.split("(")[1].strip(")")
                    # Convert this data into a dictionary
                    result  = json.loads(result)

                    IPv4 = result.get("IPv4", " 0 ")
                    country_code = result.get("country_code", "N/A")
                    country_name = result.get("country_name", "N/A")
                    city = result.get("city", "N/A")
                    state = result.get("state", "N/A")
                    latitude = result.get("latitude", " 0 ")
                    longitude= result.get("longitude", " 0 ")


                    out = "{},{},{},{},{},{},{}".format(IPv4,country_code,country_name,city,state,latitude,longitude)
                    
                    # Add newline character to all but the last line
                    if i < len(lines) - 2:
                        out += "\n"

                    outputFile.write(out)
                else:
                    print("Error: Request failed with status code", response.status_code)

            except Exception as e:
                print ("Error: ", str(e) )     


# Create Unique FileInfo and IP in staging directory
def getFileInfoIPs():
    FileInfoIPSet = set()  # Using a set to store unique lines
    with open(Staging+'OutFact1.txt', 'r') as inFile:
        lines = inFile.readlines()[1:]  #skip header line

        for line in lines:
            split=line.split(",")
            IP=split[3].strip()
            FileInfo=split[2].strip()
            if FileInfo and IP:
                FileInfoIPSet.add(FileInfo + "," + IP)  
    
    with open(Staging+'UniqFileInfoIPs.txt', 'w') as outputFile:
        outputFile.write("FileInfo,IPs\n")
        for uniqueline in FileInfoIPSet:
           outputFile.write(uniqueline + '\n')


# Create Unique OS and Browser in staging directory
def getOSBrowser():
    OSBrowser = set()  # Using a set to store unique lines
    with open(Staging+'OutFact1.txt', 'r') as inFile:
        lines = inFile.readlines()[1:]  #skip header line

        for line in lines:
            split=line.split(",")
            splitline=split[4].strip()
            if splitline not in OSBrowser:
                OSBrowser.add(splitline)  
    
    with open(Staging+'UniqOSBrowser.txt', 'w') as outputFile:
        outputFile.write("UserAgent\n")
        for uniqueline in OSBrowser:
           outputFile.write(uniqueline + '\n')


# Create Referrer in staging directory
def getReferrer():
    Referrer = set()  # Using a set to store unique lines
    with open(Staging+'OutFact1.txt', 'r') as inFile:
        lines = inFile.readlines()[1:]  #skip header line

        for line in lines:
            split=line.split(",")
            splitline=split[5].strip()
            if splitline not in Referrer:
                Referrer.add(splitline)  
    
    with open(Staging+'UniqReferrer.txt', 'w') as outputFile:
        outputFile.write("UserReferrer\n")
        for uniqueline in Referrer:
           outputFile.write(uniqueline + '\n')


# Create Unique HTTPStatus in staging directory
def getHTTPStatus():
    HTTPStatus = set()  # Using a set to store unique lines
    with open(Staging+'OutFact1.txt', 'r') as inFile:
        lines = inFile.readlines()[1:]  #skip header line

        for line in lines:
            split=line.split(",")
            Code=split[6].strip()
            if Code not in HTTPStatus:
                HTTPStatus.add(Code)  
    
    with open(Staging+'UniqHTTPStatus.txt', 'w') as outputFile:
        outputFile.write("HTTPStatus\n")
        for uniqueline in HTTPStatus:
           outputFile.write(uniqueline + '\n')


# Create Unique file size in staging directory
def getfilesize():
    filesize = set()  # Using a set to store unique lines
    with open(Staging+'OutFact1.txt', 'r') as inFile:
        lines = inFile.readlines()[1:]  #skip header line

        for line in lines:
            split=line.split(",")
            size=split[7].strip()
            if size not in filesize:
                filesize.add(size)  
    
    with open(Staging+'UniqFileSize.txt', 'w') as outputFile:
        outputFile.write("FileSize\n")
        for uniqueline in filesize:
           outputFile.write(uniqueline + '\n')


# Create Unique Response time in staging directory
def getResponseTime():
    responseTime = set()  # Using a set to store unique lines
    with open(Staging+'OutFact1.txt', 'r') as inFile:
        lines = inFile.readlines()[1:]  #skip header line

        for line in lines:
            split=line.split(",")
            time=split[8].strip()
            if time not in responseTime:
                responseTime.add(time)  
    
    with open(Staging+'UniqResponseTime.txt', 'w') as outputFile:
        outputFile.write("ResponseTime\n")
        for uniqueline in responseTime:
           outputFile.write(uniqueline + '\n')


#Create Date Dimension in Schema directory
def DimDate():
    with open(Staging+'UniqDate.txt', 'r') as inFile, \
        open(StarSchema+'DimDate.txt', 'w') as outputFile:
        
        lines = inFile.readlines()
        outputFile.write("Date,Year,Quarter,Month,Day,DayofWeek\n")

        for i, line in enumerate(lines[1:]):
            line=line.strip()

            try:
                if not line:  # Skip empty lines
                    continue
        
                date=datetime.strptime(line,"%Y-%m-%d").date()
                weekday=date.strftime("%A")
                out="{},{},{},{},{},{}".format(line,date.year,(date.month - 1)//3 + 1,date.month,date.day,weekday)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)
            except Exception as e:
                print("Error with Date:", e)


#Create Time Dimension in Schema directory
def DimTime():
    with open(Staging+'UniqTime.txt', 'r') as inFile, \
        open(StarSchema+'DimTime.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("Time,Seconds,Minute,Hour\n")

        for i, line in enumerate(lines[1:]):
            line=line.strip()

            try:
                if not line:  # Skip empty lines
                    continue

                time=datetime.strptime(line, "%H:%M:%S")
                out="{},{},{},{}".format(line,time.second,time.minute,time.hour)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)
            except Exception as e:
                print("Error with Time:", e)


#Create FileInfo Dimension in Schema directory
def DimFileInfo():
    with open(Staging+'DimFileInfo.txt', 'r') as inFile, \
        open(StarSchema+'DimFileName.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("File,Name,Type\n")

        for i, line in enumerate(lines[1:]):
            line=line.strip() 
            splitline = line.split(",")
            file = splitline[0]
            type = splitline[2]  

            try:
                if not line:  # Skip empty lines
                    continue

                if "/darwin/image" in splitline[1].lower():
                    name = "/Darwin/Image"
                elif "/php" in splitline[1].lower():
                    name = "/phpMyAdmin"
                else:
                    name = splitline[1]

                out = "{},{},{}".format(file,name,type)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"
                outputFile.write(out)
                
            except Exception as e:
                print("Error with line:", e)
                                       

#Create Visit Type Dimension in Schema directory
def DimVisit():
    with open(Staging+'UniqFileInfoIPs.txt', 'r') as inFile, \
        open(StarSchema+'DimVisit.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("FileInfo,IP,Type\n")

        # Dictionary to store whether an IP has requested "robots.txt"
        crawlerIP = {}

        # Check if "robots.txt" is present in the FileInfo column and store its IP
        for i, line in enumerate(lines[1:]):
            line=line.strip().split(",") # Split the line into columns

            try:
                if not line:  # Skip empty lines
                    continue

                FileInfo = line[0].strip()
                IP = line[1].strip()

                if "robots.txt" in FileInfo: 
                    crawlerIP[IP] = True

            except Exception as e:
                print("Error with line:", e)

        #Label IP as either Real or Crawler
        for i, line in enumerate(lines[1:]):
            line=line.strip().split(",") # Split the line into columns

            try:
                if not line:  # Skip empty lines
                    continue
        
                FileInfo = line[0].strip()
                IP = line[1].strip()

                # Check if IP has been captured as Crawler
                if IP in crawlerIP: 
                    Type = "Crawler"
                else:
                    Type = "Real"  

                out = "{},{},{}".format(FileInfo,IP,Type)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)
            
            except Exception as e:
                print("Error with line:", e)



#Create GeoLocation Dimension in Schema directory
def DimGeoLocation():
    with open(Staging+'DimLocation.txt', 'r') as inFile, \
        open(StarSchema+'DimGeoLocation.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("{},{},{},{},{},{},{}\n".format("IP","country_code","country_name","city","state","latitude","longitude"))

        for i, line in enumerate(lines[1:]):
            split = line.split(",")
            IP = split[0].strip()
            country_code = split[1].strip()
            country_name = split[2].strip()
            city = split[3].strip()
            state = split[4].strip()
            latitude = split[5].strip()
            longitude = split[6].strip()

            try:
                if "IP Not found" in IP:
                    continue
                
                out = "{},{},{},{},{},{},{}".format(IP,country_code,country_name,city,state,latitude,longitude)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)
            except Exception as e:
                print ("Error: ", str(e) )  


#Create OS Dimension in Schema directory
def DimOS():
    with open(Staging+'UniqOSBrowser.txt', 'r') as inFile, \
        open(StarSchema+'DimOS.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("UserAgent,0S\n")

        for i, line in enumerate(lines[1:]):
            UserAgent = line.strip()
            try:
                if "Windows" in UserAgent:
                    OS = "Windows"
                elif "Macintosh" in UserAgent:
                    OS = "Macintosh"
                elif "bot" in UserAgent:
                    OS = "known robots"
                else:
                    OS = "Other"
                
                out = "{},{}".format(UserAgent,OS)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)
            except Exception as e:
                print ("Error: ", str(e) )  


#Create Browser Dimension in Schema directory
def DimBrowser():
    with open(Staging+'UniqOSBrowser.txt', 'r') as inFile, \
        open(StarSchema+'DimBrowser.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("UserAgent,Browser\n")

        for i, line in enumerate(lines[1:]):
            UserAgent = line.strip()
            try:
                if not line:  # Skip empty lines
                    continue
                if "MSIE" in UserAgent:
                    browser = "MSIE"
                elif "Netscape" in UserAgent:
                    browser = "Netscape"
                elif "Firefox" in UserAgent:
                    browser = "Firefox"
                elif "msnbot" in UserAgent:
                    browser = "msnbot"
                elif "panscient.com" in UserAgent:
                    browser = "panscient.com"
                elif "Baiduspider" in UserAgent:
                    browser = "Baiduspider"
                elif "Yandex" in UserAgent:
                    browser = "Yandex"
                elif "Safari" in UserAgent:
                    browser = "Safari"
                elif "Sogou web spider" in UserAgent:
                    browser = "Sogou web spider"
                else:
                    browser = "Other"
                
                out = "{},{}".format(UserAgent,browser)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)
            except Exception as e:
                print ("Error: ", str(e) )  


#Create Referrer Dimension in Schema directory
def DimReferrer():
    with open(Staging+'UniqReferrer.txt', 'r') as inFile, \
        open(StarSchema+'DimReferrer.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("UserReferrer,Referrer\n")

        for i, line in enumerate(lines[1:]):
            stripedline = line.strip()

            try:    
                if not stripedline:  # Skip empty lines
                    Referrer = "blank"

                elif "-" in stripedline and len(stripedline) == 1:
                    Referrer = "blank"

                else:
                    splitline = re.split('[?;%]', stripedline)
                    Referrer = splitline[0].strip() 

                out = "{},{}".format(stripedline,Referrer)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)
            except Exception as e:
                print ("Error: ", str(e) )  


#Create HTTPStatus Dimension in Schema directory
def DimHTTPStatus():
    with open(Staging+'UniqHTTPStatus.txt', 'r') as inFile, \
        open(StarSchema+'DimHTTPStatus.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("HTTPStatus,Description,Type\n")

        for i, line in enumerate(lines[1:]):
            stripedline = line.strip()
            try:
                if not line:  # Skip empty lines
                    continue

                statuscode = int(stripedline)
                if 200 <= statuscode < 300:
                    Type = "Successful"
                    if statuscode == 200:
                        Description = "Ok"
                    elif statuscode == 206:
                        Description = "Partial Content"
                    else:
                        Description = "Other Successful"
                elif 300 <= statuscode < 400:
                    Type = "Redirection"
                    if statuscode == 302:
                        Description = "Found (or Moved Temporarily)"
                    elif statuscode == 304:
                        Description = "Not Modified"
                    else:
                        Description = "Other Redirection"
                elif 400 <= statuscode < 500:
                    Type = "Client Error"
                    if statuscode == 403:
                        Description = "Forbidden"
                    elif statuscode == 404:
                        Description = "Not Found"
                    elif statuscode == 405:
                        Description = "Method Not Allowed"
                    elif statuscode == 406:
                        Description = "Not Acceptable"
                    else:
                        Description = "Other Client Error"
                elif 500 <= statuscode < 600:
                    Type = "Server Error"
                    if statuscode == 500:
                        Description = "Internal Server Error"
                    else:
                        Description = "Other Server Error"
                else:
                    Description = "Unknown"
                    Type = "Unknown"

                out = "{},{},{}".format(statuscode,Description,Type)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)

            except Exception as e:
                print("Error:", e)


#Create file size Dimension in Schema directory 
def DimfileSize():
    with open(Staging+'UniqFileSize.txt', 'r') as inFile, \
        open(StarSchema+'DimFileSize.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("FileSize,SizeBucket\n")

        for i, line in enumerate(lines[1:]):
            stripedline = line.strip()

            try:
                if not stripedline:  # Skip empty lines
                    continue
            
                size = int(stripedline)
                if size <= 0:
                    SizeBucket = "0"
                elif 0 < size <= 100:
                    SizeBucket = "0B - 100B"
                elif 100 < size <= 1024:
                    SizeBucket = "100B - 1KB"
                elif 1024 < size <= 10240:
                    SizeBucket = "1KB - 10KB"
                elif 10240 < size <= 102400:
                    SizeBucket = "10KB - 100KB"
                elif 102400 < size <= 1048576:
                    SizeBucket = "100KB - 1MB"
                else:
                    SizeBucket = "Above 1MB"

                out = "{},{}".format(size,SizeBucket)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)

            except Exception as e:
                print("Error:", e)


#Create response time Dimension in Schema directory
def DimResponseTime():
    with open(Staging+'UniqResponseTime.txt', 'r') as inFile, \
        open(StarSchema+'DimResponseTime.txt', 'w') as outputFile:

        lines = inFile.readlines()
        outputFile.write("ResponseTime,TimeBucket\n")

        for i, line in enumerate(lines[1:]):
            stripedline = line.strip()

            try:
                if not stripedline:  # Skip empty lines
                    continue
            
                Time = int(stripedline)
                if Time <= 0:
                    TimeBucket = "0"
                elif 0 < (Time /1000) <= 0.01:
                    TimeBucket = "0 to 0.01"
                elif 0.01 < (Time /1000) <= 0.02:
                    TimeBucket = "0.01 to 0.02"
                elif 0.02 < (Time /1000) <= 0.05:
                    TimeBucket = "0.02 to 0.05"
                elif 0.05 < (Time /1000) <= 0.1:
                    TimeBucket = "0.05 to 0.1"
                elif 0.1 < (Time /1000) <= 0.2:
                    TimeBucket = "0.1 to 0.2"
                elif 0.2 < (Time /1000) <= 0.5:
                    TimeBucket = "0.2 to 0.5"
                elif 0.5 < (Time /1000) <= 1:
                    TimeBucket = "0.5 to 1"
                elif 1 < (Time /1000) <= 2:
                    TimeBucket = "1 to 2"
                elif 2 < (Time /1000) <= 5:
                    TimeBucket = "2 to 5"
                else:
                    TimeBucket = "Above 5"

                out = "{},{}".format(stripedline,TimeBucket)

                # Add newline character to all but the last line
                if i < len(lines) - 2:
                    out += "\n"

                outputFile.write(out)

            except Exception as e:
                print("Error:", e)


#Create fact table in Schema directory
def FactTable():
    # Copy the fact file to the starSchema directory from the Staging directory
    shutil.copy(Staging+"OutFact1.txt",StarSchema )


dag = DAG(                                                     
   dag_id="process_log_files",                          
   schedule_interval="@daily",                                     
   start_date=dt.datetime(2024, 4, 4), 
   catchup=False,
)

Extract_Files = PythonOperator(
    task_id='ExtractFiles',
    python_callable=ExtractFiles,
    dag=dag,
)

Fact_File = PythonOperator(
    task_id='FactFile',
    python_callable=FactFile,
    dag=dag,
)

get_Date = PythonOperator(
    task_id='getDate',
    python_callable=getDate,
    dag=dag,
)

get_Time = PythonOperator(
    task_id='getTime',
    python_callable=getTime,
    dag=dag,
)

get_File_Info = PythonOperator(
    task_id='getFileInfo',
    python_callable=getFileInfo,
    dag=dag,
)

get_File_Info_Dim = PythonOperator(
    task_id='getFileInfoDim',
    python_callable=getFileInfoDim,
    dag=dag,
)

get_IPs = PythonOperator(
    task_id='getIPs',
    python_callable=getIPs,
    dag=dag,
)


get_Location = PythonOperator(
    task_id='getLocation',
    python_callable=getLocation,
    dag=dag,
)

get_File_Info_IPs = PythonOperator(
    task_id='getFileInfoIPs',
    python_callable=getFileInfoIPs,
    dag=dag,
)

get_OS_Browser = PythonOperator(
    task_id='getOSBrowser',
    python_callable=getOSBrowser,
    dag=dag,
)

get_Referrer = PythonOperator(
    task_id='getReferrer',
    python_callable=getReferrer,
    dag=dag,
)

get_HTTP_Status = PythonOperator(
    task_id='getHTTPStatus',
    python_callable=getHTTPStatus,
    dag=dag,
)

get_file_size = PythonOperator(
    task_id='getfilesize',
    python_callable=getfilesize,
    dag=dag,
)

get_Response_Time = PythonOperator(
    task_id='getResponseTime',
    python_callable=getResponseTime,
    dag=dag,
)

Dim_Date = PythonOperator(
    task_id='DimDate',
    python_callable=DimDate,
    dag=dag,
)

Dim_Time = PythonOperator(
    task_id='DimTime',
    python_callable=DimTime,
    dag=dag,
)

Dim_File_Info = PythonOperator(
    task_id='DimFileInfo',
    python_callable=DimFileInfo,
    dag=dag,
)

Dim_Geo_Location = PythonOperator(
    task_id='DimGeoLocation',
    python_callable=DimGeoLocation,
    dag=dag,
)

Dim_Visit = PythonOperator(
    task_id='DimVisit',
    python_callable=DimVisit,
    dag=dag,
)

Dim_OS = PythonOperator(
    task_id='DimOS',
    python_callable=DimOS,
    dag=dag,
)

Dim_Browser = PythonOperator(
    task_id='DimBrowser',
    python_callable=DimBrowser,
    dag=dag,
)

Dim_Referrer = PythonOperator(
    task_id='DimReferrer',
    python_callable=DimReferrer,
    dag=dag,
)

Dim_HTTP_Status = PythonOperator(
    task_id='DimHTTPStatus',
    python_callable=DimHTTPStatus,
    dag=dag,
)

Dim_file_Size = PythonOperator(
    task_id='DimfileSize',
    python_callable=DimfileSize,
    dag=dag,
)

Dim_Response_Time = PythonOperator(
    task_id='DimResponseTime',
    python_callable=DimResponseTime,
    dag=dag,
)

Fact_Table = PythonOperator(
    task_id='FactTable',
    python_callable=FactTable,
    dag=dag,
)

Extract_Files >> Fact_File
Fact_File >> [get_Date, get_Time, get_File_Info, get_File_Info_Dim, get_IPs, get_File_Info_IPs, get_OS_Browser, get_Referrer, get_HTTP_Status, get_file_size, get_Response_Time, Fact_Table]
get_Date >> Dim_Date
get_Time >> Dim_Time
get_File_Info >> get_File_Info_Dim
get_File_Info_Dim >> Dim_File_Info
get_File_Info_IPs >> Dim_Visit
get_IPs >> get_Location
get_Location >> Dim_Geo_Location
get_OS_Browser >> [Dim_OS, Dim_Browser]
get_Referrer >> Dim_Referrer
get_HTTP_Status >> Dim_HTTP_Status
get_file_size >> Dim_file_Size
get_Response_Time >> Dim_Response_Time