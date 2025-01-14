DROP DATABASE IF EXISTS ETL;
CREATE DATABASE ETL;
CHARACTER SET latin1
COLLATE latin1_general_ci;
USE ETL;

CREATE TABLE IF NOT EXISTS DimBrowser (
    UserAgent VARCHAR(400),
    Browser VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS DimDate (
    Date DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT,
    DayofWeek VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS DimFileInfo (
    File VARCHAR(120) DEFAULT '',
    Name VARCHAR(100) DEFAULT '',
    Type VARCHAR(10) DEFAULT ''
);

CREATE TABLE IF NOT EXISTS DimFileSize (
    FileSize INT,
    SizeBucket VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS DimHTTPStatus (
    HTTPStatus INT,
    Description VARCHAR(30),
    Type VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS DimGeoLocation (
    IP VARCHAR(20),
    country_code VARCHAR(10),
    country_name VARCHAR(30),
    city VARCHAR(30),
    State VARCHAR(50),
    latitude VARCHAR(20),
    longitude VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS DimOS (
    UserAgent VARCHAR(400),
    OS VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS DimReferrer (
    UserReferrer VARCHAR(1500),
    Referrer VARCHAR(120)
);

CREATE TABLE IF NOT EXISTS DimTime (
    Time TIME,
    Seconds INT,
    Minute INT,
    Hour INT
);

CREATE TABLE IF NOT EXISTS DimResponseTime (
    ResponseTime INT,
    TimeBucket VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS DimVisit (
    FileInfo VARCHAR(120),
    IP VARCHAR(20),
    Type VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS OutFact1 (
    Date DATE,
    Time TIME,
    FileInfo VARCHAR(120),
    IP VARCHAR(20),
    UserAgent VARCHAR(400),
    UserReferrer VARCHAR(1500),
    HTTPStatus INT,
    FileSize INT,
    ResponseTime INT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimBrowser.txt' 
INTO TABLE DimBrowser 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimDate.txt' 
INTO TABLE DimDate 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimFileInfo.txt' 
INTO TABLE DimFileInfo 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimFileSize.txt' 
INTO TABLE DimFileSize 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimHTTPStatus.txt' 
INTO TABLE DimHTTPStatus 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimGeoLocation.txt' 
INTO TABLE DimGeoLocation 
CHARACTER SET latin1
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimOS.txt' 
INTO TABLE DimOS 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimReferrer.txt' 
INTO TABLE DimReferrer 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimResponseTime.txt' 
INTO TABLE DimResponseTime 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimTime.txt' 
INTO TABLE DimTime 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DimVisit.txt' 
INTO TABLE DimVisit 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/OutFact1.txt' 
INTO TABLE OutFact1 
FIELDS TERMINATED BY ',' 
IGNORE 1 LINES;