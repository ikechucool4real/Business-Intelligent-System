CREATE TABLE DimBrowser (
    UserAgent VARCHAR(400),
    Browser VARCHAR(20)
);

CREATE TABLE DimDate (
    Date DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT,
    DayofWeek VARCHAR(20)
);

CREATE TABLE DimFileInfo (
    File VARCHAR(120) DEFAULT '',
    Name VARCHAR(100) DEFAULT '',
    Type VARCHAR(10) DEFAULT ''
);

CREATE TABLE DimFileSize (
    FileSize INT,
    SizeBucket VARCHAR(20)
);

CREATE TABLE DimHTTPStatus (
    HTTPStatus INT,
    Description VARCHAR(30),
    Type VARCHAR(20)
);

CREATE TABLE DimGeoLocation (
    IP VARCHAR(100),
    country_code VARCHAR(100),
    country_name VARCHAR(100),
    city VARCHAR(100),
    State VARCHAR(100),
    latitude VARCHAR(100),
    longitude VARCHAR(100)
);

CREATE TABLE DimOS (
    UserAgent VARCHAR(400),
    OS VARCHAR(20)
);

CREATE TABLE DimReferrer (
    UserReferrer VARCHAR(1500),
    Referrer VARCHAR(120)
);

CREATE TABLE DimTime (
    Time TIME,
    Seconds INT,
    Minute INT,
    Hour INT
);

CREATE TABLE DimResponseTime (
    ResponseTime INT,
    TimeBucket VARCHAR(20)
);

CREATE TABLE DimVisit (
    FileInfo VARCHAR(120),
    IP VARCHAR(20),
    Type VARCHAR(10)
);

CREATE TABLE OutFact1 (
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

COPY DimBrowser FROM './DimBrowser.txt' DELIMITER ',' CSV HEADER;
COPY DimDate FROM './DimDate.txt' DELIMITER ',' CSV HEADER;
COPY DimFileInfo FROM './DimFileName.txt' DELIMITER ',' CSV HEADER;
COPY DimFileSize FROM './DimFileSize.txt' DELIMITER ',' CSV HEADER;
COPY DimHTTPStatus FROM './DimHTTPStatus.txt' DELIMITER ',' CSV HEADER;
COPY DimGeoLocation FROM './DimGeoLocation.txt' DELIMITER ',' CSV HEADER;
COPY DimOS FROM './DimOS.txt' DELIMITER ',' CSV HEADER;
COPY DimReferrer FROM './DimReferrer.txt' DELIMITER ',' CSV HEADER;
COPY DimResponseTime FROM './DimResponseTime.txt' DELIMITER ',' CSV HEADER;
COPY DimTime FROM './DimTime.txt' DELIMITER ',' CSV HEADER;
COPY DimVisit FROM './DimVisit.txt' DELIMITER ',' CSV HEADER;
COPY OutFact1 FROM './OutFact1.txt' DELIMITER ',' CSV HEADER;