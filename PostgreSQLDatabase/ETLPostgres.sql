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
    IP VARCHAR(20),
    country_code VARCHAR(10),
    country_name VARCHAR(30),
    city VARCHAR(30),
    State VARCHAR(50),
    latitude VARCHAR(20),
    longitude VARCHAR(20)
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


COPY DimBrowser FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimBrowser.txt' DELIMITER ',' CSV HEADER;
COPY DimDate FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimDate.txt' DELIMITER ',' CSV HEADER;
COPY DimFileInfo FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimFileInfo.txt' DELIMITER ',' CSV HEADER;
COPY DimFileSize FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimFileSize.txt' DELIMITER ',' CSV HEADER;
COPY DimHTTPStatus FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimHTTPStatus.txt' DELIMITER ',' CSV HEADER;
COPY DimGeoLocation FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimGeoLocation.txt' DELIMITER ',' CSV HEADER;
COPY DimOS FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimOS.txt' DELIMITER ',' CSV HEADER;
COPY DimReferrer FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimReferrer.txt' DELIMITER ',' CSV HEADER;
COPY DimResponseTime FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimResponseTime.txt' DELIMITER ',' CSV HEADER;
COPY DimTime FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimTime.txt' DELIMITER ',' CSV HEADER;
COPY DimVisit FROM 'C:/Program Files/PostgreSQL/16/data/external_data/DimVisit.txt' DELIMITER ',' CSV HEADER;
COPY OutFact1 FROM 'C:/Program Files/PostgreSQL/16/data/external_data/OutFact1.txt' DELIMITER ',' CSV HEADER;