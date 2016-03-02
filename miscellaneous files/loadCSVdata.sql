LOAD DATA INFILE '/var/lib/mysql/users/users.csv' INTO TABLE userPasswordTable
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql/users/userinfo.csv' INTO TABLE userProfileTable
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

