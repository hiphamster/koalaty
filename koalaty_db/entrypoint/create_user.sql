CREATE USER 'koalaty'@'localhost' IDENTIFIED BY 'eucalyptus';
CREATE USER 'koalaty'@'%' IDENTIFIED BY 'eucalyptus';

GRANT ALL PRIVILEGES ON *.* TO 'koalaty'@'localhost' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON *.* TO 'koalaty'@'%' WITH GRANT OPTION;
