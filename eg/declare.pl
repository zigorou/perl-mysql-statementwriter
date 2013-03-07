#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use DBI qw(:sql_types);
use MySQL::StatementWriter::Declare;

mysql_writer {
    server_side_prepare 1;
    do_query << "SQL";
DROP TABLE IF EXISTS sample;
CREATE TABLE sample (
  id int(10) unsigned not null,
  name varchar(32) not null,
  created_at int(10) unsigned not null,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS sample_count;
CREATE TABLE sample_count (
  num int(10) unsigned not null default 0
) ENGINE=InnoDB;

INSERT INTO sample_count(num) VALUES(0);
SQL

    delimiter "//";
    do_query << "SQL";
CREATE TRIGGER sample_on_after_insert AFTER INSERT ON sample 
FOR EACH ROW BEGIN
    UPDATE sample_count SET num = num + 1;
END;

SQL
    delimiter ";";

    my $sth = prepare {
        name "stmt";
        statement "INSERT INTO sample(id, name, created_at) VALUES(?, ?, ?)";
    };

    txn {
        $sth->execute({ value => 1, type => SQL_INTEGER }, "foo", \'UNIX_TIMESTAMP()');
        $sth->execute({ value => 2, type => SQL_INTEGER }, "bar", { value => time, type => SQL_INTEGER });
    };

};
