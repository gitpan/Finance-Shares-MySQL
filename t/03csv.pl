#!/usr/bin/perl
use strict;
use warnings;
use Finance::Shares::MySQL;

my $db = new Finance::Shares::MySQL( 
	user => 'tester',
	database => 'stocks',
	logfile => 'ba.log',
	directory => 'test-results',
    );
$db->to_csv_file('BA.L', '2002-06-01', '2002-06-14', 'ba.csv', 'test-results');

