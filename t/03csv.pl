#!/usr/bin/perl
use strict;
use warnings;
use Finance::Shares::MySQL;

my @logfile = ('ba.log', 'test-results');
Finance::Shares::MySQL->default_log_file(@logfile);

my $db = new Finance::Shares::MySQL( 'tester' );
$db->log_file(@logfile);
$db->to_csv_file('BA.L', '2002-06-01', '2002-06-14', 'ba.csv', 'test-results');

