#!/usr/bin/perl
use strict;
use warnings;
use Finance::Shares::MySQL;

my $db = new Finance::Shares::MySQL( 
	user => 'tester',
	database => 'stocks',
    );
my $cols = [qw(qdate open high low close volume)];
my $rh = $db->select_table('BSY.L', $cols, '2002-08-01', '2002-08-31');
Finance::Shares::MySQL::print_table( $rh, $cols, 'bsy_l.csv', 'test-results' );

