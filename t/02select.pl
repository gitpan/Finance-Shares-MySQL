#!/usr/bin/perl
use strict;
use warnings;
use Finance::Shares::MySQL;

my $db = new Finance::Shares::MySQL( 'tester' );
my $rh = $db->select_table('BSY.L', [qw(open high low close volume)], '2002-08-01', '2002-08-31');
Finance::Shares::MySQL::print_table( $db, $rh, 'bsy_l.csv', 'test-results' );

