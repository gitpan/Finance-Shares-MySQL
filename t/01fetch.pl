#!/usr/bin/perl
use strict;
use warnings;
use Finance::Shares::MySQL;

my $db = new Finance::Shares::MySQL( 'tester' );
$db->fetch('BSY.L', '2002-08-01', '2002-08-31');

