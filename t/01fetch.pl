#!/usr/bin/perl
use strict;
use warnings;
use Finance::Shares::MySQL;

my $db = new Finance::Shares::MySQL( 
	user => 'tester',
	database => 'stocks',
    );
$db->fetch('BSY.L', '2002-08-01', '2002-08-31');

