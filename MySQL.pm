package Finance::Shares::MySQL;
use strict;
use warnings;
use DBI qw(:DEFAULT :sql_types);
use Term::ReadKey;
use LWP::UserAgent;
use Finance::Shares::Log qw(:file :date);
use Date::Pcalc qw(:all);
require Exporter;

# Prototypes of local functions only
 sub search_array ($$;$);
 sub end_of_block ($$);

# Global constants
our $agent_name = 'Finance::Shares::MySQL';
our $VERSION = 0.02;
our @EXPORT_OK = qw(yahoo_uk);
our $todaystr = today_as_string();

=head1 NAME

Finance::Shares::MySQL - Access to stock data stored in a database

=head1 SYNOPSIS

    use Finance::Shares::MySQL;
    use Finance::Shares::MySQL qw(yahoo_uk);
    
=head2 Simplest

Fetch quotes for one share from the internet and store in the database.  Then output the data as a CSV file whose name is made from the EPIC and start and
end dates.

    my $db = new Finance::Shares::MySQL( user => $user );
    $db->fetch($epic, $date1, $date2);
    $db->to_csv_file($epic, $date1, $date2);

=head2 Typical

Fetch quotes listed in a file and inspect quotes for one share.

    my $db = new Finance::Shares::MySQL ( 
		user	  => $user,
		password  => $password,
		directory => '~/stocks',
		logfile	  => 'db.log',
		loglevel  => 1 );
    
    my $failures = $db->fetch_from_file( "db.req" );
    Finance::Shares::MySQL->
	print_requests( $failures, "next.req" );

    my $table = $db->select_table( "BSY_L" );
    Finance::Shares::print_table( $table, "BSY.csv" );

=head1 DESCRIPTION

The intent is to provide access to stock quotes and possibly other data, using an underlying mysql database to do
all the hard work.

=head2 Preparing the Database

Before using this module, the necessary permissions must be in place on the database server you wish to use.
This usually means logging on as root and giving the necessary password:

    root@here# mysql -u root -p mysql
    Password:

Within mysql grant the user the necessary privileges (replace quoted items with your own):

    mysql> grant all privileges on "shares".* to "joe"
	   identified by "password";

Global file privileges and all privileges on the named database should be sufficient for Finance::Shares::MySQL
use.
    
=head2 Accessing the Database

If a mysql database is available, a Finance::Shares::MySQL object handles accesses to it.  The constructor ensures
a connection is made and processing is logged.  Data is not entered directly, but fetched from the internet.  It
is then made available either as an array or a csv file.

The first step is to call one of the C<fetch> methods.  B<fetch_from_file> is probably the most convenient method
for keeping the database up to date.  If no dates are given, all shares listed in the file will be updated as
needed, attempting to refetch any failed requests.  B<fetch_batch> is the method that processes these requests,
and B<fetch> is handles the HTTP transfer of quotes for a single share.

B<select_table> is the principal function for accessing the share data.  It is a wrapper around an SQL SELECT
call, returning an array of arrays and the field order.  A class method is provided which will print the data
returned.  Alternatively B<to_csv_file> extracts quotes from the database and saves them in a suitably named file.

=head1 CONSTRUCTOR

=cut

sub new {
    my $class = shift;
    my $opt = {};
    if (@_ == 1) { $opt = $_[0]; } else { %$opt = @_; }
   
    my $o = {};
    bless( $o, $class );

    $opt->{port}     = 3306	      unless (defined $opt->{port});
    $opt->{hostname} = 'localhost'    unless (defined $opt->{hostname});
    $opt->{database} = 'shares'	      unless (defined $opt->{database});
    $opt->{user}     = $ENV{DBI_USER} unless (defined $opt->{user});
    $opt->{password} = $ENV{DBI_PASS} unless (defined $opt->{password});
    
    $o->{lf} = Finance::Shares::Log->new();
    $o->{lf}->file($opt->{logfile}, $opt->{directory}) if defined($opt->{logfile});
    $o->{lf}->level($opt->{loglevel}) if defined($opt->{loglevel});
    $o->{dir} = $opt->{directory} || File::Spec->curdir();
    
    die "User required\nStopped" unless ($opt->{user});
    unless ($opt->{password}) {
	print "Database password: ";
	ReadMode 'noecho';
	$opt->{password} = ReadLine 0;
	chomp $opt->{password};
	ReadMode 'normal';
	print "\n";
    }
    
    $o->{dbsource} = "DBI:mysql:;host=$opt->{hostname};port=$opt->{port}"; 
    $o->{dbh} = DBI->connect($o->{dbsource}, $opt->{user}, $opt->{password}, {RaiseError => 0, PrintError => 0});
    $o->{lf}->log(0, "Cannot connect to \'$o->{dbsource}\' : $DBI::errstr\nStopped") unless $o->{dbh};
    
    $o->{dbname} = $opt->{database};
    unless (search_array($o->show("databases"), $o->{dbname})) {
	$o->{dbh}->do("create database $o->{dbname}")
	    or $o->{lf}->log(0, "Cannot create \'$o->{dbname}\' : $o->{dbh}->errstr()\nStopped");
    }
    $o->{dbh}->do("use $o->{dbname}")
	or $o->{lf}->log(0, "Cannot use \'$o->{dbname}\' : $o->{dbh}->errstr()\nStopped");

    $o->{lf}->log(1, "");
    $o->{lf}->log(1, "Connected to mysql using \'$o->{dbname}\' database");

    $o->{ua} = new LWP::UserAgent;
    $o->{ua}->agent("$agent_name/$VERSION " . $o->{ua}->agent);

    $o->{urlfn} = defined($opt->{url_function}) ? $opt->{url_function} : \&yahoo_uk;
    return $o;
}

=head2 new( [options] )

A connection is made to the mysql server and the specified database is selected for use.  

It will die if the user is not known to the server or hasn't the requisite permissions.  If the user has I<create>
privileges with the server, an attempt is made to create the database if necessary.

For testing purposes, the environment variables C<DBI_USER> and C<DBI_PASS> are consulted if no user or password
are given.

C<options> may be either a hash ref or a list of hash keys and values.  Recognized keys are:

=head3 user

The user's name, e.g. 'joe'.

=head3 password

To avoid passing this as plain text, enter ''.  The password will be asked for interactively if it is not
specified here.

=head3 database

Defaults to I<shares>.

=head3 hostname

Defaults to I<localhost>.

=head3 port

Defaults to I<3306>, the port number for mysql.

=head3 directory

The default directory to use for the log and csv files.

=head3 logfile

Name of the log file.  See method L<logfile>.

=head3 loglevel

See method L<loglevel>.

=head3 url_function

Specify an alternative function for constructing the URL for fetching the quotes.  (Default: \&yahoo_uk)

See L<yahoo_uk> for function details.

=cut

sub DESTROY {
    my ($o) = @_;
    $o->{dbh}->disconnect();
    $o->{lf}->log(1, "Disconnected from mysql");
}


=head1 OBJECT METHODS

=cut

sub show {
    my($o, $string) = @_;
    return $o->{dbh}->selectall_arrayref("show $string");
}

=head2 show( item )

Pass the string C<item> to the SQL C<show> command.  Return a reference to an array of array references.  For
example:

    @$array_ref = ( [ "mysql" ]
		    [ "test" ]
		    [ "stocks" ] )

=cut

sub fetch {
    my ($o, $epic, $startstr, $endstr, $table) = @_;
    my $start_day = days_from_string( $startstr );
    my $end_day = days_from_string( $endstr );
    ($table = $epic) =~ s/[^\w]/_/g unless $table;
    $table = uc($table);
    my $failed = [];
    return $failed unless ($start_day <= $end_day);

    ## Ensure table exists
    unless (search_array($o->show("tables"), $table)) {
	my $job = "create table $table ( qdate date not null, ";
	$job   .= "open decimal(6,2), high decimal(6,2), low decimal(6,2), close decimal(6,2), ";
	$job   .= "volume integer, primary key (qdate) )";
	$o->{dbh}->do($job)
	    or $o->{lf}->log(0, "Cannot create table \'$table\' : $o->{dbh}->errstr()\nStopped");
    }
    
    ## Identify any duplicates
    my %dates = ();
    foreach my $ar (@{ $o->{dbh}->selectall_arrayref("select qdate from $table") }) {
	$dates{@$ar[0]}++;
    }
    
    ## Split into 200 day chunks
    $o->{lf}->log(1, "Requesting $epic from $startstr to $endstr");
    my ($sd, $ed);
    my ($total_fetched, $total_entered) = (0, 0);
    for ( $sd = $start_day, $ed = end_of_block($sd, $end_day); 
	  $sd <= $end_day; 
	  $sd = $ed + 1, $ed = end_of_block($sd, $end_day) ) {
	
	## Get file from internet
	my $func = $o->{urlfn};
	my $reqfile = &$func($epic, $sd, $ed);
	my $req = new HTTP::Request GET => $reqfile;
	my $sdstr = string_from_days($sd);
	my $edstr = string_from_days($ed);
	if ($o->present($table, $sdstr, $edstr)) {
	    $o->{lf}->log(2, "     $epic from $sdstr to $edstr already present");
	} else {
	    $o->{lf}->log(2, "     asked $epic from $sdstr to $edstr");
	    my $res = $o->{ua}->request($req);
	    if (not $res->is_success) {
		$o->{lf}->log(1, "     ERROR - Unsuccessful request:\n\t\"$reqfile\"");
		push @$failed, [$epic, $sdstr, $edstr];
	    } else {
		# Insert data into mysql table
		my $data = $res->content();
		pos($data) = 0;
		my ($fetched, $entered) = (0, 0);
		while ( $data =~ /^(.+)$/mg ) {
		    my @fields = split (/,/, $1);
		    # Identify lines beginning "31-Dec-02,..."
		    my ($d, $m, $y) = ($fields[0] =~ /(\d+)-(\w+)-(\d+)/);
		    if (defined($y)) {
			$fetched++;
			my $date = string_from_ymd( $y, $m, $d );
			if ( not exists($dates{$date}) ) {
			    $fields[0] = "\"$date\"";
			    my $line = join(",", @fields);
			    $o->{dbh}->do( "insert into $table (qdate, open, high, low, close, volume) values($line)" )
				or $o->{lf}->log(0, "Cannot insert into table \'$table\' : $o->{dbh}->errstr()\nStopped");
			    $entered++;
			}
		    }
		}
		my $unit = ($fetched == 1) ? "date" : "dates";
		$o->{lf}->log(2, "     $fetched $unit fetched,  $entered entered");
		$total_fetched += $fetched;
		$total_entered += $entered;
	    }
	}
    }
    my $unit = ($total_fetched == 1) ? "date" : "dates";
    $o->{lf}->log(1, "$total_fetched $unit fetched,  $total_entered entered in total");
    my $no_failed = @$failed;
    $o->{lf}->log(1, "$no_failed failed requests");
    return $failed;
}

=head2 fetch( epic, start_date, end_date [, table] )

=over 4

=item C<epic>

The stock code whose quotes are being requested.

=item C<start_date>

The first day to fetch, in the form YYYY-MM-DD.

=item C<end_date>

The last day to fetch, in the form YYYY-MM-DD.

=item C<table>

An optional name for the database table if C<epic> is not suitable.

=back

Quote data is fetched from the internet for one stock over the given period.  If successful, the data is added to
a table named C<epic>.  

Note that any non-alphanumeric characters in C<epic> will be mapped to underscore ('_') in creating the default
name for the table.  The table name may be given directly if this proves unsuitable.

Returns any failed requests as an array of (epic, start_date, end_date) arrays.

=cut

sub fetch_batch {
    my ($o, $requests, $startstr, $endstr) = @_;
    $endstr = $todaystr unless $endstr;
    my $failed = [];
    foreach my $ar (@$requests) {
	my ($epic, $sstr, $estr, $table) = @$ar;
	unless ($table) {
	    ($table = $epic) =~ s/[^\w]/_/g;
	    $table = uc($table);
	}
	$sstr = $startstr unless $sstr;
	$sstr = $o->last($table) unless $sstr;
	$estr = $endstr unless $estr;
	my $res = $o->fetch($epic, $sstr, $estr, $table);
	push @$failed, @$res if (@$res);
    }
    my $nfailed = @$failed;
    if ($nfailed) {
	$o->{lf}->log(1, "$nfailed batch requests failed");
	foreach my $ar (@$failed) {
	    my $line = join(", ", @$ar);
	    $o->{lf}->log(2, "     [$line]");
	}
    }
    return $nfailed ? $failed : ();
}

=head2 fetch_batch( requests [, start_date [, end_date]] )

=over 4

=item C<requests>

This should be an array reference.  The array in question should contain references to arrays, one for each
stock request.  These sub-arrays should contain an EPIC and start and end dates in YYYY-MM-DD format.  If the epic
won't produce a suitable name for a mysql table, a table name may be added.

=item C<start_date>

An optional date in YYYY-MM-DD format.  This becomes the default start date.

=item C<end_date>

An optional date in YYYY-MM-DD format.  This becomes the default end date.

=back

Fetch a number of stock quotes from the internet and enter them into the database.  Any failed requests are
returned in the same format, ready for resending.  0 is returned if there are no failed requests.

=head3 Example 1

Ensure that the requests are satisfied.

  my $requests = [ [ BP.L, 2000-01-01, 2000-12-31 ],
       [ BSY.L, 2002-06-01, 2002-09-04, "BSkyB" ] ];
  do {
  	my $failed = fetch_batch( $requests );
  } while ($failed);

=head3 Example 2

If the requests structure has dates as "", 0 or undefined, the specified defaults are used.  Where no defaults are
given the end date becomes today.  The start date becomes either the last date stored or today if there was none.

  my $requests = [ ["RIO.L"], ["BT.L", "2002-01-01"],
  	 ["DMGOa.L", 0, "2002-03-31", "DMGO_L1"] ];
  fetch_batch( $requests );
  
      RIO.L	as RIO_L    from last date to today
      BT.L	as BT_L	    from 2002-01-01 to today
      DMGOa.L as DMGO_L1  from last date to 2002-03-31  
  
  fetch_batch( $requests, "1999-11-20" );
  
      RIO.L	as RIO_L    from 1999-11-20 to today
      BT.L	as BT_L	    from 2002-01-01 to today
      DMGOa.L as DMGO_L1  from 1999-11-20 to 2002-03-31  
  
  fetch_batch( $requests, "1999-11-20", "2000-12-31" );
  
      RIO.L	as RIO_L    from 1999-11-20 to 2000-12-31
      BT.L	as BT_L	    from 2002-01-01 to 2000-12-31
      DMGOa.L as DMGO_L1  from 1999-11-20 to 2002-03-31

The last BT request would be ignored as the end date is before the start date.

=cut

sub fetch_from_file {
    my $o = shift;
    my $file = shift;
    my $dir = shift;
    my $start_date;
    if (defined($dir)) {
	if ($dir =~ /\d{4}-\d{2}-\d{2}/) {
	    $start_date = $dir;
	    $dir = $o->{dir};
	} else {
	    $start_date = shift;
	}
    } else {
	$dir = $o->{dir};
	$start_date = shift;
    }
	
    my $end_date = shift;
    $end_date = $todaystr unless $end_date;

    $file = check_file($file, $dir);
    open(FILE, "<", $file) or $o->{lf}->log(0, "Unable to read from \'$file\': $!\nStopped");
    my $requests = [];
    while( my $line = fetch_line(*FILE) ) {
	push @$requests, [ split(/\s*[, ]\s*/, $line) ];
    }
    my ($count, $maxcount) = (0, 3);
    while( defined($requests) and @$requests and (++$count <= $maxcount) ) {
	$requests = $o->fetch_batch( $requests, $start_date, $end_date );
	$o->{lf}->log(1, "Try $count of $maxcount failed") if ($requests);
    }
    return $requests;
}

=head2 fetch_from_file(file [, dir] [, start [, end])

=over 4

=item C<file>

A fully qualified path-and-file or a simple file name.

=item C<dir>

An optional directory.  If present (and C<file> is not already an absolute path), it is prepended to
C<file>.

=item C<start>

An optional date in YYYY-MM-DD format.  This becomes the default start date.

=item C<end>

An optional date in YYYY-MM-DD format.  This becomes the default end date.

=back

The stock codes (and dates) to be fetched are stored in the specified file.  The return value is the same as
L<fetch_batch()>, although 3 attempts are made before any requests are failed.

The file may have '#' comments and blank lines, with leading and trailing spaces stripped.  Each line should be of
the following form, with items separated by spaces or commas.

  <epic> [, <start_date> [, <end_date> [, <table_name>]]]

=head3 Example 3

  # Rio Tinto will take on both default dates while
  # BT uses the default end date (probably 'today').
  # The Daily Mail 'a' stock will be fetched from
  # the default start (probably the last quote) to
  # 31st March, stored in mysql table DMGO_L1.
  
  RIO.L
  BT.L, 2002-01-01
  DMGOa.L, "", 2002-03-31, DMGO_L1

=cut

sub select_table {
    my ($o, $table, $colref, $start_date, $end_date) = @_;
    $table =~ s/[^\w]/_/g;
    $table = uc($table);
    my $columns = $colref ? "qdate, " . join(", ", @$colref) : "*";
    my $where = $start_date ? qq(where qdate >= "$start_date") : "";
    $where .= ($where and $end_date) ? qq( and qdate <= "$end_date") : "";
    my $query = "select $columns from $table $where";
    my $sth;
    if( $sth = $o->{dbh}->prepare($query) ) {
	if( $sth->execute() ) {
	    my @rows;
	    while( my @array = $sth->fetchrow_array() ) {
		push @rows, [ @array ];
	    }
	    if (wantarray()) {
		return ( \@rows, $colref );
	    } else {
		return \@rows;
	    }
	} else {
	    $o->{lf}->log(1, "ERROR - failed to execute query:\n\t\'$query\'");
	}
    } else {
	$o->{lf}->log(1, "ERROR - failed to prepare query:\n\t\'$query\'");
    }
    return ();
}

=head2 select_table( table [, columns [, start [, end]] )

=over 4

=item C<table>

Must be the name of a table in the database.

=item C<columns>

A reference to an array holding column names.  Probably best specified as C<[qw(...)]>.  If omitted, all columns
will be returned.

=item C<start>

An optional start date in YYYY-MM-DD format.  If omitted, values for all dates will be returned.

=item C<end>

An optional end date in YYYY-MM-DD format.  Both dates are inclusive.

=back

Perform a SQL I<select> command on the database to extract a single table.  

If called in an array context, this returns two array refs.  The first is the list of columns requested (it is just the
'columns' argument).  If this is undefined, all columns have been returned.  The second array holds arrayrefs
indicating each row of data.  In a scalar context, only the rows arrayref is returned.

=head3 Example 4

To extract BP price data for the week beginning 5th August 2002.

    my ($rows, $cols) = 
	$db->select_table('BP_L', [qw(open close)],
		    '2002-08-05', '2002-08-09');

$rows would hold the open and close values for the dates requested.

    [ [ 2002-08-05, 527.39, 560.00 ],
      [ 2002-08-06, 542.14, 564.00 ],
      [ 2002-08-07, 555.89, 573.50 ],
      [ 2002-08-08, 571.13, 575.00 ],
      [ 2002-08-09, 576.05, 589.50 ] ]

$cols would point to the list of column names in the order requested.

    [ '', 'open', 'close' ]

Note that the date column is inserted automatically.

If something goes wrong, the error is logged and 'false' is returned.

=cut

sub to_csv_file {
    my ($o, $epic, $start, $end, $file, $dir) = @_;
    $end = $todaystr unless (defined $end);
    $file = "$epic-$start-$end.csv" unless (defined $file);
    my ($rows, $cols) = $o->select_table($epic, [qw(Open High Low Close Volume)], $start, $end);
    Finance::Shares::MySQL::print_table($o, $rows, $cols, $file, $dir);
}

=head2 to_csv_file( epic, start, end [,file [,dir]] )

=over 4

=item C<epic>

The share ID e.g. BSY.L.

=item C<start>

The first date required, in YYYY-MM-DD format.

=item C<end>

The last date required, in YYYY-MM-DD format.

=item C<file>

Optional file name.

=item C<dir>

Optional directory prepended to the file name.

=back

Save a portion of stock data to a csv file.  If no file name is given, one is created from the share name and the
dates.

=cut
    
sub last {
    my ($o, $table) = @_;
    my @array = $o->{dbh}->selectrow_array("select * from $table order by qdate desc");
    if (wantarray()) {
	return @array;
    } elsif (defined wantarray()) {
	return $array[0];
    }
}

=head2 last( table )

In a scalar context, return the date of the most recent quote in the named table.  In array context the whole
record is returned.

=cut

sub present {
    my ($o, $table, $start, $end) = @_;
    my $query = qq(select count(*) from $table where qdate >= '$start' and qdate <= '$end');
    my $days_found = 0;
    my $sth;
    if( $sth = $o->{dbh}->prepare($query) ) {
	if( $sth->execute() ) {
	    ($days_found) = $sth->fetchrow_array();
	} else {
	    $o->{lf}->log(1, "ERROR - failed to execute query:\n\t\'$query\'");
	}
    } else {
	$o->{lf}->log(1, "ERROR - failed to prepare query:\n\t\'$query\'");
    }
    $sth->finish();
    my $total_days = Delta_Days( ymd_from_string($start), ymd_from_string($end) ) + 1;
    my $fraction = $days_found/$total_days;
    #print "$days_found/$total_days = $fraction\n";

    return $fraction > 5/8 ? 1 : 0;
}

=head2 present( table, start, end )

Check whether an appropriate number of values exist in the specified table between the dates given.

Return 1 if seems ok, 0 otherwise.

=cut

=head1 ACCESS METHODS

=cut
sub logfile {
    my ($o, $file, $dir) = @_;
    if (defined $file) {
	$dir = $o->{dir} unless $dir;
	$o->{lf}->file($file, $dir);
    }
    return $o->{lf}->file();
}

=head2 logfile( [file [, dir]] )

=over 4

=item C<file>

An optional fully qualified path-and-file, a simple file name, or "" for null device.

=item C<dir>

An optional directory.  If present (and C<file> is not already an absolute path), it is prepended to
C<file>.

=back

Specify the file to use for logging.  If it doesn't already exist, it is created.  With no arguments, this redirects output to
STDERR, while "" is interpreted as the NULL device.

Returns current logfile or null if STDERR.

=cut

sub loglevel {
    my ($o, $level) = @_;
    if (defined $level) {
	$o->{lf}->level($level);
    }
    return $o->{lf}->level();
}

=head2 loglevel( [level] )

Subsequent log messages will only be output if they are marked as less than or equal to C<level>.  Suitable values
are 0, 1 or 2.

Returns the last message threshold set.

=cut

sub directory {
    my ($o, $dir) = @_;
    if ($dir) {
	$dir = expand_tilde($dir);
	if (File::Spec->file_name_is_absolute($dir)) {
	   $o->{dir} = $dir;
       } else {
	   $o->{dir} = File::Spec->rel2abs($dir);
       }
   } else {
       $o->{dir} = File::Spec->curdir();
   }
}

=head2 directory( [dir] )

Set the default directory for source files etc.  If C<dir> is '', it is set to the current directory.

Return the current default directory.

=cut

=head1 CLASS METHODS

=cut

sub print_requests {
    my ($class, $req, $file, $dir) = @_;
    $dir = File::Spec->curdir() unless $dir;
    my $fh;
    if ($file) {
	$file = check_file($file, $dir);
	open($fh, ">", $file) or die("Unable to write to \'$file\' : $!\nStopped");
    } else {
	$fh = *STDERR;
    }
    if ($req) {
	print $fh "# Requests failed on $todaystr\n";
	foreach my $ar (@$req) {
	    print $fh join(", ", @$ar) . "\n";
	}
    }
    close $fh if $file;
}

=head2 print_requests( req [, file [, dir]] )

=over 4

=item C<req> 

An array reference as returned by L<fetch_batch()> or L<fetch_from_file()>.

=item C<file>

An optional file to dump the requests to.

=item C<dir>

The file and directory may optionally be given seperately.

=back

Prints out any failed requests.  If C<file> is omitted, the listing is sent to STDERR, otherwise the output is
written in a format that may be read by L<fetch_from_file()> for fetching later.

=cut

sub print_table {
    my ($class, $rows, $cols, $file, $dir) = @_;
    $cols = [] unless defined($cols);
    $dir = File::Spec->curdir() unless $dir;
    my $fh;
    if ($file) {
	$file = check_file($file, $dir);
	open($fh, ">", $file) or die("Unable to write to \'$file\' : $!\nStopped");
    }
    
    if ($rows) {
	if ($file) {
	    print $fh join(",", ('', @$cols)) . "\n";
	    foreach my $row (@$rows) {
		print $fh join(",", (@$row)) . "\n";
	    }
	    close $fh;
	} else {
	    my $gap = 10;
	    foreach my $head ('', @$cols) {
		printf("%*s ", $gap, $head);
	    }
	    print "\n";
	    foreach my $row (@$rows) {
		foreach my $item (@$row) {
		    printf("%*s ", $gap, $item);
		}
		print "\n";
	    }
	}
    }
}

=head2 print_table( rows [, cols [, file [, dir]]] )

=over 4

=item C<rows> 

An array ref listing rows of data, as returned by L<select_table>.

=item C<cols>

An array ref listing the columns, as returned by L<select_table>.

=item C<file>

An optional file to dump the requests to.

=item C<dir>

The file and directory may optionally be given seperately.

=back

Prints out the results of a C<select_table()> call.  If C<file> is omitted, the output is sent to STDOUT (not
STDERR notice), otherwise the output is written to the file in CSV format.

=cut


=head1 EXPORTED FUNCTIONS

There should be no need to call this function externally, but it is made available for completeness.

    use Finance::Shares::MySQL qw(yahoo_uk);

=cut

sub yahoo_uk {
    my ($epic, $start_day, $end_day) = @_;
    my $url = "http://uk.table.finance.yahoo.com/table.csv";
    my ($day, $month, $year) = ymd_from_days( $start_day );
    $url .= ("?a=" . $month . "&b=" . $day . "&c=" . $year);
    ($day, $month, $year) = ymd_from_days( $end_day );
    $url .= ("&d=" . $month . "&e=" . $day . "&f=" . $year . "&s=$epic");
    return $url;
}
# $block_end = end_of_block( $block_start, $max_end )
# Return the smaller of +200 weekdays or the end day

=head2 yahoo_uk( epic, start, end )

=over 4

=item C<epic>

The abbreviation used to identify the stock and exchange.  E.g. 'BSY.L' for BSkyB quoted in London.

=item C<start>

The first quote date requested, in YYYY-MM-DD format.

=item C<end>

The last quote date requested, in YYYY-MM-DD format.

=back

The default function for constructing a url.  This one accesses http://uk.table.finance.yahoo.com.  Obviously
targetted for the London Stock Exchange, it will fetch quotes from other exchanges.  Try it first before writing
a replacement.

Any replacement should accept the three strings above, and return a fully qualified URL.

Example

    yahoo_uk('BA.L', '2002-06-01', '2002-06-30')

This would return (on a single line, of course)

    'http://uk.table.finance.yahoo.com/table.csv?
		a=6&b=1&c=2002&d=6&e=30&f=2002&s=BA.L'

=cut

### PRIVATE FUNCTIONS

# search_array( array_ref, string, [index] )
# 'array_ref' should refer to an array of array references.  If 'index' is given, it is the position in the
# subarray where 'string' is expected to be.
# Return the sub-array (NOT ref) found or ().

sub search_array ($$;$) {
    my ($ar, $value, $idx) = @_;
    $idx = 0 unless $idx;
    foreach my $rr (@$ar) {
	return @$rr if ($rr->[$idx] eq $value);
    }
    return ();
}

# $url = request_url( "BSY.L", "YYYY-MM-DD", "YYYY-MM-DD" );
# Turn epic, start and end dates into a suitable url

sub end_of_block ($$) {
    my ($sd, $end_day) = @_;
    my $ed = $sd + 280;	    # 200 weekdays
    return ($ed < $end_day) ? $ed : $end_day;
}

=head1 BUGS

Please report those you find to the author.

=head1 AUTHOR

Chris Willmot, chris@willmot.org.uk

=head1 SEE ALSO

L<PostScript::Graph::Stock>,
L<Finance::Shares::Log>.
L<Finance::Shares::Sample>.

=cut

1;
