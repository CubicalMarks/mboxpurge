#!/usr/bin/env perl
#
# mboxpurge.pl, a program for purge old messages from an mbox file.
#
# Copyright (c) 2006 Marcus Libäck <marcus@terminal.se>
#
# This software is provided 'as-is', without any express or implied
# warranty.  In no event will the authors be held liable for any damages
# arising from the use of this software.
#
# Permission is granted to anyone to use this software for any purpose,
# including commercial applications, and to alter it and redistribute it
# freely, subject to the following restrictions:
#
# 1. The origin of this software must not be misrepresented; you must not
#    claim that you wrote the original software. If you use this software
#    in a product, an acknowledgment in the product documentation would be
#    appreciated but is not required.
# 2. Altered source versions must be plainly marked as such, and must not be
#    misrepresented as being the original software.
# 3. This notice may not be removed or altered from any source distribution.
#

use strict;
use warnings;
use Time::Local;
use Getopt::Std;
use Switch;

# Variables
# ---------------------------------------------------------------------
my (
	%opts,
	$month,
	$day,
	@time,
	$year,
	@messages,
	$timestamp,
# 	$age,
# 	$count,
# 	$size,
	$infile,
	$outfile,
);

my $age = 0;
my $count = 0;
my $size = 0;

my $fromline = '^From\s+.*\s+\w{3}\s(\w{3})\s+(\d{1,2})\s+(\d\d:\d\d:\d\d)\s+(\d{4})';
my $purge_count = 0;
my $dry_run = 0;
my $extract = 0;
my $archive = 0;
my $append = 0;
my $verbose = 0;
my $first_message = 1;
my $messagecount = 0;
my $current_time = time();
my $version = "1.0.2-20070315";

my $message = "";
my $messagesize = 0;
my $mboxsize = 0;
my @auxvar;
my $purge_size = 0;
my $messagetimestamp = "";

# Options
# ---------------------------------------------------------------------
getopts('aAdhm:n:s:vx', \%opts);

if ( exists $opts{'h'} ) {
	help();
}

if ( exists $opts{'d'} ) {
	$dry_run = 1;
}

if ( exists $opts{'v'} ) {
	$verbose = 1;
}

if ( exists $opts{'a'} ) {
	if ( exists $opts{'x'} ) {
		print ("You cannot use -a (archive) and -x (extract) at the same time.\n");
		exit (1);
	} else {
		$archive = 1;
	}
}

if ( exists $opts{'x'} ) {
	if ( exists $opts{'a'} ) {
		print ("You cannot use -a (archive) and -x (extract) at the same time.\n");
		exit (1);
	} else {
		$extract = 1;
	}
}

if ( exists $opts{'A'} ) {
	if ( exists $opts{'a'} || exists $opts{'x'} ) {
		$append = 1;
	} else {
		print ("-A (append) must be used in combination with either -a (archive) or -x (extract).\n");
		exit (1);
	}
}

# -m, -n or -s must be presents and in the specified format.

if ( exists $opts{'m'} || exists $opts{'n'} || exists $opts{'s'} ) {
	if ( exists $opts{'m'} ) {
		if ( $opts{'m'} =~ m/^(\d+)([h|d|w|m|y]?)$/ ) {
			switch ( $2 ) {
				case ''  { $age = $1*86400 }
				case 'h' { $age = $1*3600 }
				case 'd' { $age = $1*86400 }
				case 'w' { $age = $1*604800 }
				case 'm' { $age = $1*2592000 }
				case 'y' { $age = $1*31536000 }
			}
		} elsif ( isvaliddate($opts{'m'}) ) {
			$age = time()-isvaliddate($opts{'m'});
			if ( $age < 1 ) {
				print ("$opts{'m'} is in the future\n");
				exit (1);
			}
		} else {
			print ("Invalid max age $opts{'m'}\n");
			exit (1);
		}
	}
	if ( exists $opts{'n'} ) {
		if ( $opts{'n'} =~ m/^\d+$/ ) {
			$count = $opts{'n'};
#			print ("n = $count\n");
		} else {
			print ("Invalid max number of messages $opts{'n'}\n");
			exit (1);
		}
	}
	if ( exists $opts{'s'} ) {
		if ( $opts{'s'} =~ m/^(\d+)([b|k|m|g]?)$/ ) {
			switch ( $2 ) {
				case ''  { $size = $1*1024*1024 }
				case 'b' { $size = $1 }
				case 'k' { $size = $1*1024 }
				case 'm' { $size = $1*1024*1024 }
#				case 'g' { $size = $1*1024*1024*1024 }
			}
# 			print ("s = $size\n");
		} else {
			print ("Invalid max size $opts{'s'}\n");
			exit (1);
		}
	}
} else {
	help();
}

if ( $archive || $extract ) {
	if ( $#ARGV != 1 ) {
		print "You must specify two files when using -a (archive) or -x (extract)\n";
		exit (1);
	}

	$infile = $ARGV[0];
	$outfile = $ARGV[1];

} elsif ( !$archive && !$extract ) {
	if ( $#ARGV != 0 ) {
		print "You must specify one file when purging messages\n";
		exit (1);
	}

	$infile = $outfile = $ARGV[0];
}

if ( $size > 0 ) {
	# Get the size of the mbox
# 	@auxvar = stat $infile;
# 	$mboxsize = $auxvar[7];
	$mboxsize = (stat $infile)[7];
}



# print "age: $age, count: $count, size: $size\n";



# Main
# ---------------------------------------------------------------------



# Read INFILE and separate each message into the @messages array.
open (INFILE, '<', $infile);
while ( <INFILE> ) {
	if ( $first_message ) {
		if ( /$fromline/ ) {
			$messages[$messagecount] .= $_;
			$first_message = 0;
			next;
		} else {
			die "$infile does not seem to be a valid mbox\n";
		}
	} elsif ( /^From / ) {
		$messagecount++;
		$messages[$messagecount] .= $_;
		next;
	} else {
		$messages[$messagecount] .= $_;
		next;
	}
}
close INFILE;

# Extract timestamp from each message and compare to specified age.
open (ORIGINAL, '>', $infile) or die $! if ($archive && !$dry_run);
flock (ORIGINAL, 2) if (defined fileno ORIGINAL);

if ($append) {
	open (OUTFILE, '>>', $outfile) or die $! unless ($dry_run);
} else {
	open (OUTFILE, '>', $outfile) or die $! unless ($dry_run);
}
flock OUTFILE, 2 if (defined fileno ORIGINAL);

foreach ( @messages ) {

	$message = $_;
	$messagesize = length $message;

	m/Subject:\s+(.*)/;
	my $subject = $1;

	#print "$purge_count\n";

	if ( /$fromline/ ) {
		# Convert month to numerical value.
		switch ( $1 ) {
			case "Jan" { $month = 0 }
			case "Feb" { $month = 1 }
			case "Mar" { $month = 2 }
			case "Apr" { $month = 3 }
			case "May" { $month = 4 }
			case "Jun" { $month = 5 }
			case "Jul" { $month = 6 }
			case "Aug" { $month = 7 }
			case "Sep" { $month = 8 }
			case "Oct" { $month = 9 }
			case "Nov" { $month = 10 }
			case "Dec" { $month = 11 }
		}

		$day = $2;
		@time = split (/:/, $3);
		$year = $4;
		$timestamp = timelocal("$time[2]","$time[1]","$time[0]",$day,$month,$year);
		@auxvar = localtime($timestamp);
# 		$messagetimestamp = "$auxvar[8] $auxvar[7] $auxvar[6] $auxvar[5]-$auxvar[4]-$auxvar[3] $auxvar[2]:$auxvar[1]:$auxvar[0]";
		$messagetimestamp = sprintf("%04d-%02d-%02d %02d:%02d:%02d",$auxvar[5]+1900,$auxvar[4]+1,$auxvar[3],$auxvar[2],$auxvar[1],$auxvar[0]);#$auxvar[8],$auxvar[7],$auxvar[6],
	}

	# Archive mode, old messages go into the OUTFILE and the
	# newer are written back to the original file.
	if ( $archive ) {
		if ( !checkconditions($timestamp,$purge_count,$purge_size) ) {
			print OUTFILE $_ if (!$dry_run);
			$purge_count++;
			$purge_size = $purge_size + $messagesize;
			print "Archived [$purge_count $messagetimestamp $messagesize] $subject\n" if ($verbose);
		} else {
			print ORIGINAL $_ if (!$dry_run);
		}
	}
	# Extract mode, old messages are extracted and written to OUTFILE
	# original file is kept intact.
	elsif ( $extract ) {
		if ( !checkconditions($timestamp,$purge_count,$purge_size) ) {
			print OUTFILE $_ if (!$dry_run);
			$purge_count++;
			$purge_size = $purge_size + $messagesize;
			print "Extracted [$purge_count $messagetimestamp $messagesize] $subject\n" if ($verbose);
		}
	}
	# Purge mode, old messages are purged, kept messages either go into
	# the original file or a new file.
	else {
		if ( checkconditions($timestamp,$purge_count,$purge_size) ) {
			print OUTFILE $_ if (!$dry_run);
		} else {
			$purge_count++;
			$purge_size = $purge_size + $messagesize;
			print "Purged [$purge_count $messagetimestamp $messagesize] $subject\n" if ($verbose);
# 			print ($messagesize,"\n");
		}
# print ($messagesize,"\n");
	}
}

flock ORIGINAL, 8 if (defined fileno ORIGINAL);
flock OUTFILE, 8 if (defined fileno ORIGINAL);
close ORIGINAL;
close OUTFILE;

print "$purge_count messages affected.\n";

# print "$mboxsize\n";



# Subroutines
# ---------------------------------------------------------------------

# Date validation, credits to regular-expressions.info.
sub isvaliddate {
	my $input = shift;
	if ($input =~ m/^((?:19|20)\d\d)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$/) {
		# At this point, $1 holds the year, $2 the month and $3 the day of the date entered
		if (($3 == 31) and ($2 == 4 or $2 == 6 or $2 == 9 or $2 == 11)) {
			return 0; # 31st of a month with 30 days
		} elsif ($3 >= 30 and $2 == 2) {
			return 0; # February 30th or 31st
		} elsif ($2 == 2 and $3 == 29 and not ($1 % 4 == 0 and ($1 % 100 > 0 or $1 % 400 == 0))) {
			return 0; # February 29th outside a leap year
		} else {
			return timelocal(0,0,0,$3,$2-1,$1);
		}
	}
	return 0; # Not a date
}



# checkconditions ($timestamp,$purge_count,$purge_size)
# Return true if all conditions are verified
#
sub checkconditions {

	# $age
	# $count
	# $size

	# $current_time
	# $messagecount
	# $mboxsize

	my $timestamp = $_[0];
	my $purge_count = $_[1];
	my $purge_size = $_[2];

	my $check = 1;

	if ( $age != 0 ) {
		if ( $current_time-$timestamp > $age ) {
			$check = $check && 0;
		}
	}
	if ( $count != 0 ) {
		if ( $messagecount-$purge_count >= $count ) {
			$check = $check && 0;
		}
	}
 	if ( $size != 0 && $mboxsize-$purge_size > $size ) {
 		$check = $check && 0;
 	}

	return $check;
}



# Help function.
sub help {
	print "Version $version

mboxpurge.pl purges, archives or extracts old messages out of mbox-style
mailboxes. If nothing else is specified it defaults to just purging
messages rather than archiving or extracting them into another mailbox.

Usage: $0 [options] [infile] [outfile]

Options:
 -h        Display this text.
 -a        Archive mode, archive messages into the second mbox.
 -A        Append to the second mbox instead of overwriting it.
 -d        Dry run, run the program without actually writing to any files.
 -m NN     Maximum age of messages to keep in hours, days, weeks, months or
           years (e.g. 1, 3d, 4y, default use days).
 -m <date> Keep messages newer than this date.
           (dates must in ISO format (YYYY-MM-DD)
 -n NN     Maximum number of messages to keep
 -s NN     Maximum size of mailbox in byte, kilobyte, megabyte
           (e.g. 1, 3k, 10m, default use megabyte)
 -v        Print data of purged, archived or extracted messages.
 -x        Extract mode, extract messages into the second mbox.
           (Keeps the original mbox untouched).\n";

	exit (1);
}

