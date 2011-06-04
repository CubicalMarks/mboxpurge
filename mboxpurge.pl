#!/usr/bin/env perl
#
# mboxpurge.pl, a program for purge old messages from an mbox file.
# 
# Copyright (c) 2006 Marcus Libäck <marcus@terminal.se>
#
# Credit to Tony Freitas <tony@seacow.net> for removing the deprecated switch() code. 
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
	$age,
	$infile,
	$outfile,
);

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
my $version = "1.0.3-20110604";

# Options
# ---------------------------------------------------------------------
getopts('aAdhm:vx', \%opts);

if (exists $opts{'h'}) {
	help();
}

if (exists $opts{'d'}) {
	$dry_run = 1;
}

if (exists $opts{'v'}) {
	if (exists $opts{'v'}) {
		$verbose++;
	}
}

if (exists $opts{'a'}) {
	if (exists $opts{'x'}) {
		print ("You cannot use -a (archive) and -x (extract) at the same time.\n");
		exit (1);
	} else {
		$archive = 1;
	}
} 

if (exists $opts{'x'}) {
	if (exists $opts{'a'}) {
		print ("You cannot use -a (archive) and -x (extract) at the same time.\n");
		exit (1);
	} else {
		$extract = 1;
	}
}

if (exists $opts{'A'}) {
	if (exists $opts{'a'} || exists $opts{'x'}) {
		$append = 1;
	} else {
		print ("-A (append) must be used in combination with either -a (archive) or -x (extract).\n");
		exit (1);
	}
}

# -m must be present and in the specified format. 
if (exists $opts{'m'}) {
	if ($opts{'m'} =~ m/^(\d+)([h|d|w|m|y])$/) {
		if ($2 eq 'h') {
			$age = $1*3600;
		} elsif ($2 eq 'd') {
			$age = $1*86400;
		} elsif ($2 eq 'w') {
			$age = $1*604800;
		} elsif ($2 eq 'm') {
			$age = $1*2592000;
		} elsif ($2 eq 'y') {
			$age = $1*31536000;
		}
	} elsif (isvaliddate($opts{'m'})) {
		$age = time()-isvaliddate($opts{'m'}); 
		if ($age < 1) {
			print ("$opts{'m'} is in the future");
			exit (1);
		}
	} else {
		print ("Invalid max age $opts{'m'}");
		exit (1);
	}
} else {
	help();
}

if ($archive || $extract) {
	if ($#ARGV != 1) {
		print "You must specify two files when using -a (archive) or -x (extract)\n";
		exit (1);
	}
	
	$infile = $ARGV[0];
	$outfile = $ARGV[1];
} elsif (!$archive && !$extract) {
	if ($#ARGV != 0) {
		print "You must specify one file when purging messages\n";
		exit (1);
	}
	
	$infile = $outfile = $ARGV[0];
}

# Main
# ---------------------------------------------------------------------

# Read INFILE and separate each message into the @messages array.
open (INFILE, '<', $infile);
while (<INFILE>)
{
	if ($first_message) {
		if (/$fromline/) {
			$messages[$messagecount] .= $_;
			$first_message = 0;
			next;
		} else {
			die "$infile does not seem to be a valid mbox\n";
		}
	} elsif (/^From /) {
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

my %months = ("Jan", 0, "Feb", 1, "Mar", 2, "Apr", 3, "May", 4, "Jun", 5, "Jul", 6,
	"Aug", 7, "Sep", 8, "Oct", 9, "Nov", 10, "Dec", 11);

foreach (@messages) {
	m/Subject:\s+(.*)/;
	my $subject = $1;
	
	#print "$purge_count\n";

	if (/$fromline/) {
		# Convert month to numerical value.
		$month = $months{"$1"};
		$day = $2;
		@time = split (/:/, $3);
		$year = $4;
		$timestamp = timelocal("$time[2]","$time[1]","$time[0]",$day,$month,$year);
	}
	
	# Archive mode, old messages go into the OUTFILE and the 
	# newer are written back to the original file.
	if ($archive) {
		if ($current_time-$timestamp > $age) {
			print OUTFILE $_ if (!$dry_run);
			$purge_count++;
			print "Archived $subject\n" if ($verbose);
		} else {
			print ORIGINAL $_ if (!$dry_run);
		}
	} 
	# Extract mode, old messages are extracted and written to OUTFILE
	# original file is kept intact.
	elsif ($extract) {
		if ($current_time-$timestamp > $age) {
			print OUTFILE $_ if (!$dry_run);
			$purge_count++;
			print "Extracted $subject\n" if ($verbose);
		}			
	} 
	# Purge mode, old messages are purged, kept messages either go into
	# the original file or a new file.
	else {
		if ($current_time-$timestamp < $age) {
			if (!$dry_run) {
				print OUTFILE $_;
			}
		} else {
			$purge_count++;
			print "Purged $subject\n" if ($verbose);
		}
	}
}

flock ORIGINAL, 8 if (defined fileno ORIGINAL);
flock OUTFILE, 8 if (defined fileno ORIGINAL);
close ORIGINAL;
close OUTFILE;

print "$purge_count messages affected.\n";


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
	} else {
		return 0; # Not a date
	}
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
 -a        Archive mode, archive messages into the second
           mbox.
 -A        Append to the second mbox instead of overwriting it.
 -d        Dry run, run the program without actually writing to
           any files.
 -m NN     Maximum age of messages to keep in hours,
           days, weeks, months or years (e.g. 3d, 4y).
 -m <date> Keep messages newer than this date.
           (dates must in ISO format (YYYY-MM-DD)
 -v        Print the subjects of purged, archived or extracted
           messages.
 -x        Extract mode, extract messages into the second
           mbox. (Keeps the original mbox untouched).\n";
	
	exit (1);
}
