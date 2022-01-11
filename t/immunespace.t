#
#   prove -v t/immunespace.t :: [--verbose] [--no_download] [--dry_run]
#
# Assumes containers are built and running (use up.sh)
# Represents a typical use case for download a dataset from immunespace using a predefined groupid ($GROUPID)
# --no_download if you want to create the download outside of the test because you don't want to wait 
#
# Dependencies:
#   perl
#     Test::File::Contents
# To install:
#   
#   cpan App::cpanminus
#   # restart shell
#   cpanm Test::Files
#   cpanm Cpanel::JSON::XS
# For more details:
#   http://www.cpan.org/modules/INSTALL.html

use 5.30.0;
use strict;
use warnings;

use Getopt::Long qw(GetOptions);

use Test::More tests => 9;
use Test::File::Contents;

use lib './t';
use Support;

our $verbose = 0;
our $no_download = 0;
our $dry_run = 0;

our $EMAIL = "tester%40renci.org"; 
our $GROUPID = "test-SDY61-9"; 
our $APIKEY = "apikey%7Cf486c6983c58c4a1f0339de75aa36692";

our $dl_taskid = "test-immunespace-1";
our $analysis_taskid = "f94ce7e3-eb79-47d9-a24e-825453cdc9c3";

GetOptions('no_download' => \$no_download,
	   'dry_run' => \$dry_run,
	   'verbose' => \$verbose) or die "Usage: $0 --verbose --no_download\n";
if($verbose){
    print("+ no_download: $no_download\n");
    print("+ dry_run: $dry_run\n");
    print("+ verbose: $verbose\n");
}

#our @EXPORT = qw($verbose $no_download $dry_run $EMAL $GROUPID $APIKEY $dl_taskid $analysis_taskid);
#use vars qw($verbose $no_download $dry_run $EMAIL $GROUPID $APIKEY $dl_taskid $analysis_taskid); 



my $fn ;

if($no_download != 0){
    print "+ DOWNLOAD DISABLED\n";
}

cleanup();

 SKIP: {
     skip "Downloaded for $EMAIL,$GROUPID,$APIKEY,$dl_taskid created out of loop, don't execute download and wait", 1 unless ${no_download} == 0;

     $fn = "immunespace-1.json";
     files_eq(f($fn), p($fn,"immunespace/download?email=${EMAIL}&group=${GROUPID}&apikey=${APIKEY}&requested_id=${dl_taskid}", ""),   "Download group $GROUPID, taskid ${dl_taskid}");

     if($verbose){
	 print("+ Wait for download...\n");
     }
     my $status = dl_poll($dl_taskid, 15);
     is($status, "finished", "$dl_taskid downloaded with status $status");
}

$fn = "immunespace-2.json";
files_eq(f($fn), g($fn, "immunespace/download/ids/${EMAIL}"),                                                                    "Get download ids for ${EMAIL}");
$fn = "immunespace-3.json";
files_eq(f($fn), g($fn, "immunespace/download/status/${dl_taskid}"),                                                             "Get status for taskid ${dl_taskid}");
 SKIP: {
     skip "don't do metadata yet", 1 ;
     $fn = "immunespace-4.json";
     files_eq(f($fn), g($fn, "immunespace/download/metadata/${dl_taskid}"),                                                           "Get metadata for taskid ${dl_taskid}");
}
$fn = "immunespace-5.csv";
files_eq(f($fn), g($fn, "immunespace/download/results/${dl_taskid}/geneBySampleMatrix"),                                         "Get expression data for taskid ${dl_taskid}");
$fn = "immunespace-6.csv";
files_eq(f($fn), g($fn, "immunespace/download/results/${dl_taskid}/phenoDataMatrix"),                                            "Get phenotype data for taskid ${dl_taskid}");
# In a typical use case, never delete data; see documentation
$fn = "immunespace-7.json";
files_eq(f($fn), d($fn, "immunespace/download/delete/${dl_taskid}"),                                                             "Delete a the downloaded groupid (status=deleted)");
$fn = "immunespace-8.json";
files_eq(f($fn), d($fn, "immunespace/download/delete/${dl_taskid}"),                                                             "Delete a the downloaded groupid (status=exception)");

