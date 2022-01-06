# run: prove -v 1.t
#
# Dependencies:
#   perl
#     Test::File::Contents
# To install:
#   
#   cpan App::cpanminus
#   # restart shell
#   cpanm Test::Files
# For more details:
#   http://www.cpan.org/modules/INSTALL.html

use 5.30.0;
use strict;
use warnings;

use Test::More tests => 15;
use Test::File::Contents;

my $EMAIL = "krobasky%40renci.org";
my $GROUPID = "time_series_2";
my $APIKEY = "apikey%7Cf486c6983c58c4a1f0339de75aa36692";

my $dl_taskid = "7e44cd57";
#my $dl_taskid = "9e9ffa3e";
my $analysis_taskid = "f94ce7e3-eb79-47d9-a24e-825453cdc9c3";
my $fn ;

my $do_dl = 0;
my $do_cellfie = 0;
    
print "dl enabled = ${do_dl}\n";
print "cellfie enabled = ${do_cellfie}\n";

`rm ./t/out/*`;

 SKIP: {
     skip "don't do new dl yet", 1 unless ${do_dl} >= 1;
     $fn="test1";
     files_eq(f($fn), g($fn,"immunespace/download?email=${EMAIL}&group=${GROUPID}&apikey=${APIKEY}"), "Download  group $GROUPID"); 
};


$fn = "test2";
files_eq(f($fn), g($fn, "immunespace/download/ids/${EMAIL}"),                            "Get download ids for ${EMAIL}");
$fn = "test3";
files_eq(f($fn), g($fn, "immunespace/download/status/${dl_taskid}"),                     "Get status for taskid ${dl_taskid}");
$fn = "test4";
files_eq(f($fn), g($fn, "immunespace/download/metadata/${dl_taskid}"),                   "Get metadata for taskid ${dl_taskid}");
$fn = "test5";
files_eq(f($fn), g($fn, "immunespace/download/results/${dl_taskid}/geneBySampleMatrix"), "Get expression data for taskid ${dl_taskid}");
$fn = "test6";
files_eq(f($fn), g($fn, "immunespace/download/results/${dl_taskid}/phenoDataMatrix"),    "Get phenotype data for taskid ${dl_taskid}");

 SKIP: {
     skip "don't do analysis yet", 1 unless ${do_cellfie} >= 1;
     files_eq(f("test7"), p("immunespace/cellfie/submit?immunespace_download_id=${dl_taskid}",
	 "ValueLow=5&PercentileOrValue=value&Value=5&Ref=MT_recon_2_2_entrez.mat&SampleNumber=32&ValueHigh=5&ThreshType=local&PercentileLow=25&LocalThresholdType=minmaxmean&Percentile=25&PercentileHigh=75"),
		      "WAIT [13min] Submit simple cellfie run with default parameters on download taskid ${dl_taskid}");
}

$fn = "test8";
files_eq(f($fn),  g($fn, "immunespace/cellfie/task_ids/${EMAIL}"),                        "Get cellfie job task_id's");
$fn = "test9";
files_eq(f($fn),  g($fn, "immunespace/cellfie/status/${analysis_taskid}"),                "Get cellfie job status");
$fn = "test10";
files_eq(f($fn), g($fn, "immunespace/cellfie/metadata/${analysis_taskid}"),              "Get cellfie job metadata");
$fn = "test11";
files_eq(f($fn), g($fn, "immunespace/cellfie/parameters/${analysis_taskid}"),            "Get cellfie run parameters");
$fn = "test12";
files_eq(f($fn), g($fn, "immunespace/cellfie/results/${analysis_taskid}/taskInfo"),      "Get cellfie results: taskInfo");
$fn = "test13";
files_eq(f($fn), g($fn, "immunespace/cellfie/results/${analysis_taskid}/score"),         "Get cellfie results: score");
$fn = "test14";
files_eq(f($fn), g($fn, "immunespace/cellfie/results/${analysis_taskid}/score_binary"),  "Get cellfie results: score_binary");
$fn = "test15";
files_eq(f($fn), g($fn, "immunespace/cellfie/results/${analysis_taskid}/detailScoring"), "Get cellfie results: detailScoring");
# xxx add:  delete expression data, delete cellfie results data
#file_contents_like ${file}, ${cmd}, ";


# support functions
sub g {
    my $cmd=sprintf("curl -X 'GET' 'http://localhost:8000/%s' -H 'accept: application/json'", $_[1]);
    `$cmd  2> /dev/null > ./t/out/$_[0].out`;
    print("$cmd\n");
    #return `${cmd}`;
    return "./t/out/$_[0].out";
}
sub p {
    my $cmd=sprintf("curl -X 'POST' 'http://localhost:8000/%s' -H 'accept: application/json' -d '%s'", $_[1], $_[2]);
    `$cmd 2> /dev/null > ./t/out/$_[0].out`;
    print("$cmd\n");
    return "./t/out/$_[0].out";
}
sub f {
    return "./t/expected/$_[0].out";
}
