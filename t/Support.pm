### support functions
package Support;

require Exporter;
@ISA = qw(Exporter);
our @EXPORT = qw($verbose $no_download $dry_run $EMAIL $GROUPID $APIKEY $dl_taskid $analysis_taskid 
		 cleanup f cmd generalize_output rawf dl_poll json_struct);

use Cpanel::JSON::XS;
use Switch;
use Carp qw(croak);

sub f {
    return "./t/expected/$_[0]";
}
sub rawf {  return "raw.$_[0]";}


sub cleanup {
    my $dch = "";
    if($dry_run != 0) { $dch="D";}
    if($verbose) {
	print("+$dch [cleanup] rm ./t/out/*\n");
	`rm -f ./t/out/*\n`;
    }
    # delete all the jobs associated with this tester email
    my $outfile = cmd("GET", "ids.json", "immunespace/download/ids/${EMAIL}");

    my $rm_ids;
    if($dry_run != 0) {
	print("+D [json_struct] dry_run, returning dry_run_id_1, dry_run_id_2\n"); 
	$rm_ids = [ { "immunespace_download_id" => "dry_run_id_1" },
		    { "immunespace_download_id" => "dry_run_id_2" } ];
    } else {
	$rm_ids= json_struct($outfile);
    }
    
    foreach my $rm_id (@$rm_ids) {
	cmd("DELETE", "cleanup.json", "immunespace/download/delete/" . $rm_id->{"immunespace_download_id"});
    }
}


sub cmd {
    my ($type, $fn, $endpoint, $post_args) = @_;
    
    my $cmd;
    my $myhost = "http://localhost:8000";
    switch($type) {
    	case "DELETE" { $cmd=sprintf("curl -X 'DELETE' '${myhost}/%s' -H 'accept: application/json'", $endpoint); }
	case "GET"    { $cmd=sprintf("curl -X 'GET'    '${myhost}/%s' -H 'accept: application/json'", $endpoint); }
	case "POST"   { $cmd=sprintf("curl -X 'POST'   '${myhost}/%s' -H 'accept: application/json' -d '%s'", $endpoint, $post_args);}
	else { print("+! [cmd] ERROR ${type} not recognized.\n");	}
    }

    my ($ext) = $fn =~ /(\.[^.]+)$/;
    my $outfile = "./t/out/${fn}";
    my $parse_json = "";
    if($ext eq ".json") {
	$parse_json = "| python -m json.tool | jq --sort-keys ";
    }
    my $cmd = ${cmd} . " 2> /dev/null $parse_json > $outfile";
    $dch = "";
    if($dry_run == 0) {
	`$cmd`; 
    } else {
	$dch = "D";
    }
    if($verbose == 1) {
	print("+$dch [cmd] ${cmd}\n");
	print("+$dch [cmd] OUTFILE($ext)=(${outfile})\n");
	if($dry_run == 0) {
	    if($ext eq ".json"){
		print(`python -m json.tool ${outfile}` . "\n");
	    } else {
		print (`awk -F, 'NR<3{print \$1\",\"\$2\",\"\$3,\"...\"}' ${outfile}` ."\n");
	    }
	}
    }
    if($dry_run == 0 ) {
	return ${outfile};
    } else {
	# this is a dry run, return the expected output since an output file isn't generated
	return "./t/expected/${fn}";
    }
}

sub generalize_output {
    # remove ephemeral info from meatadata for comparison
    my ($fn, $raw_outfile, $fields_ref) = @_;
    $json = json_struct($raw_outfile);
    foreach my $field (@$fields_ref) {
	$json->{$field} = "xxx";
    }
    my $fn_raw = rawf($fn);
    open $fh_raw, ">", $raw_outfile;
    print $fh_raw encode_json($json);
    close $fh_raw;
    `cat $raw_outfile |python -m json.tool|jq --sort-keys > t/out/${fn}`;
}

sub dl_poll {
    if($dry_run) {
	if($verbose){
	    print("+D [poll]: dry_run polling done\n");
	}
	return "finished";
    }
    my $job_id = $_[0];
    my $max_retries = $_[1];
    my $retry_num = 0;
    my $status = "";
    while($status ne 'finished' && $status ne 'failed' && $retry_num < $max_retries){
	my $outfile = cmd("GET", 'status.json', "immunespace/download/status/${dl_taskid}");
	$status = json_struct($outfile)->{'status'};
	$retry_num++;
	if($verbose){
	    print "+ [poll]: status=$status, retry=$retry_num\n";
	}
	sleep 5;
    }
    if($verbose){
	print("+ [poll]: FINAL download status = $status\n");
    }
    return($status);
    # {"status":"finished"}%  
}

sub json_struct {
    my $outfile = $_[0];
    my $json_text = do {
	open(my $json_fh, "<:encoding(UTF-8)", $outfile)
	    or die("Can't open \"$outfile\": $!\n");
	local $/;
	<$json_fh>
    };
    return decode_json($json_text);
}

1;
