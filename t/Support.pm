### support functions
package Support;

require Exporter;
@ISA = qw(Exporter);
our @EXPORT = qw($verbose $no_download $dry_run $EMAIL $GROUPID $APIKEY $dl_taskid $analysis_taskid 
		 cleanup f d g p dl_poll json_struct);

use Cpanel::JSON::XS;

sub cleanup {
    my $dch = "";
    if($dry_run != 0) { $dch="D";}
    if($verbose) {
	print("+$dch [cleanup] rm ./t/out/*\n");
	`rm -f ./t/out/*\n`;
    }
    # delete all the jobs associated with this tester email
    my $outfile = g("ids.json", "immunespace/download/ids/${EMAIL}");

    my $rm_ids;
    if($dry_run != 0) {
	print("+D [json_struct] dry_run, returning dry_run_id_1, dry_run_id_2\n"); 
	$rm_ids = [ { "immunespace_download_id" => "dry_run_id_1" },
		    { "immunespace_download_id" => "dry_run_id_2" } ];
    } else {
	$rm_ids= json_struct($outfile);
    }
    
    foreach my $rm_id (@$rm_ids) {
	d("cleanup.json", "immunespace/download/delete/" . $rm_id->{"immunespace_download_id"});
    }
}

sub f {
    return "./t/expected/$_[0]";
}
sub d {
    my $cmd=sprintf("curl -X 'DELETE' 'http://localhost:8000/%s' -H 'accept: application/json'", $_[1]);
    my $dch = "";
    if($dry_run != 0) { $dch = "D";}
    return exec_cmd($_[0], $cmd);
}
sub g {
    my $cmd=sprintf("curl -X 'GET' 'http://localhost:8000/%s' -H 'accept: application/json'", $_[1]);
    return exec_cmd($_[0], $cmd);
}
sub p {
    # POST commands always returns JSON in this application, don't take an extension argument
    my $cmd=sprintf("curl -X 'POST' 'http://localhost:8000/%s' -H 'accept: application/json' -d '%s'", $_[1], $_[2]);
    my $dch = "";
    if($dry_run != 0) { $dch = "D";}
    return exec_cmd($_[0], $cmd);
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
	my $outfile = g('status.json', "immunespace/download/status/${dl_taskid}");
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

sub exec_cmd {
    my ($ext) = $_[0] =~ /(\.[^.]+)$/;
    my $outfile = "./t/out/$_[0]";
    my $cmd = $_[1] . " > $outfile 2> /dev/null";
    $dch = "";
    if($dry_run == 0) {
	`$cmd`; 
    } else {
	$dch = "D";
    }
    if($verbose == 1) {
	print("+$dch [exec_cmd] $cmd\n");
	print("+$dch [exec_cmd] OUTFILE($ext)=(${outfile})\n");
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
	return "./t/expected/$_[0]";
    }
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
