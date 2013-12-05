#!/usr/bin/env perl
use v5.10;
use lib 'lib';
use lib 'Net-ZooKeeper-0.35';
use RedisCluster::Server;

use YAML;
use Getopt::Long::Descriptive;


my ($opt, $usage) = describe_options(
	'server %o <some-arg>',
	['config|f=s',	"config yaml file"],
	['server|s=s',	"redis server (ex. 172.0.0.1:9728)"],
	['id|i=i',	"shard id"],
	['verbose|v',	"print extra stuff"],
	['help',	"print usage message and exit" ],
);

print($usage->text), exit if $opt->help;


my $cfg_file = $opt->config || "redis.yaml";
my $cfg = YAML::LoadFile($cfg_file);
$0 = 'zkredis_server_manager '.$cfg_file;

my $servers = join(',', @{$cfg->{zookeeper}});
$cfg->{sharded_id} = $opt->id if $opt->id;
if ($opt->server) {
	($cfg->{redis}->{ip}, $cfg->{redis}->{port}) = split(':', $opt->server );
}

print "zk: $servers\n" if $opt->verbose;
print "redis: ".$cfg->{redis}->{ip}.':'.$cfg->{redis}->{port}."\n" if $opt->verbose;
print "id: ".$cfg->{sharded_id}."\n" if $opt->verbose;

my $server = RedisCluster::Server->new({
		zk_servers => $servers,
		zk_server_path => $cfg->{zookeeper_server_path},
		redis_server => $cfg->{redis}->{ip}.':'.$cfg->{redis}->{port},
		sharded_id => $cfg->{sharded_id},
		debug => $opt->verbose,
	});

sub exit_handler {
	$server->DESTROY() if $server;
	print "DIE...\n" if $opt->verbose;
	exit(0);
}
$SIG{INT} = \&exit_handler;
$SIG{TERM} = \&exit_handler;
$SIG{HUP} = \&exit_handler;

$server->run() if $server;

1;
