#!/usr/bin/env perl
use v5.10;
use lib 'lib';
use RedisCluster::Client;
use Data::Dump qw/pp/;

my $servers = '172.17.4.107:2181,172.17.4.108:2181,172.17.4.109:2181';

my $client = RedisCluster::Client->new({
		zk_servers => $servers,
		debug => 1,
	});

$client->hset("myhash", "field1", "Hello");
$client->hset("myhash", "field2", "World");
my $tmp = $client->hkeys("myhash");
pp( $tmp );
say "##".$client->get_byid(0, "key1");
say "##".$client->get_bykey("key1", "key1");

for (1..5) {
	$client->set("key".$_, "value".$_);
}
sleep(1);
for (1..20) {
	my $val = $client->get("key".$_);
	if (!$val) {
		print "not fount : key$_\n";
		next;
	}
	print "$val\n";
	sleep(1);
}
$client->set('key' => 'value');

