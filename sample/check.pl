#!/usr/bin/env perl
use v5.10;
use Net::ZooKeeper qw/:node_flags :acls/;

my $servers = '172.17.4.107:2181,172.17.4.108:2181,172.17.4.109:2181';

my $zkh = Net::ZooKeeper->new($servers);

print "==/redis/client\n";
my @childs = $zkh->get_children('/redis/client');
for my $child (@childs) {
	print ref $child;
	print "$child\n";
}
print "==/redis/servers/cluster\n";
my @childs = $zkh->get_children('/redis/servers/cluster');
for my $child (@childs) {
	print ref $child;
	print "$child\n";
	my $watch = $zkh->watch('timeout' => 0);
	print " - ".$zkh->get('/redis/servers/cluster/'.$child, 'watch' => $watch), "\n";

=pod
	for(1..20) {
		print ".\n";
		if ($watch->wait('timeout' => 0)) {
		print "  event: $watch->{event}\n";
		print "  state: $watch->{state}\n";
		}
		sleep(1);
	}
=cut
}
print "==/redis/servers/cluster/0\n";
my @childs = $zkh->get_children('/redis/servers/cluster/0');
for my $child (@childs) {
	print ref $child;
	print "$child\n";
	print " - ".$zkh->get('/redis/servers/cluster/0/'.$child), "\n";
}
print "==/redis/servers/cluster/1\n";
my @childs = $zkh->get_children('/redis/servers/cluster/1');
for my $child (@childs) {
	print ref $child;
	print "$child\n";
	print " - ".$zkh->get('/redis/servers/cluster/1/'.$child), "\n";
}

#$zkh->delete('/redis/servers/cluster/0');
