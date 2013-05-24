package RedisKeeper::Server;

use 5.008;
use strict;
use warnings FATAL => 'all';

=head1 NAME

RedisKeeper::Server - Redis Server Manager via Zookeeper

=head1 VERSION

version 0.01
=cut

our $VERSION = '0.01';

use strict;
use warnings;

use Try::Tiny;
use Net::ZooKeeper qw(:events :node_flags :acls);
use Redis;
use Params::Validate qw(:all);

sub new {
	my $class = shift;

	my $p = validate(@_ , {
		zk_servers	=> { type => SCALAR, default => '' },
		zk_server_path	=> { type => SCALAR, default => '/redis/server' },
		redis_server	=> { type => SCALAR, default => '127.0.0.1:6378' },
		sharded_id	=> { type => SCALAR, default => 0 },
		data		=> { type => SCALAR, default => 0 },
		debug		=> { type => BOOLEAN, default => undef },
	});
	my $self = $p;
	bless $self, $class;


	# connect to zk
	$self->{zkh} = Net::ZooKeeper->new($self->{zk_servers});
	if ( !$self->{zkh} ) {
		return;
	}
	$self->{zk_watch} = undef;

	# if not created create path
	my $path_tmpl = $self->{zk_server_path}.'/cluster/'.$self->{sharded_id};
	$self->_create_cyclic_path( $path_tmpl );

	# check already running manager is exists.
	for my $child ( $self->{zkh}->get_children( $path_tmpl ) ) {
		if ( $self->{zkh}->get( $path_tmpl."/$child" ) eq $self->{redis_server} ) {
			print "already running manager is exists\n" if $self->{debug};
			return;
		}
	}
	return $self;
}


sub run {
	my $self = shift;

	while (1) {
		try {
			$self->{redis} = Redis->new( server => $self->{redis_server}, encoding => undef );
			$self->_in_service();
		} catch {
			# if redis is not avaliable sleep 2sec then retry
			print "redis fail(".$self->{redis_server}.")\n" if $self->{debug};
			$self->{redis} = undef;
		};
		if ( !$self->{redis} ) {
			$self->_out_of_service();
			print "redis failed.\n" if $self->{debug};
			sleep(2);
			next;
		}

		print "in service... \n" if $self->{debug};

		# liveness check
		while (1) {
			my $ret = ' ';
			try {$ret = $self->{redis}->ping;} catch {};
			if (!defined $ret || $ret ne 'PONG') {
				print "redis ping fail\n" if $self->{debug};
				last;
			}

			if ($self->{zk_watch}) {
				# Slave
				$self->{zk_watch}->wait('timeout' => 2000);
				if ( $self->{zk_watch}->{event} ) {
					print "watch event ".$self->{zk_watch}->{event}."\n" if $self->{debug};
					try {
						$self->_election();
					} catch {
						$self->{redis} = undef;
					};
					if ( !$self->{redis} ) {
						last; #exit ping loop
					}
				}
			}
			else {
				# Master
				sleep(1);
			}
		}

		# redis dead
		$self->_out_of_service();
	}
}

sub _in_service {
	my ($self) = @_;

	# Leader Election Start
	# 1. create node with ZOO_EPHEMERAL | ZOO_SEQUENCE
	my $ephemeral_node = $self->{zkh}->create(
		$self->{zk_server_path}.'/cluster/'.$self->{sharded_id}.'/',
		$self->{redis_server},
		'flags' => (ZOO_EPHEMERAL | ZOO_SEQUENCE),
		acl   => ZOO_OPEN_ACL_UNSAFE,
		);
	$self->{ephemeral_node} = $ephemeral_node;
	$ephemeral_node =~ s/^.*\/(\d+)$/$1/;

	$self->_election();
}

sub _election {
	my ($self) = @_;
	my $ephemeral_node = $self->{ephemeral_node};
	$ephemeral_node =~ s/^.*\/(\d+)$/$1/;

	# 2. election
	# TODO: overflow check
	#  if ephemeral_node value is over 2147483647 then -2147483647...
	my @childs = sort $self->{zkh}->get_children( $self->{zk_server_path}.'/cluster/'.$self->{sharded_id} );
	my $prev_seq = $childs[0];
	for my $seq ( @childs ) {
		if ( $seq >= $ephemeral_node ) {
			last;
		}
		$prev_seq = $seq;
	}

	if ( $prev_seq == $ephemeral_node ) {
		# Master Case
		# no smaller than me => i'm leader
		$self->_be_master();

		print "Master:$ephemeral_node\n" if $self->{debug};
	}
	else {
		# Slave Case
		$self->_be_slave($prev_seq);

		print "Slave:$ephemeral_node\n" if $self->{debug};
	}

}

sub _be_master {
	my ($self) = @_;

	# reset watch
	$self->{zk_watch} = undef;

	# make master
	$self->{redis}->slaveof("NO", "ONE");

	# set master info to zookeeper
	my $ret = $self->{zkh}->set( $self->{zk_server_path}.'/cluster/'.$self->{sharded_id}, $self->{redis_server} );
	if ( !$ret ) {
		print "zkCritcal Error: ".$self->{zkh}->get_error()."\n";
	}
	print $self->{zk_server_path}.'/cluster/'.$self->{sharded_id}." ".$self->{redis_server}."\n";
	print $self->{zkh}->get($self->{zk_server_path}.'/cluster/'.$self->{sharded_id})."\n";
}

sub _be_slave {
	my ($self, $prev_seq) = @_;
	# make new watch
	if ( !$self->{zk_watch} ) {
		$self->{zk_watch} = $self->{zkh}->watch('timeout' => 2000);
	}

	# make slave
	my $tmp = $self->{zkh}->get( $self->{zk_server_path}.'/cluster/'.$self->{sharded_id} );
	my ($host,$ip) = split ':', $tmp;
	$self->{redis}->slaveof($host, $ip);


	$self->{zkh}->exists( $self->{zk_server_path}.'/cluster/'.$self->{sharded_id}.'/'.$prev_seq, 'watch' => $self->{zk_watch} );
}

sub _create_cyclic_path {
	my ($self, $path) = @_;

	my $current_index = 1;
	while ($current_index > 0) {
		$current_index = index($path, "/", $current_index + 1);
		my $current_path;
		if ($current_index > 0) {
			$current_path = substr($path, 0, $current_index);
		} else {
			$current_path = $path;
		}

		if (!$self->{zkh}->exists($current_path)) {
			$self->{zkh}->create($current_path,
				'0',
				acl => ZOO_OPEN_ACL_UNSAFE
			);
		}
	}
}

sub _out_of_service {
	my ($self) = @_;

	if ( $self->{ephemeral_node} ) {
		if (!$self->{zk_watch}) {
			# if Master
			$self->{zkh}->set( $self->{zk_server_path}.'/cluster/'.$self->{sharded_id}, '0' );
		}
		$self->{zkh}->delete( $self->{ephemeral_node} );
		$self->{ephemeral_node} = undef;
	}
}


sub DESTROY {
	my $self = shift;

	$self->_out_of_service();
	$self->{zkh} = undef;

	print "RedisKeeper Destroy\n" if $self->{debug};
	# TODO:zk release
}

1;

__END__

=head1 SYNOPSIS

Redis Server Manager

=head1 DESCRIPTION

Zookeeper Managed Redis Cluster

=head1 SUBROUTINES/METHODS

=over

=item new
  new()
  - zk_servers : zookeeper server list (ex. 127.0.0.1:2181,127.0.0.1:2182)
  - zk_server_path : default is /redis/server
  - redis_server : redis server ip:port
  - sharded_id : sharding id
  - debug : boolean
=cut
=item run
  main loop
=cut
=item DESTROY
  zookeeper ephemeral node destroy for fast failover.
=cut

=back

=head1 SEE ALSO

L<Redis>
L<Net::ZooKeeper>

=head1 AUTHOR

Dongsik Park, C<< <luzluna at gmail.com> >>



=head1 BUGS

=head1 SUPPORT

=head1 ACKNOWLEDGEMENTS

=head1 LICENSE AND COPYRIGHT

Copyright 2013 Dongsik Park.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

=cut


