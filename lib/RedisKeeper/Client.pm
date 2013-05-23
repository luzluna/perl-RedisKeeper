package RedisKeeper::Client;

our $VERSION = '0.01';

use strict;
use warnings;

use Try::Tiny;
use Net::ZooKeeper qw(:events :node_flags :acls);
use Redis;
use Params::Validate qw(:all);
use String::CRC32;

# TODO: timeout implementation
#  - if timeout is 0. if fail make exception
#  - if timeout is n sec. retry while n sec.

sub new {
	my $class = shift;

	my $p = validate(@_ , {
		zk_servers	=> { type => SCALAR, default => '' },
		zk_client_path	=> { type => SCALAR, default => '/redis/client' },
		zk_client_name	=> { type => SCALAR, default => 'client' },
		zk_server_path	=> { type => SCALAR, default => '/redis/servers/cluster' },
		timeout		=> { type => SCALAR, default => 5000 },
		data		=> { type => SCALAR, default => 0 },
		debug		=> { type => BOOLEAN, default => undef },
	});
	$p->{redis} = ();
	my $self = $p;
	bless $self, $class;


	# connect to zk
	$self->{zkh} = Net::ZooKeeper->new($self->{zk_servers});
	if ( !$self->{zkh} ) {
		return;
	}


	# if not created create path
	$self->_create_cyclic_path( $self->{zk_client_path} );

	my $client_tmpl = $self->{zk_client_path}. "/" . $self->{zk_client_name} . "-";
	$self->{client} = $self->{zkh}->create( $client_tmpl, $self->{data},
			      'flags' => (ZOO_EPHEMERAL | ZOO_SEQUENCE),
			       acl   => ZOO_OPEN_ACL_UNSAFE,
			      );

	# watch for change sharding configuration
	$self->{zk_watch} = $self->{zkh}->watch();

	$self->_refresh_redis();

	return $self;
}

sub _refresh_redis {
	my ($self, $path) = @_;

	my @childs = $self->{zkh}->get_children( $self->{zk_server_path}, 'watch' => $self->{zk_watch} );
	$self->{redis} = ();
	for my $id (@childs) {
		my $watch = $self->{zkh}->watch('timeout' => 0);
		my $redis_address = $self->{zkh}->get( $self->{zk_server_path}.'/'.$id, 'watch' => $watch );

		push( @{$self->{redis}} , {
			id    => $id,
			redis => Redis->new( server => $redis_address, encoding => undef ),
			address => $redis_address,
			watch => $watch,
		});
	}
}


sub _choose_server {
	my ($self, $key) = @_;

	# if sharding configuration changed
	if ( $self->{zk_watch}->wait('timeout'=>0) ) {
		print "cluster changed\n" if ($self->{debug});
		$self->_refresh_redis();
	}


	my $idx = crc32($key) % (scalar @{$self->{redis}});
	my $redis = $self->{redis}[$idx];

	if ( $redis->{watch}->wait('timeout'=>0) ) {
		# redis master node changed
		$self->_update_redis( $idx );
	}

	return $redis->{redis};
}
sub _choose_server_byid {
	my ($self, $id) = @_;
	# if sharding configuration changed
	if ( $self->{zk_watch}->wait('timeout'=>0) ) {
		print "cluster changed\n" if ($self->{debug});
		$self->_refresh_redis();
	}

	my $redis = $self->{redis}[$id];

	if ( $redis->{watch}->wait('timeout'=>0) ) {
		# redis master node changed
		$self->_update_redis( $id );
	}

	return $redis->{redis};
}
sub _update_redis {
	my ($self, $idx) = @_;
	my $redis = $self->{redis}[$idx];

	my $fail;
	for (1..5) {	# retry 5 times
		my $redis_address = $self->{zkh}->get( $self->{zk_server_path}.'/'.$redis->{id}, 'watch' => $redis->{watch} );
		try {
			$redis->{redis} = Redis->new( server => $redis_address, encoding => undef );
			$redis->{address} = $redis_address;
			$fail = undef;
		}
		catch {
			$fail = 1;
		};
		last if (!$fail);

		$redis->{watch}->wait('timeout'=>1000);
	}
	print "master changed($idx : $redis->{address})\n" if ($self->{debug} && !$fail);
	print "master change fail($idx : $redis->{address})\n" if ($self->{debug} && $fail);
}

### Deal with common, general case, Redis commands
our $AUTOLOAD;
sub AUTOLOAD {
	my $command = $AUTOLOAD;
	my $self = shift;
	my ($key) = @_;
	$command =~ s/.*://;

	my $r;
	if ($command =~ m/^(.*)_byid$/) {
		shift;
		$command = $1;
		$r = $self->_choose_server_byid($key);
	}
	elsif ($command =~ m/^(.*)_bykey$/) {
		shift;
		$command = $1;
		$r = $self->_choose_server($key);
	}
	else {
		$r = $self->_choose_server($key);
	}
	my $method = sub { $r->$command( @_ ) };

	goto $method;
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
			$self->{zkh}->create($current_path, '0',
			acl => ZOO_OPEN_ACL_UNSAFE
		);
		}
	}
}

sub DESTROY {
	local $@;

	my $self = shift;

	$self->{zkh} = undef;
	# TODO:zk release
}

1;

__END__

=pod

=head NAME

RedisKeeper - Zookeeper Managed Redis Cluster

=head1 VERSION

version 0.01

=head1 SYNOPSIS

not yet...

=head1 DESCRIPTION

Zookeeper Managed Redis Cluster



=head1 SEE ALSO

L<Redis>
L<Net::ZooKeeper>

=head1 ACKNOWLEDGEMENTS


=head1 AUTHOR

Dongsik Park <luzluna@gmail.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Dongsik Park.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
