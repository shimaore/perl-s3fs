#!/usr/bin/perl
# s3fs.pl -- Amazon S3 based FUSE filesystem
#    Copyright (C) 2009 Stephane Alnet
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
use strict; use warnings;

use Amazon::S3;
use Fuse;
use Fcntl ':mode';
use POSIX qw(ENOENT);

our $bucket = undef;

sub main {
  my ( $bucket_name, $mountpoint ) = @_;

  my $secret_file = $ENV{HOME}.'/.s3fs/.secret';
  open(my $fh, $secret_file) or die "$secret_file: $!";
  my $aws_access_key_id = <$fh>;
  chomp $aws_access_key_id;
  my $aws_secret_access_key = <$fh>;
  chomp $aws_secret_access_key;
  close($fh);

  my $s3 = new Amazon::S3 (
    {
      aws_access_key_id => $aws_access_key_id,
      aws_secret_access_key => $aws_secret_access_key,
      retry => 2,
      secure => 1,
    }
  );

  $bucket = $s3->bucket($bucket_name);

  my @ops =
    map { $_ => "main::s3fs_$_" } qw (
      getattr readlink getdir mknod mkdir unlink rmdir symlink rename
      link chmod chown truncate utime open read write statfs
      flush release fsync setxattr getxattr listxattr removexattr
    );

  Fuse::main (
    debug => 1,
    mountpoint => $mountpoint,
    mountopts => "default_permissions",
    threaded => 0,
    @ops
  );
}

our %write_cache = ();

our %node_cache = ();

sub s3fs_getattr {
  my ($fn) = (@_);
  print STDERR "getattr $fn\n";
  $fn =~ s{^/}{};

  my $now = time();

  # Root directory
  if($fn eq '')
  {
    my @r = (
      1,              # dev
      2,              # ino
      S_IRWXU | S_IRWXG | S_IRWXO | S_IFDIR,           # mode
      1,              # nlink
      0,              # uid
      0,              # gid
      0,              # rdev
      4,              # size
      $now,           # atime
      $now,           # mtime
      $now,           # ctime
      4096,           # blksize
      1,              # blocks
    );
    return @r;
  }

  # Other files or directories
  my $hk = exists($node_cache{$fn}) ? $node_cache{$fn} : $bucket->head_key($fn);
  return -ENOENT() unless defined $hk;
  print STDERR join(', ', map { "$_ => $hk->{$_}" } keys %{$hk}),"\n";
  my @r = (
    1,              # dev
    2,              # ino
    $hk->{'x-amz-meta-s3fs-mode'} || S_IRWXU | S_IRWXG | S_IRWXO | S_IFREG,  # mode
    1,              # nlink
    0,              # uid
    0,              # gid
    0,              # rdev
    $hk->{content_length},    # size
    $hk->{'x-amz-meta-s3fs-mtime'}||$now,   # atime
    $hk->{'x-amz-meta-s3fs-mtime'}||$now,   # mtime
    $hk->{'x-amz-meta-s3fs-mtime'}||$now,   # ctime
    4096,             # blksize
    ($hk->{content_length})/4096, # blocks
  );
  return @r;
}

sub s3fs_readlink {
  return -1011;
}

sub s3fs_getdir {
  my ($dir) = (@_);
  print STDERR "getdir $dir\n";
  $dir =~ s{^/}{};
  my $r = $bucket->list_all({
    delimiter => '/',
    prefix => $dir,
  });
  return (0) unless defined $r;
  my @r = map { $_->{key} } @{$r->{keys}};
  return (@r,0);
}

sub s3fs_mknod {
  my ($fn,$mode,$dev) = @_;
  print STDERR "mknod $fn $mode $dev\n";
  $fn =~ s{^/}{};
  my $configuration = {};
  $configuration->{acl_short} = 'private';
  # $configuration->{'x-amz-meta-s3fs-mode'} = $mode;
  $configuration->{'x-amz-meta-s3fs-mode'} = S_IRWXU | S_IRWXG | S_IRWXO | S_IFREG;
  $configuration->{'x-amz-meta-s3fs-mtime'} = time();
  $configuration->{content_length} = 0;
  $node_cache{$fn} = $configuration;
  return 0;
}

sub s3fs_mkdir {
  my ($dir,$mode) = @_;
  $dir =~ s{^/}{};
  my $configuration = {};
  $configuration->{acl_short} = 'private';
  $configuration->{'x-amz-meta-s3fs-mode'} = S_IRWXU | S_IRWXG | S_IRWXO | S_IFDIR;
  $configuration->{'x-amz-meta-s3fs-mtime'} = time();
  $configuration->{content_length} = 0;
  my $r = $bucket->add_key($dir,'',$configuration);
  return $r ? 0 : -ENOENT();
}

sub s3fs_unlink {
  my ($fn) = @_;
  $fn =~ s{^/}{};
  my $r = $bucket->delete_key($fn);
  delete $write_cache{$fn};
  delete $node_cache{$fn};
  return $r ? 0 : -ENOENT();
}

sub s3fs_rmdir {
  my ($dir) = @_;
  $dir =~ s{^/}{};
  my $r = $bucket->delete_key($dir);
  delete $write_cache{$dir};
  delete $node_cache{$dir};
  return $r ? 0 : -ENOENT();
}
sub s3fs_symlink {
  return -1009;
}

sub s3fs_rename {
  my ($ofn,$fn) = @_;
  return 0;
}

sub s3fs_link {
  return -1008;
}

sub s3fs_chmod {
  return 0;
}

sub s3fs_chown {
  return 0;
}

sub s3fs_truncate {
  return -1007;
}

sub s3fs_utime {
  my ($fn,$atime,$mtime) = @_;
  return 0;
}
sub s3fs_open {
  my ($fn,$flags) = @_;
  return 0;
}
sub s3fs_read {
  my ($fn,$size,$offset) = @_;
  $fn =~ s{^/}{};
  if(exists($write_cache{$fn}))
  {
    return substr($write_cache{$fn}->{value},$offset,$size);
  }
  my $range = 'bytes='.$offset.'-'.($offset+$size);
  my $r = $bucket->get_key_with_headers($fn,undef,undef,{Range=> $range});
  return -ENOENT() unless defined $r;
  return $r->{value};
}
sub s3fs_write {
  my ($fn,$buffer,$offset) = @_;
  $fn =~ s{^/}{};
  print STDERR "write $fn '$buffer' $offset\n";
  $write_cache{$fn} = $bucket->get_key($fn)
    if not exists $write_cache{$fn};
  my $return = 0;
  if(defined $buffer)
  {
    $return = length($buffer);
    my $value = $write_cache{$fn}->{value};
    $value = '' if not defined $value;
    substr($value,$offset||0,length($buffer),$buffer);
    $node_cache{$fn}->{content_length} = length($value);
    print STDERR "value=$value\n";
    $write_cache{$fn}->{value} = $value;
  }
  return $return;
}
sub s3fs_statfs {
  return (4096,10000,10000,10000,10000,4096);
}

sub s3fs_flush {
  my ($fn) = @_;
  $fn =~ s{^/}{};
  if(exists($write_cache{$fn}))
  {
    my $configuration = $node_cache{$fn};
    $configuration->{acl_short} = 'private';
    $configuration->{'x-amz-meta-s3fs-mtime'} = time();
    my $r = $bucket->add_key($fn,$write_cache{$fn}->{value},$configuration);
    delete $write_cache{$fn};
    delete $node_cache{$fn};
    return $r ? 0 : -ENOENT();
  }
  return 0;
}
sub s3fs_release {
  my ($fn,$flags) = @_;
  return 0;
}
sub s3fs_fsync {
  my ($fn,$flags) = @_;
  return 0;
}
sub s3fs_setxattr {
  return -1002;
}
sub s3fs_getxattr {
  return -1003;
}
sub s3fs_listxattr {
  return -1004;
}
sub s3fs_removexattr {
  return -1005;
}

main(@ARGV);

package Amazon::S3::Bucket;

# Modified version of "get_key"

sub get_key_with_headers {
    my ($self, $key, $method, $filename, $headers) = @_;
    $method ||= "GET";
    $filename = $$filename if ref $filename;
    my $acct = $self->account;
    $headers = {} if not defined $headers;

    my $request = $acct->_make_request($method, $self->_uri($key), $headers);
    my $response = $acct->_do_http($request, $filename);

    if ($response->code == 404) {
        return undef;
    }

    $acct->_croak_if_response_error($response);

    my $etag = $response->header('ETag');
    if ($etag) {
        $etag =~ s/^"//;
        $etag =~ s/"$//;
    }

    my $return = {
                  content_length => $response->content_length || 0,
                  content_type   => $response->content_type,
                  etag           => $etag,
                  value          => $response->content,
    };

    foreach my $header ($response->headers->header_field_names) {
        next unless $header =~ /x-amz-meta-/i;
        $return->{lc $header} = $response->header($header);
    }

    return $return;

}
