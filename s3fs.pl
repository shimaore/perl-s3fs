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
use Fuse qw(fuse_get_context);
use Fcntl qw(:mode);
use POSIX qw(ENOENT EIO);

our $bucket = undef;

=pod
  The write_cache is indexed on the filename.
  It stores the content of the file while it is been written to.
=cut

our %node_cache = ();

use constant SECRET_FILE => $ENV{HOME}.'/.s3fs/.secret';

use constant S3_SECURE_ACCESS => 1;
use constant S3_RETRY => 2;
use constant S3_TIMEOUT => 7;

# Some of these are MacFUSE extensions.
# See http://code.google.com/p/macfuse/wiki/OPTIONS
# Could use auto_xattr,daemon_timeout=240
use constant FUSE_MOUNTOPTS => "default_permissions";

use constant DEBUG => 1;

sub _debug {
  print STDERR $ARGV[0].': '.join('',@_)."\n" if DEBUG;
}

sub get_secret {
  my $secret_file = shift;
  open(my $fh, $secret_file) or die "$secret_file: $!";
  my $aws_access_key_id = <$fh>;
  chomp $aws_access_key_id;
  my $aws_secret_access_key = <$fh>;
  chomp $aws_secret_access_key;
  close($fh);
  return ($aws_access_key_id,$aws_secret_access_key);
}

our $s3_params = undef;
our $s3_bucket_name = undef;
our $s3_cache  = undef;

sub main {
  my ( $bucket_name, $mountpoint, $cache ) = @_;

  my ($aws_access_key_id,$aws_secret_access_key) = get_secret(SECRET_FILE);

  $s3_params = {
    aws_access_key_id     => $aws_access_key_id,
    aws_secret_access_key => $aws_secret_access_key,
    secure    => S3_SECURE_ACCESS,
    retry     => S3_RETRY,
    timeout   => S3_TIMEOUT,
  };
  $s3_bucket_name = $bucket_name;
  $s3_cache       = $cache;

  if(fork())
  {
    close(STDIN);
    my $s3 = new Amazon::S3 ( $s3_params );
    $bucket = $s3->bucket($s3_bucket_name);

    start_daemon();
  }
  else
  {
    my $s3 = new Amazon::S3 ( $s3_params );
    $bucket = $s3->bucket($s3_bucket_name);

    start_fuse($mountpoint);

    # Tell the daemon to quit.
    _debug("Asking the daemon to quit.");
    my $fh;
    open($fh,'>',"${s3_cache}/.quit") && close($fh);
    wait();
  }
}

sub start_fuse {
  my ($mountpoint) = @_;
  my @ops =
    map { $_ => "main::s3fs_$_" } qw (
      getattr readlink getdir mknod mkdir unlink rmdir symlink rename
      link chmod chown truncate utime open read write statfs
      flush release fsync setxattr getxattr listxattr removexattr
    );

  my $mountopts = FUSE_MOUNTOPTS;
  $mountopts .= ($mountopts && ',') . "fsname=s3fs:${s3_bucket_name}";

  _debug('Starting Fuse::main');
  Fuse::main (
    debug => 1,
    mountpoint => $mountpoint,
    mountopts => $mountopts,
    threaded => 0,
    @ops
  );
}

use constant S_DEFAULT_REG => S_IRUSR | S_IWUSR | S_IRGRP | S_IFREG;
use constant S_DEFAULT_DIR => S_IRWXU | S_IRGRP | S_IXGRP | S_IFDIR;

sub s3fs_getattr {
  my ($fn) = (@_);
  _debug("getattr $fn");
  $fn =~ s{^/}{};

  my $now = time();

  my $fuse_context = fuse_get_context();

  # Root directory
  if($fn eq '')
  {
    my @r = (
      1,              # dev
      2,              # ino
      S_DEFAULT_DIR,           # mode
      1,              # nlink
      $fuse_context->{uid},   # uid
      $fuse_context->{gid},   # gid
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
  my $hk;
  eval {
    $hk = exists($node_cache{$fn}) ? $node_cache{$fn} : $bucket->head_key($fn);
  };
  _debug("s3fs_getattr/head_key: $@");
  return -EIO() if $@;
  return -ENOENT() unless defined $hk;
  _debug(join(', ', map { "$_ => $hk->{$_}" } keys %{$hk}));
  my @r = (
    1,              # dev
    2,              # ino
    $hk->{'x-amz-meta-s3fs-mode'} || S_DEFAULT_REG,  # mode
    1,              # nlink
    $fuse_context->{uid},   # uid
    $fuse_context->{gid},   # gid
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
  _debug("getdir $dir");
  $dir =~ s{^/}{};
  my $r;
  eval {
    $r = $bucket->list_all({
      delimiter => '/',
      prefix => $dir,
    });
  };
  _debug("s3fs_getdir/list_all: $@");
  return -EIO() if $@;
  return (0) unless defined $r;
  my @r = map { $_->{key} } @{$r->{keys}};
  return (@r,0);
}

sub s3fs_mknod {
  my ($fn,$mode,$dev) = @_;
  _debug("mknod $fn $mode $dev");
  $fn =~ s{^/}{};
  my $configuration = {};
  $configuration->{acl_short} = 'private';
  # $configuration->{'x-amz-meta-s3fs-mode'} = $mode;
  $configuration->{'x-amz-meta-s3fs-mode'} = S_DEFAULT_REG;
  $configuration->{'x-amz-meta-s3fs-mtime'} = time();
  $configuration->{content_length} = 0;
  $node_cache{$fn} = $configuration;
  return 0;
}

=pod

  mkdir
  rmdir
    Use S3 directly.
    
=cut

sub s3fs_mkdir {
  my ($dir,$mode) = @_;
  $dir =~ s{^/}{};
  my $configuration = {};
  $configuration->{acl_short} = 'private';
  $configuration->{'x-amz-meta-s3fs-mode'} = S_DEFAULT_DIR;
  $configuration->{'x-amz-meta-s3fs-mtime'} = time();
  $configuration->{content_length} = 0;
  my $r;
  eval {
    $r = $bucket->add_key($dir,'',$configuration);
  };
  _debug("s3fs_mkdir/add_key: $@",$bucket->errstr);
  return -EIO() if $@;
  return $r ? 0 : -ENOENT();
}

sub s3fs_rmdir {
  my ($dir) = @_;
  $dir =~ s{^/}{};
  delete $node_cache{$dir};
  my $r;
  eval {
    $r = $bucket->delete_key($dir);
  };
  _debug("s3fs_rmdir/delete_key: $@");
  return -EIO() if $@;
  return $r ? 0 : -ENOENT();
}

=pod

  Write Cache management
    Note: this is only the "write" side of the cache.
    There is a daemon running to actually upload the files
    to S3. The daemon only upload files which are present and
    have a metafile.

=cut

sub _write_cache
{
  my ($fn) = @_;
  $fn =~ s/[^\w]/_/g;
  return "${s3_cache}/${s3_bucket_name},${fn}";
}

sub _write_cache_meta
{
  my ($fn) = @_;
  return _write_cache($fn).',meta';
}

use Data::Dumper;
$Data::Dumper::Terse = 1;
$Data::Dumper::Indent = 0;

sub _wc_write_meta
{
  my ($fn,$meta) = @_;
  open(my $fh, '>', _write_cache_meta($fn)) or return -EIO();
  print $fh Dumper($meta);
  close($fh) or return -EIO();
  return 0;
}

sub delete_write_cache
{
  my ($fn) = @_;
  return unlink(_write_cache($fn)) + unlink(_write_cache_meta($fn));
}

sub exists_write_cache
{
  my ($fn) = @_;
  return -f _write_cache($fn);
}

sub _load_write_cache
{
  my ($fn) = @_;
  return 1 if exists_write_cache($fn);
  my $r;
  eval {
    $bucket->get_key_filename($fn,'',_write_cache($fn));
  };
  _debug("_load_write_cache: $@");
  return -EIO() if $@;
  return $r ? 1 : 0;
}

=pod

  unlink

=cut


sub s3fs_unlink {
  my ($fn) = @_;
  $fn =~ s{^/}{};

  # Clear the cache if any.
  delete_write_cache($fn);
  delete $node_cache{$fn};

  # Delete the file on S3.
  my $r;
  eval {
    $r = $bucket->delete_key($fn);
  };
  _debug("s3fs_unlink/delete_key: $@");
  return -EIO() if $@;
  return $r ? 0 : -ENOENT();
}

sub s3fs_truncate {
  my ($fn,$offset) = @_;
  $fn =~ s{^/}{};

  # Load the file from S3.
  my $r = _load_write_cache($fn);
  return $r if $r < 0;

  # Truncate locally.
  eval {
    truncate(_write_cache($fn),$offset);
  };
  _debug("s3fs_truncate: $@");
  return -EIO() if $@;
  $node_cache{$fn}->{content_length} = $offset;
  return 0;
}

sub s3fs_open {
  my ($fn,$flags) = @_;
  return 0;
}

sub s3fs_read {
  my ($fn,$size,$offset) = @_;
  $fn =~ s{^/}{};

  # Use the write_cache if newer content might be there.
  if(exists_write_cache($fn))
  {
    open(my $fh,'<',_write_cache($fn)) or return -EIO();
    my $buffer;
    my $r = read($fh,$buffer,$size,$offset);
    return -EIO() if !defined $r;
    close($fh) or return -EIO();
    return $buffer;
  }

  # Use S3 otherwise.
  my $range = 'bytes='.$offset.'-'.($offset+$size);
  my $r;
  eval {
    $r = $bucket->get_key_with_headers($fn,undef,undef,{Range=> $range});
  };
  _debug("s3fs_read/get_key_with_headers: $@");
  return -EIO() if $@;
  return -ENOENT() unless defined $r;
  return substr($r->{value},0,$size);
}

sub s3fs_write {
  my ($fn,$buffer,$offset) = @_;
  $fn =~ s{^/}{};
  _debug("write $fn '$buffer' $offset");

  # Download the file from S3 if needed.
  my $r = _load_write_cache($fn);
  return $r if $r < 0;

  # Write the data.
  open(my $fh,($r == 0?'>':'+<'),_write_cache($fn)) or return -EIO();
  seek($fh,$offset,0) or return -EIO();
  print $fh $buffer;
  my $return = tell($fh)-$offset;
  close($fh) or return -EIO();
  $node_cache{$fn}->{content_length} = -s _write_cache($fn);
  return $return;
}

sub s3fs_statfs {
  return (4096,10000,10000,10000,10000,4096);
}

sub s3fs_flush {
  my ($fn) = @_;
  $fn =~ s{^/}{};
  return 0;
}

sub s3fs_release {
  my ($fn,$flags) = @_;
  $fn =~ s{^/}{};
  if(exists_write_cache($fn))
  {
    my $configuration = $node_cache{$fn};
    $configuration->{acl_short} = 'private';
    $configuration->{'x-amz-meta-s3fs-mtime'} = time();
    $configuration->{fn} = $fn;
    _wc_write_meta($fn,$configuration);
  }
  return 0;
}

sub s3fs_fsync {
  my ($fn,$flags) = @_;
  $fn =~ s{^/}{};
  return 0;
}

=pod
  Probably will never implement these.
=cut

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

=pod

  These are still TBD.

=cut

sub s3fs_rename {
  my ($ofn,$fn) = @_;
  $ofn =~ s{^/}{};
  $fn =~ s{^/}{};
  
  # Copy
  my $configuration = {};
  $configuration->{acl_short} = 'private';
  $configuration->{'x-amz-copy-source'} = "${s3_bucket_name}/${ofn}";
  my $r;
  eval {
    $r = $bucket->add_key($fn,'',$configuration);
  };
  _debug("s3fs_rename/add_key: $@");
  return -EIO() if $@;
  return -ENOENT() if ! $r;

  # Delete the old one
  return s3fs_unlink($ofn);
}

sub s3fs_symlink {
  return -1009;
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

sub s3fs_utime {
  my ($fn,$atime,$mtime) = @_;
  $fn =~ s{^/}{};
  return 0;
}

=pod

   Daemon features
   
=cut

sub daemon_read_meta
{
  my ($cn) = @_;
  my $meta_fn = "${s3_cache}/${cn},meta";
  my $work_fn = "${s3_cache}/${cn},work";
  rename($meta_fn,$work_fn);
  open(my $fh, '<', $work_fn) or return undef;
  local $/;
  my $content = <$fh>;
  close($fh) or return undef;
  unlink($work_fn);
  return eval($content);
}



sub daemon_upload {
  my ($cn) = @_;
  # Make sure we obtain the metadata.
  my $configuration = daemon_read_meta($cn);
  _debug('daemon_upload: No configuration'),
  return -EIO() unless defined $configuration;
  # Upload the file.
  my $fn = $configuration->{fn};
  _debug('daemon_upload: No filename'),
  return -EIO() unless defined $fn;
  my $r;
  eval {
    $r = $bucket->add_key_filename($fn,"${s3_cache}/${cn}",$configuration);
  };
  _debug("daemon_upload: $@",$bucket->errstr);
  return -EIO() if $@;
  unlink("${s3_cache}/${cn}");
  return $r ? 0 : -ENOENT();
}

sub start_daemon {
  _debug('Starting S3 daemon');
  while(1)
  {
    return unlink("${s3_cache}/.quit")
      if -e "${s3_cache}/.quit";
    sleep(1);
    if(opendir(my $dh, $s3_cache))
    {
      my @v;
      my @to_upload = map {
        @v = split(/,/);
        defined($v[2]) && $v[0] eq $s3_bucket_name && $v[2] eq 'meta' ?
        ("$v[0],$v[1]") : ();
      } readdir($dh);
      closedir($dh);
      for my $cn (@to_upload)
      {
        _debug("Attempting to upload $cn");
        daemon_upload($cn);
      }
    }
    else
    {
      print STDERR "opendir($s3_cache): $!";
    }
  }
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
