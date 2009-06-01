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

=pod

  S3FS_fuse module

=cut

package S3FS_fuse;
use strict; use warnings;

use Logger::Syslog;

use Fuse qw(fuse_get_context);
use Fcntl qw(:mode);
use POSIX qw(O_ACCMODE O_WRONLY :errno_h);

use threads; use threads::shared;

# Some of these are MacFUSE extensions.
# See http://code.google.com/p/macfuse/wiki/OPTIONS
# Could use auto_xattr,daemon_timeout=240
use constant FUSE_MOUNTOPTS => "default_permissions";

use constant FUSE_DEBUG => 0;

sub new {
  my $class = shift;
  my $self = {};
  bless $self, $class;
  $self->initialize(@_);
  return $self;
}

sub initialize {
  my $self = shift;
  my ($s3_cache,$bucket,$s3_bucket_name) = @_;
  $self->{s3_cache} = $s3_cache;
  $self->{bucket}   = $bucket;
  $self->{s3_bucket_name} = $s3_bucket_name;
}

=pod

  Write Cache management
    Note: this is only the "write" side of the cache.
    There is a daemon running to actually upload the files
    to S3. The daemon only upload files which are present and
    have a metafile.

=cut

sub _wc_name
{
  my $self = shift;
  my ($fn) = @_;
  $fn =~ s/[^\w]/_/g;
  return $self->{s3_cache}.'/'.$self->{s3_bucket_name}.",${fn}";
}

sub _wc_meta_name
{
  my $self = shift;
  my ($fn) = @_;
  return $self->_wc_name($fn).',meta';
}

use Data::Dumper;
$Data::Dumper::Terse = 1;
$Data::Dumper::Indent = 0;

sub _wc_write_meta
{
  my $self = shift;
  my ($fn,$meta) = @_;
  show_call(@_);
  open(my $fh, '>', $self->_wc_meta_name($fn)) or return -EIO();
  print $fh Dumper($meta);
  close($fh) or return -EIO();
  return 0;
}

sub delete_write_cache
{
  my $self = shift;
  my ($fn) = @_;
  return unlink($self->_wc_name($fn)) 
    + unlink($self->_wc_meta_name($fn));
}

sub exists_write_cache
{
  my $self = shift;
  my ($fn) = @_;
  return -f $self->_wc_name($fn);
}

sub _load_write_cache
{
  my $self = shift;
  my ($fn) = @_;
  return 1 if $self->exists_write_cache($fn);
  # debug("load_write_cache($fn): gather from S3");
  my $r;
  eval {
    $r = $self->{bucket}->get_key_filename($fn,'',$self->_wc_name($fn));
  };
  warning("get_key_filename: $@") if $@;
  return -EIO() if $@;
  if(!defined($r))
  {
    # The file does not exists on S3. Create a local (empty) file so
    # that we don't attempt to download it again next time.
    # Also this allows s3fs_write to use '+<'.
    main::_touch($self->_wc_name($fn));
  }
  return $r ? 1 : 0;
}


# Since Fuse.pm calls back using name we have to share $self for
# the callbacks.
our $fuse_self = undef;

sub start_fuse {
  our $self = shift;
  my ($mountpoint) = @_;
  # Build closures of our functions.
  my $n;
  my @ops_names = qw (
    getattr readlink getdir mknod mkdir unlink rmdir symlink rename
    link chmod chown truncate utime open read write statfs
    flush release fsync setxattr getxattr listxattr removexattr
  );
  my @ops = ();
  for my $name (@ops_names) {
    my $n = "s3fs_${name}";
    my $f = sub { return $fuse_self->$n(@_) };
    push @ops, $name => $f;
  }

  my $mountopts = FUSE_MOUNTOPTS;
  $mountopts .= ($mountopts && ',') . "fsname=s3fs:".$self->{s3_bucket_name};
  # Modified Fuse.pm
  $mountopts .= ($mountopts && ',') . "volname=s3fs:".$self->{s3_bucket_name};
  $mountopts .= ($mountopts && ',') . "iosize=".BLOCK_SIZE();

  info('Starting Fuse::main');
  $fuse_self = $self;
  eval {
    Fuse::main (
      debug => FUSE_DEBUG,
      mountpoint => $mountpoint,
      mountopts => $mountopts,
      threaded => 0,
      @ops
    );
  };
  warning("Fuse::main died: $@") if $@;
  info('Fuse::main exited');
}

=pod

  s3fs_* functions

=cut


use constant S_DEFAULT_REG => S_IRUSR | S_IWUSR | S_IRGRP | S_IFREG;
use constant S_DEFAULT_DIR => S_IRWXU | S_IRGRP | S_IXGRP | S_IFDIR;
use constant S_DEFAULT_LNK => S_IRWXU | S_IRWXG | S_IRWXO | S_IFLNK;

use constant BLOCK_SIZE => 256*1024;

sub show_call {
  # my $caller_name = (caller(1))[3];
  # debug($caller_name.'('.join(',',@_).')');
}


sub s3fs_getattr {
  my $self = shift;
  show_call(@_);

  my ($fn) = (@_);
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
      BLOCK_SIZE,     # blksize
      1,              # blocks
    );
    return @r;
  }

  # Other files or directories
  my $hk;
  if(exists($self->{node_cache}->{$fn}))
  {
    # debug("Using cache for $fn");
    $hk = $self->{node_cache}->{$fn};
  }
  else
  {
    # debug("No cache for $fn");
    eval {
      $hk = $self->{bucket}->head_key($fn);
    };
    warning("head_key: $@") if $@;
    return -EIO() if $@;
    return -ENOENT() unless defined $hk;
  }

  $hk->{'x-amz-meta-s3fs-mode'} ||= S_DEFAULT_REG;
  $hk->{'x-amz-meta-s3fs-atime'}||= $now;
  $hk->{'x-amz-meta-s3fs-mtime'}||= $now;
  $hk->{'x-amz-meta-s3fs-ctime'}||= $now;
  $self->{node_cache}->{$fn} = $hk;

  # debug(join(', ', map { "$_ => $hk->{$_}" } keys %{$hk}));
  my @r = (
    1,              # dev
    2,              # ino
    $hk->{'x-amz-meta-s3fs-mode'},  # mode
    1,              # nlink
    $fuse_context->{uid},   # uid
    $fuse_context->{gid},   # gid
    0,              # rdev
    $hk->{content_length},    # size
    $hk->{'x-amz-meta-s3fs-atime'},   # atime
    $hk->{'x-amz-meta-s3fs-mtime'},   # mtime
    $hk->{'x-amz-meta-s3fs-ctime'},   # ctime
    BLOCK_SIZE,     # blksize
    ($hk->{content_length})/BLOCK_SIZE, # blocks
  );
  
  return @r;
}

sub s3fs_mknod {
  my $self = shift;
  show_call(@_);

  my ($fn,$mode,$dev) = @_;
  $fn =~ s{^/}{};

  my $configuration = {};
  $configuration->{acl_short} = 'private';
  # $configuration->{'x-amz-meta-s3fs-mode'} = $mode;
  $configuration->{'x-amz-meta-s3fs-mode'} = S_DEFAULT_REG;
  $configuration->{'x-amz-meta-s3fs-atime'} = time();
  $configuration->{'x-amz-meta-s3fs-mtime'} = time();
  $configuration->{'x-amz-meta-s3fs-ctime'} = time();
  $configuration->{content_length} = 0;
  $self->{node_cache}->{$fn} = $configuration;
  $self->_gd_add($fn);
  return 0;
}

=pod

  getdir
  mkdir
  rmdir
    Use S3 directly.

=cut

sub _gd_add {
  my $self = shift;
  my ($fn) = @_;
  my ($parent,$part) = ($fn =~ m{^(.*)/([^/]+)});
  $self->{getdir}->{$parent}->{$part} = 1;
}

sub _gd_del {
  my $self = shift;
  my ($fn) = @_;
  my ($parent,$part) = ($fn =~ m{^(.*)/([^/]+)});
  delete $self->{getdir}->{$parent}->{$part};
}

sub s3fs_getdir {
  my $self = shift;
  show_call(@_);

  my ($dir) = (@_);
  $dir =~ s{^/}{};

  if(exists($self->{getdir}->{$dir}))
  {
    # debug("Using cache for $dir");
    return (keys(%{$self->{getdir}->{$dir}}),0);
  }

  # debug("No cache for $dir");

  # Make sure we can use delimiter in list_all()
  my $prefix = $dir;
  $prefix .= '/' if $prefix ne '';

  my $r;
  eval {
    $r = $self->{bucket}->list_all({
      delimiter => '/',
      prefix => $prefix
    });
  };
  warning("list_all: $@") if $@;
  return -EIO() if $@;
  return (0) unless defined $r;

  my $prefix_l = length($prefix);

  # Remove the prefix from the results.
  my @r = map { substr($_->{key},$prefix_l) } @{$r->{keys}};
  foreach (@r) { $self->{getdir}->{$dir}->{$_} = 1; }
  return (@r,0);
}

sub s3fs_mkdir {
  my $self = shift;
  show_call(@_);

  my ($dir,$mode) = @_;
  $dir =~ s{^/}{};

  return -EINVAL() if $dir eq '';

  my $configuration = {};
  $configuration->{acl_short} = 'private';
  $configuration->{'x-amz-meta-s3fs-mode'} = S_DEFAULT_DIR;
  $configuration->{'x-amz-meta-s3fs-atime'} = time();
  $configuration->{'x-amz-meta-s3fs-mtime'} = time();
  $configuration->{'x-amz-meta-s3fs-ctime'} = time();
  $configuration->{content_length} = 0;

  my $r;
  eval {
    $r = $self->{bucket}->add_key($dir,'',$configuration);
  };
  warning("add_key: $@") if $@;
  return -EIO() if $@;
  return -ENOENT() unless $r;
  $self->{node_cache}->{$dir} = $configuration;
  $self->_gd_add($dir);
  return 0;
}

sub s3fs_rmdir {
  my $self = shift;
  show_call(@_);

  my ($dir) = @_;
  $dir =~ s{^/}{};
  return -EINVAL() if $dir eq '';
  delete $self->{node_cache}->{$dir};
  my $r;
  eval {
    $r = $self->{bucket}->delete_key($dir);
  };
  warning("delete_key: $@") if $@;
  return -EIO() if $@;
  $self->_gd_del($dir) if $r;
  return $r ? 0 : -ENOENT();
}

sub s3fs_unlink {
  my $self = shift;
  show_call(@_);

  my ($fn) = @_;
  $fn =~ s{^/}{};

  # Clear the cache if any.
  $self->delete_write_cache($fn);
  delete $self->{node_cache}->{$fn};

  # Delete the file on S3.
  my $r;
  eval {
    $r = $self->{bucket}->delete_key($fn);
  };
  warning("delete_key: $@") if $@;
  return -EIO() if $@;
  $self->_gd_del($fn) if $r;
  return $r ? 0 : -ENOENT();
}

sub s3fs_truncate {
  my $self = shift;
  show_call(@_);

  my ($fn,$offset) = @_;
  $fn =~ s{^/}{};

  # Load the file from S3.
  my $r = $self->_load_write_cache($fn);
  return $r if $r < 0;

  # Truncate locally.
  eval {
    truncate($self->_wc_name($fn),$offset);
  };
  warning("truncate: $@") if $@;
  return -EIO() if $@;
  $self->{node_cache}->{$fn}->{content_length} = $offset;
  return 0;
}

sub s3fs_open {
  my $self = shift;
  show_call(@_);

  my ($fn,$flags) = @_;
  if(($flags & O_ACCMODE) == O_WRONLY)
  {
    main::_touch($self->_wc_name($fn));
  }
  return 0;
}

sub s3fs_read {
  my $self = shift;
  show_call(@_);

  my ($fn,$size,$offset) = @_;
  $fn =~ s{^/}{};

  # Use the write_cache if newer content might be there.
  if($self->exists_write_cache($fn))
  {
    open(my $fh,'<',$self->_wc_name($fn)) or return -EIO();
    my $buffer;
    my $r = read($fh,$buffer,$size,$offset);
    return -EIO() if !defined $r;
    close($fh) or return -EIO();
    return substr($buffer,0,$size);
  }

  # Use S3 otherwise.
  my $range = 'bytes='.$offset.'-'.($offset+$size);
  my $r;
  eval {
    $r = $self->{bucket}->get_key_with_headers($fn,undef,undef,{Range=> $range});
  };
  warning("s3fs_read/get_key_with_headers: $@") if $@;
  return -EIO() if $@;
  return -ENOENT() unless defined $r;
  return substr($r->{value},0,$size);
}

sub s3fs_write {
  my $self = shift;
  show_call(@_);

  my ($fn,$buffer,$offset) = @_;
  $fn =~ s{^/}{};

  # Download the file from S3 if needed.
  my $r = $self->_load_write_cache($fn);
  return $r if $r < 0;

  # Write the data.
  open(my $fh,'+<',$self->_wc_name($fn)) or return -EIO();
  seek($fh,$offset,0) or return -EIO();
  print $fh $buffer;
  my $return = tell($fh)-$offset;
  close($fh) or return -EIO();
  $self->{node_cache}->{$fn}->{'x-amz-meta-s3fs-mtime'} = time();
  $self->{node_cache}->{$fn}->{content_length} = -s $self->_wc_name($fn);
  return $return;
}

sub s3fs_statfs {
  my $self = shift;
  show_call(@_);

  return (4096,10000,10000,10000,10000,BLOCK_SIZE);
}

sub s3fs_flush {
  my $self = shift;
  show_call(@_);

  my ($fn) = @_;
  $fn =~ s{^/}{};
  return 0;
}

sub s3fs_release {
  my $self = shift;
  show_call(@_);

  my ($fn,$flags) = @_;
  $fn =~ s{^/}{};
  if($self->exists_write_cache($fn))
  {
    my $configuration = $self->{node_cache}->{$fn};
    $configuration->{acl_short} = 'private';
    $configuration->{'x-amz-meta-s3fs-atime'} = time();
    $configuration->{fn} = $fn;
    $self->_wc_write_meta($fn,$configuration);
  }
  return 0;
}

sub s3fs_fsync {
  my $self = shift;
  show_call(@_);

  my ($fn,$flags) = @_;
  $fn =~ s{^/}{};
  return 0;
}

sub s3fs_rename {
  my $self = shift;
  show_call(@_);

  my ($ofn,$fn) = @_;
  $ofn =~ s{^/}{};
  $fn =~ s{^/}{};

  # Copy
  my $hk;
  if(exists($self->{node_cache}->{$fn}))
  {
    # debug("Using cache for $fn");
    $hk = $self->{node_cache}->{$fn};
  }
  else
  {
    # debug("No cache for $fn");
    eval {
      $hk = $self->{bucket}->head_key($fn);
    };
    warning("head_key: $@") if $@;
    return -EIO() if $@;
    return -ENOENT() unless defined $hk;
  }

  $hk->{acl_short} = 'private';
  $hk->{'x-amz-copy-source'} = join('/',$self->{s3_bucket_name},$ofn);
  my $r;
  eval {
    $r = $self->{bucket}->add_key($fn,'',$hk);
  };
  warning("add_key: $@") if $@;
  return -EIO() if $@;
  return -ENOENT() if ! $r;
  $self->{node_cache}->{$fn} = $hk;
  $self->_gd_add($fn) if $r;

  # Delete the old one
  return $self->s3fs_unlink($ofn);
}

sub s3fs_symlink {
  my $self = shift;
  show_call(@_);

  my ($to,$fn) = @_;
  $fn =~ s{^/}{};

  my $configuration = {};
  $configuration->{acl_short} = 'private';
  $configuration->{'x-amz-meta-s3fs-mode'} = S_DEFAULT_LNK;
  $configuration->{'x-amz-meta-s3fs-atime'} = time();
  $configuration->{'x-amz-meta-s3fs-mtime'} = time();
  $configuration->{'x-amz-meta-s3fs-ctime'} = time();
  $configuration->{content_length} = 0;
  my $r;
  eval {
    $r = $self->{bucket}->add_key($fn,$to,$configuration);
  };
  warning("add_key: $@") if $@;
  return -EIO() if $@;
  return -ENOENT() unless $r;
  $self->{node_cache}->{$fn} = $configuration;
  $self->_gd_add($fn);
  return 0;
}

sub s3fs_readlink {
  my $self = shift;
  show_call(@_);

  my ($fn) = @_;
  $fn =~ s{^/}{};
  
  my $r;
  eval {
    $r = $self->{bucket}->get_key($fn);
  };
  warning("add_key: $@") if $@;
  return -EIO() if $@;
  $self->{node_cache}->{$fn} = $r;
  delete $self->{node_cache}->{$fn}->{value};
  return $r ? $r->{value} : -ENOENT();
}

sub s3fs_utime {
  my $self = shift;
  show_call(@_);

  my ($fn,$atime,$mtime) = @_;
  $fn =~ s{^/}{};

  my $hk;
  if(exists($self->{node_cache}->{$fn}))
  {
    # debug("Using cache for $fn");
    $hk = $self->{node_cache}->{$fn};
  }
  else
  {
    # debug("No cache for $fn");
    eval {
      $hk = $self->{bucket}->head_key($fn);
    };
    warning("head_key: $@") if $@;
    return -EIO() if $@;
    return -ENOENT() unless defined $hk;
  }

  $hk->{'x-amz-meta-s3fs-atime'} = $atime;
  $hk->{'x-amz-meta-s3fs-mtime'} = $mtime;

  # Change the attributes
  $hk->{acl_short} = 'private';
  $hk->{'x-amz-copy-source'} = join('/',$self->{s3_bucket_name},$fn);
  my $r;
  eval {
    $r = $self->{bucket}->add_key($fn,'',$hk);
  };
  warning("add_key: $@") if $@;
  return -EIO() if $@;
  $self->{node_cache}->{$fn} = $hk if $r;
  return $r ? 0 : -ENOENT();
}

sub s3fs_chmod {
  my $self = shift;
  show_call(@_);
  return 0;
}

sub s3fs_chown {
  my $self = shift;
  show_call(@_);
  return 0;
}

=pod
  Probably will never implement these.
=cut

sub s3fs_link {
  my $self = shift;
  show_call(@_);
  return -EOPNOTSUPP();
}

sub s3fs_setxattr {
  my $self = shift;
  show_call(@_);
  return -EOPNOTSUPP();
}
sub s3fs_getxattr {
  my $self = shift;
  show_call(@_);
  return -EOPNOTSUPP();
}
sub s3fs_listxattr {
  my $self = shift;
  show_call(@_);
  return -EOPNOTSUPP();
}
sub s3fs_removexattr {
  my $self = shift;
  show_call(@_);
  return -EOPNOTSUPP();
}

=pod

   Daemon features

=cut

package S3FS_upload_daemon;
use strict; use warnings;
use Logger::Syslog;

use POSIX qw(:errno_h);

sub new {
  my $class = shift;
  my $self = {};
  bless $self, $class;
  $self->initialize(@_);
  return $self;
}

sub initialize {
  my $self = shift;
  my ($s3_cache,$bucket,$s3_bucket_name) = @_;
  $self->{s3_cache} = $s3_cache;
  $self->{bucket}   = $bucket;
  $self->{s3_bucket_name} = $s3_bucket_name;
}

use constant RENAME_META => 0;

sub daemon_read_meta
{
  my $self = shift;
  my ($cn) = @_;
  my $meta_fn = $self->{s3_cache}."/${cn},meta";
  my $work_fn;
  if(RENAME_META)
  {
    $work_fn = $self->{s3_cache}."/${cn},work";
    rename($meta_fn,$work_fn);
  }
  else
  {
    $work_fn = $meta_fn;
  }
  open(my $fh, '<', $work_fn) or return undef;
  local $/;
  my $content = <$fh>;
  close($fh) or return undef;
  return eval($content);
}

sub daemon_upload {
  my $self = shift;
  my ($cn) = @_;

  # Make sure we obtain the metadata.
  my $configuration = $self->daemon_read_meta($cn);
  error('daemon_upload: No configuration'),
  return -EIO() unless defined $configuration;

  # Make sure we know where to upload.
  my $fn = $configuration->{fn};
  delete $configuration->{fn};
  error('daemon_upload: No filename'),
  return -EIO() unless defined $fn;

  # Make sure we have something to upload.
  my $cname = $self->{s3_cache}."/${cn}";
  error("no data file $cname"),
  return -EIO() unless -f $cname;

  my $r;
  eval {
    $r = $self->{bucket}->add_key_filename($fn,$cname,$configuration);
  };
  warning("daemon_upload($cn): $@") if $@;
  return -EIO() if $@;
  return -ENOENT() if !$r;

  debug("Upload completed successfully.");
  my $work_fn;
  if(RENAME_META)
  {
    $work_fn = $self->{s3_cache}."/${cn},work";
  }
  else
  {
    $work_fn = $self->{s3_cache}."/${cn},meta";
  }
  unlink($work_fn);
  unlink($self->{s3_cache}."/${cn}");
  return 0;
}

sub stop_daemon {
  my $s3_cache = shift;
  info("Stopping S3 daemon.");
  main::_touch($s3_cache."/.quit");
}

sub start_daemon {
  my $self = shift;
  info('Starting S3 daemon');
  while(1)
  {
    if(opendir(my $dh, $self->{s3_cache}))
    {
      my @v;
      my @to_upload = map {
        @v = split(/,/);
        defined($v[2]) && $v[0] eq $self->{s3_bucket_name} && $v[2] eq 'meta' ?
        ("$v[0],$v[1]") : ();
      } readdir($dh);
      closedir($dh);
      for my $cn (@to_upload)
      {
        info("Attempting to upload $cn");
        $self->daemon_upload($cn);
      }
    }
    else
    {
      error("opendir(".$self->{s3_cache}."): $!");
    }

    sleep(3);
    info('Daemon received request to stop'),
    return unlink($self->{s3_cache}."/.quit")
      if -e $self->{s3_cache}."/.quit";
  }
}


package main;
use strict; use warnings;

use Amazon::S3;
# use S3FS_upload_daemon;
# use S3FS_fuse;

use POSIX qw(setsid);

=pod
  The write_cache is indexed on the filename.
  It stores the content of the file while it is been written to.
=cut

use constant SECRET_FILE => $ENV{HOME}.'/.s3fs/.secret';

use constant S3_SECURE_ACCESS => 1;
use constant S3_RETRY => 2;
use constant S3_TIMEOUT => 7;

use Logger::Syslog;

# From the perlipc manpage.
sub daemonize {
    chdir '/'               or die "Can't chdir to /: $!";
    open STDIN, '/dev/null' or die "Can't read /dev/null: $!";
    open STDOUT, '>/dev/null'
                            or die "Can't write to /dev/null: $!";
    defined(my $pid = fork) or die "Can't fork: $!";
    exit if $pid;
    setsid                  or die "Can't start a new session: $!";
    open STDERR, '>&STDOUT' or die "Can't dup stdout: $!";
}

sub _touch
{
  my ($name) = @_;
  my $fh;
  open($fh,'>',$name) && close($fh);
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

sub main {
  my ( $s3_bucket_name, $mountpoint, $s3_cache ) = @_;

  logger_prefix("s3fs.pl:$s3_bucket_name");

  info("Started with mountpoint=$mountpoint and cache=$s3_cache");

  my ($aws_access_key_id,$aws_secret_access_key) = get_secret(SECRET_FILE);

  my $s3_params = {
    aws_access_key_id     => $aws_access_key_id,
    aws_secret_access_key => $aws_secret_access_key,
    secure    => S3_SECURE_ACCESS,
    retry     => S3_RETRY,
    timeout   => S3_TIMEOUT,
  };

  if(fork())
  {
    daemonize();
    eval {
      my $s3 = new Amazon::S3 ( $s3_params );
      my $bucket = $s3->bucket($s3_bucket_name);
      my $daemon = new S3FS_upload_daemon($s3_cache,$bucket,$s3_bucket_name);
      $daemon->start_daemon();
    };
    warning($@) if $@;
    info("Upload daemon terminated.");
    exit(0);
  }

  if(fork())
  {
    daemonize() if !S3FS_fuse::FUSE_DEBUG;
    eval {
      my $s3 = new Amazon::S3 ( $s3_params );
      my $bucket = $s3->bucket($s3_bucket_name);

      my $fuse = new S3FS_fuse($s3_cache,$bucket,$s3_bucket_name);
      $fuse->start_fuse($mountpoint);

      S3FS_upload_daemon::stop_daemon($s3_cache);
    };
    warning($@) if $@;
    info("S3-FUSE terminated.");
    exit(0);
  }

  info("System started");
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
