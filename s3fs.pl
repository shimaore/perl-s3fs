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
  my ( $aws_access_key_id, $aws_secret_access_key, $bucket, $mountpoint ) = @_;

  my $s3 = new Amazon::S3 (
    {
      aws_access_key_id => $aws_access_key_id,
      aws_secret_access_key => $aws_secret_access_key,
      retry => 2,
      secure => 1,
    }
  );
  
  $bucket = $s3->bucket($bucket);

  my @ops = 
    map { $_ => "main::s3fs_$_" } qw (
      getattr readlink getdir mknod mkdir unlink rmdir symlink rename
      link chmod chown truncate utime open read write statfs
      flush release fsync setxattr getxattr listxattr removexattr
    );
  
  print @_;

  Fuse::main (
    debug => 1,
    mountpoint => $mountpoint,
    mountopts => "default_permissions",
    threaded => 0,
    @ops
  );
}

sub s3fs_getattr {
  my ($fn) = (@_);
  print STDERR "getattr $fn\n";
  if($fn =~ m{/$})
  {
    my $now = time();
    my @r = (
      1,              # dev
      2,              # ino
      S_IRWXU | S_IFDIR,           # mode
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
  my $hk = $bucket->head_key($fn);
  return -ENOENT() unless defined $hk;
  warn($hk);
  my @r = (
    1,              # dev
    $hk->{etag},    # ino
    S_IRWXU | S_IRWXG | S_IRWXO | S_IFREG,  # mode
    1,              # nlink
    0,              # uid
    0,              # gid
    0,              # rdev
    $hk->{size},    # size
    $hk->{last_modified},     # atime
    $hk->{last_modified},     # mtime
    $hk->{last_modified},     # ctime
    4096,             # blksize
    $hk->{size}/4096, # blocks
  );
  return @r;
}

sub s3fs_readlink {
  return -1011;
}

sub s3fs_getdir {
  my ($dir) = (@_);
  my $r = $bucket->list_all({
    delimiter => '/',
    prefix => $dir,
  });
  return (0) unless defined $r;
  my @r = map { $_->key } @{$r->{keys}};
  return (@r,0);
}

sub s3fs_mknod {
  return -1010;
}

sub s3fs_mkdir {
  my ($dir,$mode) = @_;
  return 0;
}

sub s3fs_unlink {
  my ($fn) = @_;
  my $r = $bucket->delete_key($fn);
  return $r ? 0 : -ENOENT();
}

sub s3fs_rmdir {
  my ($dir) = @_;
  return 0;
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
  return -ENOENT();
  # return $content;
}
sub s3fs_write {
  my ($fn,$buffer,$offset) = @_;
  return 0;
}
sub s3fs_statfs {
  return (4096,10000,10000,10000,10000,4096);
}

sub s3fs_flush {
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
