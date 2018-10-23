#!/usr/bin/perl --

use Time::Local;

$file = @ARGV[0];

if ( open( FILE, "<$file" ) ) {
  while ( my $line = <FILE> ) {
    chomp $line;
    my $reason = "NULL";

    # ts,host,isDelayed,log

    # /mnt/data-remote-log/dashboard/BC910041/messages-2018-07-24-11-30-03
    # /mnt/data-remote-log/syslog/BSU00719/messages-2018-07-24
    # /mnt/data-remote-log/HULFT/BC710001/messages-2018-07-24-10-30-01
    if( $line =~ /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).*?\/mnt\/data-remote-log\/([a-zA-Z0-9]+)\/(.*?)\/messages-(\d{4})-(\d{2})-(\d{2}).*$/ ) {
      $ts_y   = $1;
      $ts_mon = $2;
      $ts_md  = $3;
      $ts_h   = $4;
      $ts_min = $5;
      $ts_sec = $6;
      $agent  = $7;
      $host   = $8;
      $f_y    = $9;
      $f_mon  = $10;
      $f_md   = $11;
      $file   = sprintf( "messages-%s-%s-%s", $f_y, $f_mon, $f_md );

      if ( $line =~ /INFO (.*?): .*$/ ) {
        $reason = $1;
      }

#      $isDelayed = ( timelocal( 0, 0, 0, $ts_md, $ts_mon - 1, $ts_y) - timelocal( 0, 0, 0, $f_md, $f_mon -1, $f_y ) > 0 ) ? 1 : 0 ;
      
      printf( "%s-%s-%sT%s:%s:%s,%s;%s;%s,%s,%s,%s,%s\n",
               $ts_y, $ts_mon, $ts_md, $ts_h, $ts_min, $ts_sec, $host, $file, $agent, $host, $file, $reason, $line );
    }

    elsif( $line =~ /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).*?\/var\/log\/(syslogng)\/([A-Z0-9]+)\/[A-Z0-9]+-(\d{4})-(\d{2})-(\d{2}).*$/ ) {
      $ts_y   = $1;
      $ts_mon = $2;
      $ts_md  = $3;
      $ts_h   = $4;
      $ts_min = $5;
      $ts_sec = $6;
      $agent  = $7;
      $host   = $8;
      $f_y    = $9;
      $f_mon  = $10;
      $f_md   = $11;
      $file   = sprintf( "%s-%s-%s-%s",$host,$f_y,$f_mon,$f_md );

      if ( $line =~ /INFO (.*?): .*$/ ) {
        $reason = $1;
      }

      # Compare the timestamp and date time of file
#      $isDelayed = ( timelocal( 0, 0, 0, $ts_md, $ts_mon - 1, $ts_y) - timelocal( 0, 0, 0, $f_md, $f_mon -1, $f_y ) > 0 ) ? 1 : 0 ;

      printf( "%s-%s-%sT%s:%s:%s,%s;%s;%s,%s,%s,%s,%s\n",
               $ts_y, $ts_mon, $ts_md, $ts_h, $ts_min, $ts_sec, $host, $file, $agent, $host, $file, $reason, $line );

    }
  }
}
