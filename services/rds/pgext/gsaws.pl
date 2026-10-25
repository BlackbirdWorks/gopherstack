use strict; use IO::Socket::INET; use MIME::Base64;
my ($mode, $method, $path, $hdrs, $b64) = @ARGV;
my $body = '';
if ($method eq 'PUT') { local $/; binmode STDIN; $body = <STDIN>; $body = '' unless defined $body; }
elsif ($b64) { $body = decode_base64($b64); }
my $s = IO::Socket::INET->new(PeerAddr => $ENV{GS_CALLOUT_ADDR}, Proto => 'tcp', Timeout => 60)
  or die "gopherstack callout: cannot connect to $ENV{GS_CALLOUT_ADDR}: $!\n";
binmode $s;
my $extra = join('', map { "$_\r\n" } grep { length } split(/\n/, $hdrs // ''));
print $s "$method $path HTTP/1.0\r\nHost: gopherstack\r\nX-Gs-Token: $ENV{GS_CALLOUT_TOKEN}\r\n"
  . "Content-Length: " . length($body) . "\r\n$extra\r\n$body";
my $resp = do { local $/; <$s> };
my ($head, $rb) = split(/\r\n\r\n/, $resp // '', 2);
$rb = '' unless defined $rb;
my ($status) = ($head // '') =~ m{^HTTP/\S+\s+(\d+)};
die "gopherstack callout: malformed response\n" unless $status;
my %h; for my $l (split(/\r\n/, $head)) { $h{lc $1} = $2 if $l =~ /^([^:]+):\s*(.*)$/; }
if ($mode eq 'json') {
  my $e = sub { encode_base64(defined $_[0] ? $_[0] : '', '') };
  print join("\t", $status, $e->($h{'x-amz-executed-version'}), $e->($h{'x-amz-log-result'}),
    $e->($h{'x-amz-function-error'}), $e->($h{'content-length'}), $e->($rb)), "\n";
} else {
  if ($status >= 400) { print STDERR "gopherstack callout: HTTP $status\n"; exit 1; }
  print $rb unless $method eq 'PUT';
}
